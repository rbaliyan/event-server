package client

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
)

const ackTimeout = 10 * time.Second

// remoteSubscription implements transport.Subscription by reading from a
// gRPC server-streaming Subscribe RPC and converting proto Messages to
// transport.Message with ack-over-gRPC.
type remoteSubscription struct {
	id     string
	ch     chan transport.Message
	closed int32
	doneCh chan struct{}
	cancel context.CancelFunc
	ctx    context.Context

	// For reconnect
	client        eventpb.EventServiceClient
	req           *eventpb.SubscribeRequest
	reconnect     bool
	reconnectWait time.Duration
	maxErrors     int
	onError       func(error)
}

// Compile-time interface check.
var _ transport.Subscription = (*remoteSubscription)(nil)

func (s *remoteSubscription) ID() string                         { return s.id }
func (s *remoteSubscription) Messages() <-chan transport.Message  { return s.ch }

func (s *remoteSubscription) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	s.cancel()
	<-s.doneCh
	return nil
}

func (s *remoteSubscription) isClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

// receiveLoop reads messages from gRPC streams, reconnecting on transient errors.
func (s *remoteSubscription) receiveLoop(initialStream eventpb.EventService_SubscribeClient) {
	defer close(s.doneCh)
	defer close(s.ch)

	consecutiveErrors := 0
	stream := initialStream

	for {
		if s.isClosed() {
			return
		}

		// If we lost the stream, reconnect
		if stream == nil {
			var err error
			stream, err = s.client.Subscribe(s.ctx, s.req)
			if err != nil {
				if s.notifyError(err) {
					return
				}
				consecutiveErrors++
				if consecutiveErrors > s.maxErrors {
					return
				}
				if !s.backoff() {
					return
				}
				continue
			}
			consecutiveErrors = 0
		}

		// Read from the current stream
		shouldReconnect := s.readStream(stream)
		stream = nil

		if !shouldReconnect || s.isClosed() {
			return
		}

		consecutiveErrors++
		if consecutiveErrors > s.maxErrors {
			return
		}
		if !s.backoff() {
			return
		}
	}
}

// readStream reads messages from a single gRPC stream until it errors.
// Returns true if the caller should attempt to reconnect.
func (s *remoteSubscription) readStream(stream eventpb.EventService_SubscribeClient) bool {
	for {
		if s.isClosed() {
			return false
		}

		protoMsg, err := stream.Recv()
		if err != nil {
			if err == io.EOF || s.isClosed() || errors.Is(err, context.Canceled) {
				return false
			}
			s.notifyError(err)
			return s.reconnect
		}

		msg := protoToMessage(protoMsg, s.client, s.ctx)

		select {
		case s.ch <- msg:
		case <-s.ctx.Done():
			return false
		}
	}
}

// notifyError calls the error callback if set. Returns true if reconnect is disabled.
func (s *remoteSubscription) notifyError(err error) bool {
	if s.onError != nil {
		s.onError(err)
	}
	return !s.reconnect
}

// backoff waits for the reconnect interval. Returns false if the subscription was closed.
func (s *remoteSubscription) backoff() bool {
	select {
	case <-time.After(s.reconnectWait):
		return true
	case <-s.ctx.Done():
		return false
	}
}

// protoToMessage converts a proto Message to a transport.Message.
// The Ack function sends an Ack RPC to the server with a bounded timeout.
func protoToMessage(pm *eventpb.Message, client eventpb.EventServiceClient, subCtx context.Context) transport.Message {
	ts := pm.Timestamp.AsTime()
	ackID := pm.AckId
	ackFn := func(err error) error {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		ctx, cancel := context.WithTimeout(subCtx, ackTimeout)
		defer cancel()
		_, ackErr := client.Ack(ctx, &eventpb.AckRequest{
			Entries: []*eventpb.AckEntry{
				{AckId: ackID, Error: errStr},
			},
		})
		return ackErr
	}

	return message.New(
		pm.Id,
		pm.Source,
		pm.Payload,
		pm.Metadata,
		message.WithTimestamp(ts),
		message.WithRetryCount(int(pm.RetryCount)),
		message.WithAckFunc(ackFn),
	)
}
