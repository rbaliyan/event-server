package client

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeStream is a minimal eventpb.EventService_SubscribeClient. It embeds the
// generated interface so unused ClientStream methods are satisfied; only Recv
// is implemented. recvFn supplies the sequence of (message, error) results.
type fakeStream struct {
	grpc.ServerStreamingClient[eventpb.Message]
	recvFn func() (*eventpb.Message, error)
}

func (s *fakeStream) Recv() (*eventpb.Message, error) {
	return s.recvFn()
}

// fakeSubscribeClient is a minimal eventpb.EventServiceClient. It embeds the
// generated interface so unused RPCs are satisfied; only Subscribe and Ack are
// implemented. subscribeFn returns the stream (and error) for each Subscribe
// call so reconnect behavior can be scripted.
type fakeSubscribeClient struct {
	eventpb.EventServiceClient

	mu             sync.Mutex
	subscribeCalls int
	subscribeFn    func(call int) (eventpb.EventService_SubscribeClient, error)
}

func (c *fakeSubscribeClient) Subscribe(_ context.Context, _ *eventpb.SubscribeRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[eventpb.Message], error) {
	c.mu.Lock()
	c.subscribeCalls++
	call := c.subscribeCalls
	fn := c.subscribeFn
	c.mu.Unlock()
	return fn(call)
}

func (c *fakeSubscribeClient) Ack(_ context.Context, _ *eventpb.AckRequest, _ ...grpc.CallOption) (*eventpb.AckResponse, error) {
	return &eventpb.AckResponse{}, nil
}

func (c *fakeSubscribeClient) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscribeCalls
}

// transientStream returns a stream whose first Recv yields a transient error.
func transientStream() *fakeStream {
	return &fakeStream{recvFn: func() (*eventpb.Message, error) {
		return nil, status.Error(codes.Unavailable, "boom")
	}}
}

func TestReceiveLoop_ReconnectsThenTerminatesOnMaxErrors(t *testing.T) {
	t.Parallel()

	const maxErrors = 2

	client := &fakeSubscribeClient{}
	client.subscribeFn = func(int) (eventpb.EventService_SubscribeClient, error) {
		// Every reconnect attempt fails, so consecutive errors accumulate
		// (a successful Subscribe would reset the counter) until maxErrors is
		// exceeded and the loop gives up.
		return nil, status.Error(codes.Unavailable, "down")
	}

	var errCount int
	var errMu sync.Mutex
	sub := &remoteSubscription{
		id:            "test-sub",
		ch:            make(chan transport.Message, 8),
		doneCh:        make(chan struct{}),
		ctx:           context.Background(),
		cancel:        func() {},
		client:        client,
		req:           &eventpb.SubscribeRequest{Event: "orders"},
		reconnect:     true,
		reconnectWait: time.Millisecond,
		maxErrors:     maxErrors,
		onError: func(error) {
			errMu.Lock()
			errCount++
			errMu.Unlock()
		},
	}

	go sub.receiveLoop(transientStream())

	select {
	case <-sub.doneCh:
		// Loop terminated as expected once errors exceeded maxErrors.
	case <-time.After(5 * time.Second):
		t.Fatal("receiveLoop did not terminate within 5s")
	}

	// Initial stream errored (reconnect), then reconnect attempts via Subscribe
	// continued until consecutive errors exceeded maxErrors.
	if got := client.calls(); got < 1 {
		t.Fatalf("Subscribe called %d times, want > 0 (expected reconnect attempts)", got)
	}
	errMu.Lock()
	defer errMu.Unlock()
	if errCount == 0 {
		t.Fatal("expected onError to be invoked at least once")
	}
}

func TestReceiveLoop_ReconnectDeliversAfterTransientError(t *testing.T) {
	t.Parallel()

	client := &fakeSubscribeClient{}
	delivered := make(chan struct{})
	client.subscribeFn = func(int) (eventpb.EventService_SubscribeClient, error) {
		// On reconnect, deliver one message then close cleanly with EOF.
		sent := false
		return &fakeStream{recvFn: func() (*eventpb.Message, error) {
			if !sent {
				sent = true
				return &eventpb.Message{
					Id:        "m1",
					Source:    "srv",
					Payload:   []byte("hi"),
					Timestamp: timestamppb.Now(),
				}, nil
			}
			return nil, io.EOF
		}}, nil
	}

	sub := &remoteSubscription{
		id:            "test-sub",
		ch:            make(chan transport.Message, 8),
		doneCh:        make(chan struct{}),
		ctx:           context.Background(),
		cancel:        func() {},
		client:        client,
		req:           &eventpb.SubscribeRequest{Event: "orders"},
		reconnect:     true,
		reconnectWait: time.Millisecond,
		maxErrors:     5,
		onError:       func(error) {},
	}

	go func() {
		for range sub.Messages() {
			select {
			case delivered <- struct{}{}:
			default:
			}
		}
	}()

	// Initial stream errors transiently, forcing a reconnect that then delivers.
	go sub.receiveLoop(transientStream())

	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("expected a message after reconnect within 5s")
	}

	if got := client.calls(); got < 1 {
		t.Fatalf("Subscribe called %d times, want >= 1 (reconnect expected)", got)
	}

	// EOF on the reconnected stream should terminate the loop.
	select {
	case <-sub.doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("receiveLoop did not terminate after clean EOF within 5s")
	}
}
