package client

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RemoteTransport implements transport.Transport by connecting to an event server.
// It is safe for concurrent use by multiple goroutines.
type RemoteTransport struct {
	addr string
	opts *options

	mu      sync.RWMutex
	conn    *grpc.ClientConn
	client  eventpb.EventServiceClient
	state   atomic.Int32
	stateMu sync.Mutex

	// Circuit breaker state
	circuitMu       sync.Mutex
	circuitOpen     bool
	circuitOpenAt   time.Time
	consecutiveFail int

	// Shutdown
	closeOnce sync.Once
	closeCh   chan struct{}
}

// Compile-time interface check.
var _ transport.Transport = (*RemoteTransport)(nil)

// New creates a new RemoteTransport connecting to the given address.
func New(addr string, opts ...Option) (*RemoteTransport, error) {
	if addr == "" {
		return nil, errors.New("event-server: address must not be empty")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	t := &RemoteTransport{
		addr:    addr,
		opts:    o,
		closeCh: make(chan struct{}),
	}
	t.state.Store(int32(ConnStateDisconnected))
	return t, nil
}

// State returns the current connection state.
func (t *RemoteTransport) State() ConnState {
	return ConnState(t.state.Load())
}

func (t *RemoteTransport) setState(state ConnState) {
	t.stateMu.Lock()
	old := ConnState(t.state.Swap(int32(state)))
	if old != state && t.opts.onStateChange != nil {
		t.opts.onStateChange(state)
	}
	t.stateMu.Unlock()
}

// Connect establishes the connection to the event server.
func (t *RemoteTransport) Connect(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.State() == ConnStateClosed {
		return transport.ErrTransportClosed
	}

	if t.conn != nil {
		_ = t.conn.Close()
		t.conn = nil
		t.client = nil
	}

	t.setState(ConnStateConnecting)

	dialOpts := t.opts.buildDialOpts()
	conn, err := grpc.NewClient(t.addr, dialOpts...)
	if err != nil {
		t.setState(ConnStateDisconnected)
		return err
	}

	t.conn = conn
	t.client = eventpb.NewEventServiceClient(conn)
	t.setState(ConnStateConnected)
	t.resetCircuit()

	return nil
}

// Close shuts down the transport and releases resources.
func (t *RemoteTransport) Close(ctx context.Context) error {
	var err error
	t.closeOnce.Do(func() {
		close(t.closeCh)
		t.setState(ConnStateClosed)

		t.mu.Lock()
		defer t.mu.Unlock()

		if t.conn != nil {
			err = t.conn.Close()
			t.conn = nil
			t.client = nil
		}
	})
	return err
}

// Ready returns true if the transport is connected and ready.
func (t *RemoteTransport) Ready() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.conn == nil {
		return false
	}
	state := t.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// RegisterEvent creates transport resources for a named event.
func (t *RemoteTransport) RegisterEvent(ctx context.Context, name string) error {
	return t.retry(ctx, func(ctx context.Context) error {
		client, err := t.getClient()
		if err != nil {
			return err
		}
		_, err = client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: name})
		return fromGRPCError(err)
	})
}

// UnregisterEvent removes transport resources for a named event.
func (t *RemoteTransport) UnregisterEvent(ctx context.Context, name string) error {
	return t.retry(ctx, func(ctx context.Context) error {
		client, err := t.getClient()
		if err != nil {
			return err
		}
		_, err = client.UnregisterEvent(ctx, &eventpb.UnregisterEventRequest{Name: name})
		return fromGRPCError(err)
	})
}

// Publish sends a message to an event.
func (t *RemoteTransport) Publish(ctx context.Context, name string, msg transport.Message) error {
	return t.retry(ctx, func(ctx context.Context) error {
		client, err := t.getClient()
		if err != nil {
			return err
		}
		_, err = client.Publish(ctx, &eventpb.PublishRequest{
			Event:    name,
			Id:       msg.ID(),
			Payload:  msg.Payload(),
			Metadata: msg.Metadata(),
		})
		return fromGRPCError(err)
	})
}

// Subscribe creates a subscription to receive messages for an event.
func (t *RemoteTransport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	client, err := t.getClient()
	if err != nil {
		return nil, err
	}

	subOpts := transport.ApplySubscribeOptions(opts...)
	req := buildSubscribeRequest(name, subOpts)

	subCtx, cancel := context.WithCancel(ctx)
	stream, err := client.Subscribe(subCtx, req)
	if err != nil {
		cancel()
		return nil, fromGRPCError(err)
	}

	sub := &remoteSubscription{
		id:            uuid.New().String(),
		ch:            make(chan transport.Message, t.opts.subscribeBufferSize),
		doneCh:        make(chan struct{}),
		cancel:        cancel,
		ctx:           subCtx,
		client:        client,
		req:           req,
		reconnect:     t.opts.subscribeReconnect,
		reconnectWait: t.opts.subscribeReconnectWait,
		maxErrors:     t.opts.subscribeMaxErrors,
		onError:       t.opts.onStreamError,
	}

	go sub.receiveLoop(stream)

	return sub, nil
}

// getClient returns the gRPC client, checking circuit breaker and connection state.
func (t *RemoteTransport) getClient() (eventpb.EventServiceClient, error) {
	if t.State() == ConnStateClosed {
		return nil, transport.ErrTransportClosed
	}

	if t.isCircuitOpen() {
		return nil, &RemoteError{Message: "circuit breaker open"}
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.client == nil {
		return nil, transport.ErrTransportClosed
	}
	return t.client, nil
}

// Circuit breaker methods

func (t *RemoteTransport) isCircuitOpen() bool {
	if !t.opts.enableCircuit {
		return false
	}
	t.circuitMu.Lock()
	defer t.circuitMu.Unlock()

	if !t.circuitOpen {
		return false
	}
	if time.Since(t.circuitOpenAt) > t.opts.circuitTimeout {
		t.circuitOpen = false
		t.consecutiveFail = 0
		return false
	}
	return true
}

func (t *RemoteTransport) recordSuccess() {
	if !t.opts.enableCircuit {
		return
	}
	t.circuitMu.Lock()
	t.consecutiveFail = 0
	t.circuitMu.Unlock()
}

func (t *RemoteTransport) recordFailure() {
	if !t.opts.enableCircuit {
		return
	}
	t.circuitMu.Lock()
	t.consecutiveFail++
	if t.consecutiveFail >= t.opts.circuitThreshold {
		t.circuitOpen = true
		t.circuitOpenAt = time.Now()
	}
	t.circuitMu.Unlock()
}

func (t *RemoteTransport) resetCircuit() {
	t.circuitMu.Lock()
	t.circuitOpen = false
	t.consecutiveFail = 0
	t.circuitMu.Unlock()
}

// retry executes fn with retries and exponential backoff.
func (t *RemoteTransport) retry(ctx context.Context, fn func(ctx context.Context) error) error {
	var lastErr error
	backoff := t.opts.retryBackoff

	for attempt := 0; attempt <= t.opts.maxRetries; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64()))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.closeCh:
				return transport.ErrTransportClosed
			case <-time.After(jitter):
			}

			backoff *= 2
			if backoff > t.opts.maxBackoff {
				backoff = t.opts.maxBackoff
			}
		}

		attemptCtx := ctx
		var cancel context.CancelFunc
		if t.opts.callTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, t.opts.callTimeout)
		}

		lastErr = fn(attemptCtx)

		if cancel != nil {
			cancel()
		}
		if lastErr == nil {
			t.recordSuccess()
			return nil
		}

		if isNonRetryable(lastErr) {
			t.recordFailure()
			return lastErr
		}
	}

	t.recordFailure()
	return lastErr
}

func isNonRetryable(err error) bool {
	switch {
	case errors.Is(err, transport.ErrEventNotRegistered),
		errors.Is(err, transport.ErrEventAlreadyExists),
		errors.Is(err, transport.ErrNoSubscribers):
		return true
	}
	var permErr *PermissionDeniedError
	return errors.As(err, &permErr)
}

// buildSubscribeRequest converts transport options to a proto SubscribeRequest.
func buildSubscribeRequest(name string, opts *transport.SubscribeOptions) *eventpb.SubscribeRequest {
	req := &eventpb.SubscribeRequest{
		Event: name,
	}

	switch opts.DeliveryMode {
	case transport.Broadcast:
		req.DeliveryMode = eventpb.DeliveryMode_DELIVERY_MODE_BROADCAST
	case transport.WorkerPool:
		req.DeliveryMode = eventpb.DeliveryMode_DELIVERY_MODE_WORKER_POOL
		req.WorkerGroup = opts.WorkerGroup
	}

	switch opts.StartFrom {
	case transport.StartFromBeginning:
		req.StartFrom = eventpb.StartPosition_START_POSITION_BEGINNING
	case transport.StartFromLatest:
		req.StartFrom = eventpb.StartPosition_START_POSITION_LATEST
	case transport.StartFromTimestamp:
		req.StartFrom = eventpb.StartPosition_START_POSITION_TIMESTAMP
		if !opts.StartTime.IsZero() {
			req.StartTime = timestamppb.New(opts.StartTime)
		}
	}

	if opts.MaxAge > 0 {
		req.MaxAge = durationpb.New(opts.MaxAge)
	}

	req.LatestOnly = opts.LatestOnly

	if opts.BufferSize > 0 {
		req.BufferSize = int32(opts.BufferSize)
	}

	req.ConsumerId = opts.ConsumerID

	return req
}
