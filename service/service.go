package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errAckTimeout   = errors.New("ack timeout")
	errStreamClosed = errors.New("subscribe stream closed")
)

// Service implements the EventService gRPC server.
type Service struct {
	eventpb.UnimplementedEventServiceServer

	transport  transport.Transport
	guard      SecurityGuard
	ackTracker *ackTracker
	logger     *slog.Logger
	metrics    *serviceMetrics

	// Track registered events for ListEvents
	eventsMu sync.RWMutex
	events   map[string]struct{}
}

// NewService creates a new EventService.
// Returns an error if transport is nil.
func NewService(t transport.Transport, opts ...Option) (*Service, error) {
	if t == nil {
		return nil, fmt.Errorf("event-server: NewService requires a non-nil transport")
	}
	o := &serviceOptions{
		guard:      DenyAll(),
		logger:     slog.Default(),
		ackTimeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Service{
		transport:  t,
		guard:      o.guard,
		ackTracker: newAckTracker(o.ackTimeout, o.logger),
		logger:     o.logger,
		metrics:    newServiceMetrics(),
		events:     make(map[string]struct{}),
	}, nil
}

// Stop stops the service and cleans up resources.
// Should be called when the server shuts down.
func (s *Service) Stop() {
	s.ackTracker.Stop()
}

// RegisterEvent creates transport resources for a named event.
func (s *Service) RegisterEvent(ctx context.Context, req *eventpb.RegisterEventRequest) (*eventpb.RegisterEventResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "event name is required")
	}

	if err := s.transport.RegisterEvent(ctx, req.Name); err != nil {
		return nil, toGRPCError(err)
	}

	s.eventsMu.Lock()
	s.events[req.Name] = struct{}{}
	s.eventsMu.Unlock()

	return &eventpb.RegisterEventResponse{}, nil
}

// UnregisterEvent removes transport resources.
func (s *Service) UnregisterEvent(ctx context.Context, req *eventpb.UnregisterEventRequest) (*eventpb.UnregisterEventResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "event name is required")
	}

	if err := s.transport.UnregisterEvent(ctx, req.Name); err != nil {
		return nil, toGRPCError(err)
	}

	s.eventsMu.Lock()
	delete(s.events, req.Name)
	s.eventsMu.Unlock()

	return &eventpb.UnregisterEventResponse{}, nil
}

// ListEvents returns all registered event names.
func (s *Service) ListEvents(ctx context.Context, req *eventpb.ListEventsRequest) (*eventpb.ListEventsResponse, error) {
	s.eventsMu.RLock()
	events := make([]string, 0, len(s.events))
	for name := range s.events {
		events = append(events, name)
	}
	s.eventsMu.RUnlock()

	return &eventpb.ListEventsResponse{Events: events}, nil
}

// Publish sends a message to an event.
func (s *Service) Publish(ctx context.Context, req *eventpb.PublishRequest) (*eventpb.PublishResponse, error) {
	if req.Event == "" {
		return nil, status.Error(codes.InvalidArgument, "event name is required")
	}

	msgID := req.Id
	if msgID == "" {
		msgID = uuid.New().String()
	}

	start := time.Now()
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	msg := transport.NewMessage(msgID, sourceFromContext(ctx), req.Payload, req.Metadata, spanCtx)
	if err := s.transport.Publish(ctx, req.Event, msg); err != nil {
		s.metrics.recordPublish(ctx, req.Event, start, true)
		return nil, toGRPCError(err)
	}
	s.metrics.recordPublish(ctx, req.Event, start, false)

	return &eventpb.PublishResponse{Id: msgID}, nil
}

// Subscribe streams messages from an event.
func (s *Service) Subscribe(req *eventpb.SubscribeRequest, stream eventpb.EventService_SubscribeServer) error {
	ctx := stream.Context()

	if req.Event == "" {
		return status.Error(codes.InvalidArgument, "event name is required")
	}

	// Build subscribe options from proto request
	var opts []transport.SubscribeOption

	switch req.DeliveryMode {
	case eventpb.DeliveryMode_DELIVERY_MODE_WORKER_POOL:
		opts = append(opts, transport.WithDeliveryMode(transport.WorkerPool))
		if req.WorkerGroup != "" {
			opts = append(opts, transport.WithWorkerGroup(req.WorkerGroup))
		}
	case eventpb.DeliveryMode_DELIVERY_MODE_BROADCAST:
		opts = append(opts, transport.WithDeliveryMode(transport.Broadcast))
	}

	switch req.StartFrom {
	case eventpb.StartPosition_START_POSITION_BEGINNING:
		opts = append(opts, transport.WithStartFrom(transport.StartFromBeginning))
	case eventpb.StartPosition_START_POSITION_LATEST:
		opts = append(opts, transport.WithStartFrom(transport.StartFromLatest))
	case eventpb.StartPosition_START_POSITION_TIMESTAMP:
		if req.StartTime != nil {
			opts = append(opts, transport.WithStartTime(req.StartTime.AsTime()))
		}
	}

	if req.MaxAge != nil {
		opts = append(opts, transport.WithMaxAge(req.MaxAge.AsDuration()))
	}

	if req.LatestOnly {
		opts = append(opts, transport.WithLatestOnly())
	}

	if req.BufferSize > 0 {
		opts = append(opts, transport.WithBufferSize(int(req.BufferSize)))
	}

	if req.ConsumerId != "" {
		opts = append(opts, transport.WithConsumerID(req.ConsumerId))
	}

	sub, err := s.transport.Subscribe(ctx, req.Event, opts...)
	if err != nil {
		return toGRPCError(err)
	}
	defer func() { _ = sub.Close(ctx) }()

	s.metrics.addStream(ctx, req.Event)
	defer s.metrics.removeStream(ctx, req.Event)

	streamID := uuid.New().String()
	defer s.ackTracker.NackStream(streamID, errStreamClosed)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-sub.Messages():
			if !ok {
				return nil
			}

			ackID := s.ackTracker.Track(streamID, msg.Ack)

			protoMsg := &eventpb.Message{
				Id:         msg.ID(),
				Source:     msg.Source(),
				Payload:    msg.Payload(),
				Metadata:   msg.Metadata(),
				RetryCount: int32(msg.RetryCount()), // #nosec G115 -- value is bounded
				AckId:      ackID,
			}
			if !msg.Timestamp().IsZero() {
				protoMsg.Timestamp = timestamppb.New(msg.Timestamp())
			}

			if err := stream.Send(protoMsg); err != nil {
				return err
			}
			s.metrics.recordMessageSent(ctx, req.Event)
		}
	}
}

// Ack acknowledges one or more messages.
func (s *Service) Ack(ctx context.Context, req *eventpb.AckRequest) (*eventpb.AckResponse, error) {
	for _, entry := range req.Entries {
		if entry.AckId == "" {
			continue
		}

		var ackErr error
		if entry.Error != "" {
			ackErr = errors.New(entry.Error)
		}

		if !s.ackTracker.Ack(entry.AckId, ackErr) {
			s.logger.Debug("ack for unknown or expired ack_id",
				"ack_id", entry.AckId)
			continue
		}
		s.metrics.recordAck(ctx, ackErr != nil)
	}

	return &eventpb.AckResponse{}, nil
}

// Health returns server and transport health status.
func (s *Service) Health(ctx context.Context, req *eventpb.HealthRequest) (*eventpb.HealthResponse, error) {
	resp := &eventpb.HealthResponse{
		Status:  eventpb.HealthStatus_HEALTH_STATUS_HEALTHY,
		Message: "ok",
	}

	// Check if transport implements HealthChecker
	if hc, ok := s.transport.(transport.HealthChecker); ok {
		result := hc.Health(ctx)
		resp.Details = make(map[string]string)

		switch result.Status {
		case transport.HealthStatusHealthy:
			resp.Status = eventpb.HealthStatus_HEALTH_STATUS_HEALTHY
		case transport.HealthStatusDegraded:
			resp.Status = eventpb.HealthStatus_HEALTH_STATUS_DEGRADED
		case transport.HealthStatusUnhealthy:
			resp.Status = eventpb.HealthStatus_HEALTH_STATUS_UNHEALTHY
		}

		if result.Message != "" {
			resp.Message = result.Message
		}
		if result.Latency > 0 {
			resp.Details["latency"] = result.Latency.String()
		}
	}

	return resp, nil
}

// UnaryInterceptor returns a gRPC unary server interceptor that authenticates
// and authorizes every unary RPC using the service's SecurityGuard.
// Wire it into the gRPC server: grpc.NewServer(grpc.UnaryInterceptor(svc.UnaryInterceptor())).
func (s *Service) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id, err := s.guard.Authenticate(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
		}
		ctx = contextWithIdentity(ctx, id)
		d, err := s.guard.Authorize(ctx, id, info.FullMethod)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "authorization error: %v", err)
		}
		if !d.Allowed {
			return nil, permissionDenied(d)
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream server interceptor that authenticates
// and authorizes every streaming RPC using the service's SecurityGuard.
// Wire it into the gRPC server: grpc.NewServer(grpc.StreamInterceptor(svc.StreamInterceptor())).
func (s *Service) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		id, err := s.guard.Authenticate(ctx)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
		}
		ctx = contextWithIdentity(ctx, id)
		d, err := s.guard.Authorize(ctx, id, info.FullMethod)
		if err != nil {
			return status.Errorf(codes.Internal, "authorization error: %v", err)
		}
		if !d.Allowed {
			return permissionDenied(d)
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// sourceFromContext extracts the publisher source from gRPC metadata.
// Clients can set this via the "x-source" metadata header.
func sourceFromContext(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if sources := md.Get("x-source"); len(sources) > 0 {
			return sources[0]
		}
	}
	return "remote"
}
