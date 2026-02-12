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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var emptySpanContext trace.SpanContext

var errAckTimeout = errors.New("ack timeout")

// Service implements the EventService gRPC server.
type Service struct {
	eventpb.UnimplementedEventServiceServer

	transport  transport.Transport
	authorizer Authorizer
	ackTracker *ackTracker
	logger     *slog.Logger

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
		authorizer: DenyAll(),
		logger:     slog.Default(),
		ackTimeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Service{
		transport:  t,
		authorizer: o.authorizer,
		ackTracker: newAckTracker(o.ackTimeout, o.logger),
		logger:     o.logger,
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

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Event:     req.Name,
		Operation: OperationRegister,
	}); err != nil {
		return nil, err
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

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Event:     req.Name,
		Operation: OperationUnregister,
	}); err != nil {
		return nil, err
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
	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Operation: OperationList,
	}); err != nil {
		return nil, err
	}

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

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Event:     req.Event,
		Operation: OperationPublish,
	}); err != nil {
		return nil, err
	}

	msgID := req.Id
	if msgID == "" {
		msgID = uuid.New().String()
	}

	msg := transport.NewMessage(msgID, sourceFromContext(ctx), req.Payload, req.Metadata, emptySpanContext)
	if err := s.transport.Publish(ctx, req.Event, msg); err != nil {
		return nil, toGRPCError(err)
	}

	return &eventpb.PublishResponse{Id: msgID}, nil
}

// Subscribe streams messages from an event.
func (s *Service) Subscribe(req *eventpb.SubscribeRequest, stream eventpb.EventService_SubscribeServer) error {
	ctx := stream.Context()

	if req.Event == "" {
		return status.Error(codes.InvalidArgument, "event name is required")
	}

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Event:     req.Event,
		Operation: OperationSubscribe,
	}); err != nil {
		return err
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

	streamID := uuid.New().String()
	defer s.ackTracker.NackStream(streamID, fmt.Errorf("subscribe stream closed"))

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
				RetryCount: int32(msg.RetryCount()),
				AckId:      ackID,
			}
			if !msg.Timestamp().IsZero() {
				protoMsg.Timestamp = timestamppb.New(msg.Timestamp())
			}

			if err := stream.Send(protoMsg); err != nil {
				return err
			}
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
		}
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
