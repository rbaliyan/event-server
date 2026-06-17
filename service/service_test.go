package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"github.com/rbaliyan/event/v3/transport/channel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setup(t *testing.T) (eventpb.EventServiceClient, func()) {
	t.Helper()

	ch := channel.New()
	svc, err := service.NewService(ch,
		service.WithSecurityGuard(service.AllowAll()),
		service.WithLogger(slog.Default()),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(svc.UnaryInterceptor()),
		grpc.StreamInterceptor(svc.StreamInterceptor()),
	)
	eventpb.RegisterEventServiceServer(srv, svc)

	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := eventpb.NewEventServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		svc.Stop()
		_ = ch.Close(context.Background())
	}

	return client, cleanup
}

func TestRegisterEvent(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()

	// Register an event
	_, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "test.event"})
	if err != nil {
		t.Fatalf("RegisterEvent failed: %v", err)
	}

	// List events should include it
	resp, err := client.ListEvents(ctx, &eventpb.ListEventsRequest{})
	if err != nil {
		t.Fatalf("ListEvents failed: %v", err)
	}
	if len(resp.Events) != 1 || resp.Events[0] != "test.event" {
		t.Fatalf("expected [test.event], got %v", resp.Events)
	}
}

func TestRegisterEvent_EmptyName(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	_, err := client.RegisterEvent(context.Background(), &eventpb.RegisterEventRequest{Name: ""})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestUnregisterEvent(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "test.event"})
	if err != nil {
		t.Fatalf("RegisterEvent failed: %v", err)
	}

	_, err = client.UnregisterEvent(ctx, &eventpb.UnregisterEventRequest{Name: "test.event"})
	if err != nil {
		t.Fatalf("UnregisterEvent failed: %v", err)
	}

	resp, err := client.ListEvents(ctx, &eventpb.ListEventsRequest{})
	if err != nil {
		t.Fatalf("ListEvents failed: %v", err)
	}
	if len(resp.Events) != 0 {
		t.Fatalf("expected empty events, got %v", resp.Events)
	}
}

func TestPublishAndSubscribe(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()

	// Register event
	_, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("RegisterEvent failed: %v", err)
	}

	// Start subscribe stream
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Subscribe(subCtx, &eventpb.SubscribeRequest{
		Event:        "orders",
		DeliveryMode: eventpb.DeliveryMode_DELIVERY_MODE_BROADCAST,
		StartFrom:    eventpb.StartPosition_START_POSITION_LATEST,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Deterministically wait until the subscription is registered server-side.
	r := readStream(stream)
	waitReady(t, client, r, "orders")

	// Publish a message
	pubResp, err := client.Publish(ctx, &eventpb.PublishRequest{
		Event:   "orders",
		Payload: []byte(`{"id":"order-1"}`),
		Metadata: map[string]string{
			"source": "test",
		},
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if pubResp.Id == "" {
		t.Fatal("expected non-empty message ID")
	}

	// Receive the message (recvUntil matches by ID, skipping readiness probes).
	msg := recvUntil(t, r, pubResp.Id, 3*time.Second)

	if string(msg.Payload) != `{"id":"order-1"}` {
		t.Fatalf("expected payload %q, got %q", `{"id":"order-1"}`, string(msg.Payload))
	}
	if msg.Metadata["source"] != "test" {
		t.Fatalf("expected metadata source=test, got %v", msg.Metadata)
	}
	if msg.AckId == "" {
		t.Fatal("expected non-empty ack_id")
	}

	// Ack the message
	_, err = client.Ack(ctx, &eventpb.AckRequest{
		Entries: []*eventpb.AckEntry{
			{AckId: msg.AckId},
		},
	})
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}

func TestPublish_EventNotRegistered(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	_, err := client.Publish(context.Background(), &eventpb.PublishRequest{
		Event:   "nonexistent",
		Payload: []byte("test"),
	})
	if err == nil {
		t.Fatal("expected error for unregistered event")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestHealth(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	resp, err := client.Health(context.Background(), &eventpb.HealthRequest{})
	if err != nil {
		t.Fatalf("Health failed: %v", err)
	}
	if resp.Status != eventpb.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Fatalf("expected healthy, got %v", resp.Status)
	}
}

func TestDenyAllAuthorizer(t *testing.T) {
	ch := channel.New()
	svc, err := service.NewService(ch) // Default is DenyAll
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(svc.UnaryInterceptor()),
		grpc.StreamInterceptor(svc.StreamInterceptor()),
	)
	eventpb.RegisterEventServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() {
		_ = conn.Close()
		srv.Stop()
		svc.Stop()
		_ = ch.Close(context.Background())
	}()

	client := eventpb.NewEventServiceClient(conn)

	_, err = client.RegisterEvent(context.Background(), &eventpb.RegisterEventRequest{Name: "test"})
	if err == nil {
		t.Fatal("expected PermissionDenied")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestSubscribeStreamClose(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "events"})
	if err != nil {
		t.Fatalf("RegisterEvent failed: %v", err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	stream, err := client.Subscribe(subCtx, &eventpb.SubscribeRequest{
		Event:     "events",
		StartFrom: eventpb.StartPosition_START_POSITION_LATEST,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Cancel the context to close the stream
	cancel()

	// Recv should return an error
	_, err = stream.Recv()
	if err == nil || err == io.EOF {
		t.Fatal("expected error after context cancel")
	}
}

func TestAckTimeout(t *testing.T) {
	ch := channel.New()
	svc, err := service.NewService(ch,
		service.WithSecurityGuard(service.AllowAll()),
		service.WithAckTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(svc.UnaryInterceptor()),
		grpc.StreamInterceptor(svc.StreamInterceptor()),
	)
	eventpb.RegisterEventServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() {
		_ = conn.Close()
		srv.Stop()
		svc.Stop()
		_ = ch.Close(context.Background())
	}()

	client := eventpb.NewEventServiceClient(conn)
	ctx := context.Background()

	// Register and publish
	_, err = client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "timeout-test"})
	if err != nil {
		t.Fatalf("RegisterEvent failed: %v", err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Subscribe(subCtx, &eventpb.SubscribeRequest{
		Event:     "timeout-test",
		StartFrom: eventpb.StartPosition_START_POSITION_LATEST,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	r := readStream(stream)
	waitReady(t, client, r, "timeout-test")

	pubResp, err := client.Publish(ctx, &eventpb.PublishRequest{
		Event:   "timeout-test",
		Payload: []byte("test"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Receive the message but do not ack it.
	msg := recvUntil(t, r, pubResp.Id, 3*time.Second)
	if msg.AckId == "" {
		t.Fatal("expected non-empty ack_id")
	}

	// A late Ack (after the entry may have been reaped) must be idempotent: no
	// error whether or not the entry still exists. Deterministic reaping is
	// covered by TestAckTracker_ReapStaleEntries.
	_, err = client.Ack(ctx, &eventpb.AckRequest{
		Entries: []*eventpb.AckEntry{
			{AckId: msg.AckId},
		},
	})
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}

// stubGuard lets tests inject arbitrary Authenticate/Authorize results.
type stubGuard struct {
	authNErr error
	authZErr error
	decision service.Decision
	identity service.Identity
}

func (g *stubGuard) Authenticate(context.Context) (service.Identity, error) {
	if g.authNErr != nil {
		return nil, g.authNErr
	}
	if g.identity != nil {
		return g.identity, nil
	}
	return service.AllowAll().Authenticate(context.Background())
}

func (g *stubGuard) Authorize(context.Context, service.Identity, string) (service.Decision, error) {
	if g.authZErr != nil {
		return service.Decision{}, g.authZErr
	}
	if g.decision == (service.Decision{}) {
		return service.Decision{Allowed: true, Scope: "all"}, nil
	}
	return g.decision, nil
}

// setupWithGuard builds a bufconn-backed service using the given guard. The
// returned cleanup function releases all resources.
func setupWithGuard(t *testing.T, guard service.SecurityGuard) (eventpb.EventServiceClient, func()) {
	t.Helper()

	ch := channel.New()
	svc, err := service.NewService(ch, service.WithSecurityGuard(guard))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(svc.UnaryInterceptor()),
		grpc.StreamInterceptor(svc.StreamInterceptor()),
	)
	eventpb.RegisterEventServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	client := eventpb.NewEventServiceClient(conn)
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		svc.Stop()
		_ = ch.Close(context.Background())
	}
	return client, cleanup
}

// TestInterceptor_AuthenticateError asserts that an Authenticate error is
// surfaced as codes.Unauthenticated with a generic message (no internals).
func TestInterceptor_AuthenticateError(t *testing.T) {
	client, cleanup := setupWithGuard(t, &stubGuard{
		authNErr: errors.New("bad jwt signature kid=abc123"),
	})
	defer cleanup()

	_, err := client.RegisterEvent(context.Background(), &eventpb.RegisterEventRequest{Name: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a status error: %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", st.Code())
	}
	if st.Message() != "authentication failed" {
		t.Fatalf("message leaked guard internals: %q", st.Message())
	}
}

// TestInterceptor_AuthorizeError asserts that an Authorize error is surfaced
// as codes.Internal with a generic message.
func TestInterceptor_AuthorizeError(t *testing.T) {
	client, cleanup := setupWithGuard(t, &stubGuard{
		authZErr: errors.New("policy backend unreachable dsn=postgres://user:pw@db"),
	})
	defer cleanup()

	_, err := client.RegisterEvent(context.Background(), &eventpb.RegisterEventRequest{Name: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a status error: %v", err)
	}
	if st.Code() != codes.Internal {
		t.Fatalf("code = %v, want Internal", st.Code())
	}
	if st.Message() != "authorization error" {
		t.Fatalf("message leaked guard internals: %q", st.Message())
	}
}

// TestInterceptor_DecisionDenied asserts that a denied Decision propagates its
// Reason as the PermissionDenied message.
func TestInterceptor_DecisionDenied(t *testing.T) {
	client, cleanup := setupWithGuard(t, &stubGuard{
		decision: service.Decision{Allowed: false, Reason: "tenant quota exceeded"},
	})
	defer cleanup()

	_, err := client.RegisterEvent(context.Background(), &eventpb.RegisterEventRequest{Name: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a status error: %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied", st.Code())
	}
	if st.Message() != "tenant quota exceeded" {
		t.Fatalf("message = %q, want decision reason", st.Message())
	}
}

// TestInterceptor_HealthExempt asserts that Health remains reachable under the
// default DenyAll guard so that liveness probes continue to work.
func TestInterceptor_HealthExempt(t *testing.T) {
	client, cleanup := setupWithGuard(t, service.DenyAll())
	defer cleanup()

	resp, err := client.Health(context.Background(), &eventpb.HealthRequest{})
	if err != nil {
		t.Fatalf("Health under DenyAll must succeed, got: %v", err)
	}
	if resp.Status != eventpb.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Fatalf("status = %v, want HEALTHY", resp.Status)
	}

	// Sanity-check: another RPC is still denied under DenyAll.
	_, err = client.RegisterEvent(context.Background(), &eventpb.RegisterEventRequest{Name: "x"})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("RegisterEvent under DenyAll should be denied, got: %v", err)
	}
}

// TestApplyGuard_AllowAllPopulatesIdentity verifies ApplyGuard returns an
// identity-bearing context on success, enabling in-process callers to
// observe the identity via IdentityFromContext.
func TestApplyGuard_AllowAllPopulatesIdentity(t *testing.T) {
	ch := channel.New()
	defer func() { _ = ch.Close(context.Background()) }()

	svc, err := service.NewService(ch, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Stop()

	ctx, err := svc.ApplyGuard(context.Background(), "/event.v1.EventService/Publish")
	if err != nil {
		t.Fatalf("ApplyGuard: %v", err)
	}
	id, ok := service.IdentityFromContext(ctx)
	if !ok {
		t.Fatal("expected identity in context")
	}
	if id.Claims() == nil {
		t.Fatal("AllowAll identity must return a non-nil Claims map")
	}
}

// TestApplyGuard_HealthExempt verifies the Health full method name is
// treated as exempt and does not invoke the guard.
func TestApplyGuard_HealthExempt(t *testing.T) {
	ch := channel.New()
	defer func() { _ = ch.Close(context.Background()) }()

	svc, err := service.NewService(ch) // default DenyAll
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer svc.Stop()

	if _, err := svc.ApplyGuard(context.Background(), "/event.v1.EventService/Health"); err != nil {
		t.Fatalf("ApplyGuard Health must not fail under DenyAll, got: %v", err)
	}
}

// TestSubscribeStreamClose_PendingAckNacked verifies that when a Subscribe stream
// closes, pending ack_ids are nacked by the ack tracker. A subsequent Ack call
// for the same ack_id must succeed (idempotent) without finding the entry.
func TestSubscribeStreamClose_PendingAckNacked(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "close-test"})
	if err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	stream, err := client.Subscribe(subCtx, &eventpb.SubscribeRequest{
		Event:     "close-test",
		StartFrom: eventpb.StartPosition_START_POSITION_LATEST,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	r := readStream(stream)
	waitReady(t, client, r, "close-test")

	pubResp, err := client.Publish(ctx, &eventpb.PublishRequest{
		Event:   "close-test",
		Payload: []byte(`"x"`),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := recvUntil(t, r, pubResp.Id, 3*time.Second)
	ackID := msg.AckId
	if ackID == "" {
		t.Fatal("expected non-empty ack_id")
	}

	// Cancel the stream — triggers NackStream(streamID, errStreamClosed) on the
	// server. Scoped nacking itself is covered by TestAckTracker_NackStreamScopesToStream.
	cancel()

	// Ack after stream close must remain idempotent — no error, no panic —
	// whether or not NackStream has already consumed the ack_id.
	_, err = client.Ack(ctx, &eventpb.AckRequest{
		Entries: []*eventpb.AckEntry{{AckId: ackID}},
	})
	if err != nil {
		t.Fatalf("Ack after stream close must be idempotent, got: %v", err)
	}
}
