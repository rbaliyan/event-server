package service_test

import (
	"context"
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
		service.WithAuthorizer(service.AllowAll()),
		service.WithLogger(slog.Default()),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
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

	// Give subscription time to establish
	time.Sleep(100 * time.Millisecond)

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

	// Receive the message
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}

	if msg.Id != pubResp.Id {
		t.Fatalf("expected message ID %s, got %s", pubResp.Id, msg.Id)
	}
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

	lis := bufconn.Listen(bufSize)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	srv := grpc.NewServer()
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
		service.WithAuthorizer(service.AllowAll()),
		service.WithAckTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
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

	time.Sleep(100 * time.Millisecond)

	_, err = client.Publish(ctx, &eventpb.PublishRequest{
		Event:   "timeout-test",
		Payload: []byte("test"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Receive message but don't ack it
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if msg.AckId == "" {
		t.Fatal("expected non-empty ack_id")
	}

	// Wait for ack timeout (the ack tracker will nack stale entries)
	time.Sleep(time.Second)

	// Try to ack after timeout - should return ok but not find the entry
	_, err = client.Ack(ctx, &eventpb.AckRequest{
		Entries: []*eventpb.AckEntry{
			{AckId: msg.AckId},
		},
	})
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}
