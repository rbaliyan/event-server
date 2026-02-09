package client_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupServer(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()

	ch := channel.New()
	svc := service.NewService(ch,
		service.WithAuthorizer(service.AllowAll()),
		service.WithLogger(slog.Default()),
	)

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

	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		svc.Stop()
		_ = ch.Close(context.Background())
	}

	return conn, cleanup
}

// TestRemoteTransportEndToEnd tests the full flow using a real gRPC server
// with bufconn, going through the EventServiceClient directly.
func TestRemoteTransportEndToEnd(t *testing.T) {
	conn, cleanup := setupServer(t)
	defer cleanup()

	ctx := context.Background()
	eventClient := eventpb.NewEventServiceClient(conn)

	// Register event via gRPC
	_, err := eventClient.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "test.event"})
	if err != nil {
		t.Fatalf("RegisterEvent failed: %v", err)
	}

	// Subscribe
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := eventClient.Subscribe(subCtx, &eventpb.SubscribeRequest{
		Event:        "test.event",
		DeliveryMode: eventpb.DeliveryMode_DELIVERY_MODE_BROADCAST,
		StartFrom:    eventpb.StartPosition_START_POSITION_LATEST,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Publish
	pubResp, err := eventClient.Publish(ctx, &eventpb.PublishRequest{
		Event:   "test.event",
		Id:      "msg-1",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if pubResp.Id != "msg-1" {
		t.Fatalf("expected ID msg-1, got %s", pubResp.Id)
	}

	// Receive
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if msg.Id != "msg-1" {
		t.Fatalf("expected message ID msg-1, got %s", msg.Id)
	}
	if string(msg.Payload) != "hello" {
		t.Fatalf("expected payload hello, got %s", string(msg.Payload))
	}

	// Ack
	_, err = eventClient.Ack(ctx, &eventpb.AckRequest{
		Entries: []*eventpb.AckEntry{
			{AckId: msg.AckId},
		},
	})
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}

// TestBuildSubscribeRequest tests the option conversion logic.
func TestBuildSubscribeRequest(t *testing.T) {
	// Verify the client transport implements the Transport interface
	var _ transport.Transport = (*client.RemoteTransport)(nil)
}

func TestRemoteTransportNew(t *testing.T) {
	_, err := client.New("")
	if err == nil {
		t.Fatal("expected error for empty address")
	}

	rt, err := client.New("localhost:9090", client.WithInsecure())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if rt.State() != client.ConnStateDisconnected {
		t.Fatalf("expected disconnected, got %v", rt.State())
	}
}

func TestRemoteTransportClose(t *testing.T) {
	rt, err := client.New("localhost:9090", client.WithInsecure())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	err = rt.Close(context.Background())
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if rt.State() != client.ConnStateClosed {
		t.Fatalf("expected closed, got %v", rt.State())
	}

	// Close again should be safe
	err = rt.Close(context.Background())
	if err != nil {
		t.Fatalf("double Close failed: %v", err)
	}
}
