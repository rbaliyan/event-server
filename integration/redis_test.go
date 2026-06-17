//go:build integration

// Package integration holds end-to-end tests that require a real backend
// (e.g. Redis). Run with: just test-integration, which provisions a throwaway
// Redis (auto-detecting docker, falling back to podman) and sets
// EVENT_REDIS_ADDR. In CI, a GitHub Actions `services: redis` container
// provides the address. Tests skip when EVENT_REDIS_ADDR is unset.
package integration

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	redistransport "github.com/rbaliyan/event/v3/transport/redis"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// TestRedisTransport_RoundTrip exercises the full server stack
// (RemoteTransport -> gRPC -> Service -> Redis Streams transport) against a
// real Redis instance addressed by EVENT_REDIS_ADDR.
func TestRedisTransport_RoundTrip(t *testing.T) {
	addr := os.Getenv("EVENT_REDIS_ADDR")
	if addr == "" {
		t.Skip("EVENT_REDIS_ADDR not set; run via `just test-integration`")
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	rt, err := redistransport.New(rdb, redistransport.WithConsumerGroup("itest"))
	if err != nil {
		t.Fatalf("redis transport: %v", err)
	}

	tr, cleanup := connectThroughServer(t, rt)
	defer cleanup()

	ctx := context.Background()
	const event = "orders"

	if err := tr.RegisterEvent(ctx, event); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	waitReady(t, tr, sub, event)

	if err := tr.Publish(ctx, event, message.New("real-1", "publisher-A", []byte("hello"), map[string]string{"k": "v"})); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := recvKeyed(t, sub, "real-1", 5*time.Second)
	if got := msg.Source(); got != "publisher-A" {
		t.Fatalf("Source = %q, want publisher-A", got)
	}
	if string(msg.Payload()) != "hello" {
		t.Fatalf("Payload = %q, want hello", msg.Payload())
	}
	if msg.Metadata()["k"] != "v" {
		t.Fatalf("Metadata = %v, want k=v", msg.Metadata())
	}

	if err := msg.Ack(nil); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// connectThroughServer stands up an in-process gRPC server backed by the given
// transport and returns a connected RemoteTransport plus cleanup.
func connectThroughServer(t *testing.T, backing transport.Transport) (*client.RemoteTransport, func()) {
	t.Helper()

	svc, err := service.NewService(backing,
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

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	tr, err := client.New("passthrough://bufnet",
		client.WithInsecure(),
		client.WithDialOptions(grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	if err := tr.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	cleanup := func() {
		_ = tr.Close(context.Background())
		srv.Stop()
		svc.Stop()
	}
	return tr, cleanup
}

// waitReady drives probe messages until one round-trips, proving the
// subscription is live, then drains queued probes.
func waitReady(t *testing.T, tr *client.RemoteTransport, sub transport.Subscription, event string) {
	t.Helper()

	deadline := time.After(15 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := tr.Publish(context.Background(), event, message.New("__probe__", "probe", []byte("probe"), nil)); err != nil {
			t.Fatalf("probe publish: %v", err)
		}
		select {
		case <-sub.Messages():
			drain(sub)
			return
		case <-tick.C:
		case <-deadline:
			t.Fatal("subscription did not become ready in time")
		}
	}
}

func drain(sub transport.Subscription) {
	for {
		select {
		case <-sub.Messages():
		case <-time.After(200 * time.Millisecond):
			return
		}
	}
}

func recvKeyed(t *testing.T, sub transport.Subscription, id string, timeout time.Duration) transport.Message {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-sub.Messages():
			if !ok {
				t.Fatalf("subscription closed before %q", id)
			}
			if msg.ID() == id {
				return msg
			}
			_ = msg.Ack(nil)
		case <-deadline:
			t.Fatalf("timed out waiting for %q", id)
		}
	}
}
