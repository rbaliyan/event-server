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
	"google.golang.org/grpc/test/bufconn"
)

// bufServer starts an in-process event server backed by the channel transport
// and returns a gRPC context dialer for it plus a cleanup func.
func bufServer(t *testing.T, guard service.SecurityGuard) (func(context.Context, string) (net.Conn, error), func()) {
	t.Helper()

	ch := channel.New()
	svc, err := service.NewService(ch,
		service.WithSecurityGuard(guard),
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

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	cleanup := func() {
		srv.Stop()
		svc.Stop()
		_ = ch.Close(context.Background())
	}
	return dialer, cleanup
}

// newConnectedTransport builds a RemoteTransport wired to an in-process server
// (AllowAll guard) and returns it already connected, plus a cleanup func.
func newConnectedTransport(t *testing.T, opts ...client.Option) (*client.RemoteTransport, func()) {
	t.Helper()

	dialer, srvCleanup := bufServer(t, service.AllowAll())
	base := []client.Option{
		client.WithInsecure(),
		client.WithDialOptions(grpc.WithContextDialer(dialer)),
	}
	tr, err := client.New("passthrough://bufnet", append(base, opts...)...)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	if err := tr.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	cleanup := func() {
		_ = tr.Close(context.Background())
		srvCleanup()
	}
	return tr, cleanup
}

// waitReady drives probe messages through a live subscription until one
// round-trips, proving the server-side subscription is registered. It drains
// every probe it sees so the caller starts from an empty channel. This is a
// deterministic replacement for time.Sleep-based "let it establish" waits.
func waitReady(t *testing.T, tr *client.RemoteTransport, sub transport.Subscription, event string) {
	t.Helper()

	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		if err := tr.Publish(context.Background(), event, newMessage("__probe__", "probe", []byte("probe"), nil)); err != nil {
			t.Fatalf("probe publish: %v", err)
		}
		select {
		case <-sub.Messages():
			// Drain any further probes already queued, then return.
			drainProbes(sub)
			return
		case <-tick.C:
		case <-deadline:
			t.Fatal("subscription did not become ready within 5s")
		}
	}
}

// drainProbes removes any immediately-available probe messages from the channel.
func drainProbes(sub transport.Subscription) {
	for {
		select {
		case <-sub.Messages():
		case <-time.After(20 * time.Millisecond):
			return
		}
	}
}

// recvKeyed waits for a message whose ID matches id, skipping others, or fails.
func recvKeyed(t *testing.T, sub transport.Subscription, id string, timeout time.Duration) transport.Message {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-sub.Messages():
			if !ok {
				t.Fatalf("subscription channel closed before receiving %q", id)
			}
			if msg.ID() == id {
				return msg
			}
		case <-deadline:
			t.Fatalf("timed out waiting for message %q", id)
		}
	}
}
