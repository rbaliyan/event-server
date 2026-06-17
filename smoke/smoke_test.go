//go:build smoke

// Package smoke holds a fast, dependency-free pre-merge gate: one in-process
// round trip per entry point (gRPC service + RemoteTransport client, HTTP
// gateway, and the default-deny security posture). Run with: just smoke
// (go test -tags smoke ./...). It uses only bufconn + the channel transport,
// so it needs no external services and completes in ~1s.
package smoke

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	"github.com/rbaliyan/event-server/gateway"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
	"github.com/rbaliyan/event/v3/transport/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func bufDialer(t *testing.T, guard service.SecurityGuard) (func(context.Context, string) (net.Conn, error), *service.Service, func()) {
	t.Helper()
	ch := channel.New()
	svc, err := service.NewService(ch, service.WithSecurityGuard(guard), service.WithLogger(slog.Default()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.UnaryInterceptor(svc.UnaryInterceptor()), grpc.StreamInterceptor(svc.StreamInterceptor()))
	eventpb.RegisterEventServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	cleanup := func() { srv.Stop(); svc.Stop(); _ = ch.Close(context.Background()) }
	return dialer, svc, cleanup
}

func connect(t *testing.T, dialer func(context.Context, string) (net.Conn, error)) *client.RemoteTransport {
	t.Helper()
	tr, err := client.New("passthrough://bufnet", client.WithInsecure(), client.WithDialOptions(grpc.WithContextDialer(dialer)))
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	if err := tr.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return tr
}

// TestSmoke_PublishSubscribeAck is the core happy path through the real
// RemoteTransport, service, and channel transport.
func TestSmoke_PublishSubscribeAck(t *testing.T) {
	dialer, _, cleanup := bufDialer(t, service.AllowAll())
	defer cleanup()
	tr := connect(t, dialer)
	defer func() { _ = tr.Close(context.Background()) }()

	ctx := context.Background()
	const event = "smoke.evt"
	if err := tr.RegisterEvent(ctx, event); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	// Probe until the subscription is live, then publish the real message.
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
ready:
	for {
		_ = tr.Publish(ctx, event, message.New("__probe__", "p", []byte("p"), nil))
		select {
		case <-sub.Messages():
			break ready
		case <-tick.C:
		case <-deadline:
			t.Fatal("subscription not ready")
		}
	}
	if err := tr.Publish(ctx, event, message.New("real", "publisher", []byte("hi"), nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	stop := time.After(3 * time.Second)
	for {
		select {
		case m := <-sub.Messages():
			if m.ID() == "real" {
				if string(m.Payload()) != "hi" {
					t.Fatalf("payload = %q, want hi", m.Payload())
				}
				if err := m.Ack(nil); err != nil {
					t.Fatalf("Ack: %v", err)
				}
				return
			}
		case <-stop:
			t.Fatal("did not receive real message")
		}
	}
}

// TestSmoke_GatewayHealth confirms the HTTP gateway boots and serves health.
func TestSmoke_GatewayHealth(t *testing.T) {
	ch := channel.New()
	svc, err := service.NewService(ch, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer func() { svc.Stop(); _ = ch.Close(context.Background()) }()

	h, err := gateway.NewInProcessHandler(context.Background(), svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler: %v", err)
	}
	defer func() { _ = h.Close() }()

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/health")
	if err != nil {
		t.Fatalf("GET health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", resp.StatusCode)
	}
}

// TestSmoke_DenyAllDefault confirms the safe default: with no guard configured,
// operations are denied (surfacing as a permission error through the client).
func TestSmoke_DenyAllDefault(t *testing.T) {
	dialer, _, cleanup := bufDialer(t, service.DenyAll())
	defer cleanup()
	tr := connect(t, dialer)
	defer func() { _ = tr.Close(context.Background()) }()

	err := tr.RegisterEvent(context.Background(), "smoke.evt")
	if err == nil {
		t.Fatal("expected RegisterEvent to be denied under DenyAll")
	}
	// Assert it is specifically a permission denial, not an incidental error
	// (e.g. a transport failure) that would give false confidence.
	if !errors.Is(err, client.ErrPermissionDenied) {
		t.Fatalf("error = %v, want a permission-denied error", err)
	}
}
