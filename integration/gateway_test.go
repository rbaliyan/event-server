//go:build integration

package integration

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	"github.com/rbaliyan/event-server/gateway"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"google.golang.org/grpc"
)

// TestRedis_RemoteGatewaySSE drives the remote (gRPC-backed) HTTP gateway over
// a real Redis backend: a message published through the gRPC API is delivered
// over an SSE stream served by the gateway.
func TestRedis_RemoteGatewaySSE(t *testing.T) {
	addr := redisAddr(t)
	rt := newRedisTransport(t, addr)

	svc, err := service.NewService(rt, service.WithSecurityGuard(service.AllowAll()), service.WithLogger(slog.Default()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(svc.Stop)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(svc.UnaryInterceptor()),
		grpc.StreamInterceptor(svc.StreamInterceptor()),
	)
	eventpb.RegisterEventServiceServer(grpcSrv, svc)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	// Remote gateway dialing the real gRPC address.
	h, err := gateway.NewHandler(context.Background(), lis.Addr().String(), gateway.WithInsecure())
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	httpSrv := httptest.NewServer(h)
	t.Cleanup(httpSrv.Close)

	// A client (also over the real gRPC address) to register and publish.
	pub, err := client.New(lis.Addr().String(), client.WithInsecure())
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close(context.Background()) })
	if err := pub.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	event := "evt_" + uniqueSuffix(t)
	if err := pub.RegisterEvent(ctx, event); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	// Open the SSE stream through the gateway.
	req, _ := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/events/"+event+"/stream?start_from=latest", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200", resp.StatusCode)
	}

	var mu sync.Mutex
	var buf strings.Builder
	go func() {
		b := make([]byte, 4096)
		for {
			n, rerr := resp.Body.Read(b)
			if n > 0 {
				mu.Lock()
				buf.Write(b[:n])
				mu.Unlock()
			}
			if rerr != nil {
				return
			}
		}
	}()
	contains := func(s string) bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(buf.String(), s)
	}

	// Probe until the SSE subscription is live, then publish the real message.
	deadline := time.After(20 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for !contains("probe") {
		_ = pub.Publish(ctx, event, newMessage("__probe__", "p", []byte("probe"), nil))
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatal("SSE stream did not become ready")
		}
	}

	if err := pub.Publish(ctx, event, newMessage("gw-real", "p", []byte("over-gateway"), nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for !contains("gw-real") {
		select {
		case <-time.After(200 * time.Millisecond):
		case <-deadline:
			mu.Lock()
			body := buf.String()
			mu.Unlock()
			t.Fatalf("SSE message not delivered through gateway; got:\n%s", body)
		}
	}
}
