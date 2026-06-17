//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"github.com/rbaliyan/event/v3/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// redisAddr returns the Redis address from EVENT_REDIS_ADDR, skipping the test
// when it is unset (no backend available).
func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("EVENT_REDIS_ADDR")
	if addr == "" {
		t.Skip("EVENT_REDIS_ADDR not set; run via `just test-integration`")
	}
	return addr
}

// uniqueSuffix derives a per-test token from the test name so events and
// consumer groups never collide across tests or runs sharing one Redis.
func uniqueSuffix(t *testing.T) string {
	t.Helper()
	r := strings.NewReplacer("/", "_", " ", "_")
	return r.Replace(t.Name())
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
		client.WithDialOptions(grpc.WithContextDialer(dialer)),
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
	deadline := time.After(20 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := tr.Publish(context.Background(), event, newMessage("__probe__", "probe", []byte("probe"), nil)); err != nil {
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
		case <-time.After(300 * time.Millisecond):
			return
		}
	}
}

// recvKeyed returns the first message whose ID == id, acking and skipping
// others, or fails on timeout.
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

// tcpProxy is an in-process TCP proxy used to simulate backend outages without
// touching the container: pause() drops live connections and rejects new ones;
// resume() restores forwarding. CI-safe (no container control required).
type tcpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	paused bool
	conns  map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &tcpProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *tcpProxy) addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *tcpProxy) handle(client net.Conn) {
	p.mu.Lock()
	if p.paused {
		p.mu.Unlock()
		_ = client.Close()
		return
	}
	p.conns[client] = struct{}{}
	p.mu.Unlock()

	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	p.conns[up] = struct{}{}
	p.mu.Unlock()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, up); done <- struct{}{} }()
	<-done
	_ = client.Close()
	_ = up.Close()
}

func (p *tcpProxy) pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

func (p *tcpProxy) resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

func (p *tcpProxy) close() { _ = p.ln.Close() }
