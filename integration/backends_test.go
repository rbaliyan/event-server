//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	"github.com/rbaliyan/event/v3/transport"
	natstransport "github.com/rbaliyan/event/v3/transport/nats"
	"github.com/nats-io/nats.go"
)

// backend describes one real transport implementation to run the shared
// scenario suite against, so coverage spans multiple backends.
type backend struct {
	name         string
	newTransport func(t *testing.T) transport.Transport
}

// backends returns the set of backends configured via environment: Redis
// (EVENT_REDIS_ADDR) and/or NATS JetStream (EVENT_NATS_URL). It skips when none
// is set.
func backends(t *testing.T) []backend {
	t.Helper()
	var bs []backend
	if addr := os.Getenv("EVENT_REDIS_ADDR"); addr != "" {
		bs = append(bs, backend{"redis", func(t *testing.T) transport.Transport {
			return newRedisTransport(t, addr)
		}})
	}
	if url := os.Getenv("EVENT_NATS_URL"); url != "" {
		bs = append(bs, backend{"nats", func(t *testing.T) transport.Transport {
			return newNATSTransport(t, url)
		}})
	}
	if len(bs) == 0 {
		t.Skip("no backend configured; set EVENT_REDIS_ADDR and/or EVENT_NATS_URL")
	}
	return bs
}

// newNATSTransport builds a JetStream-backed transport against url.
func newNATSTransport(t *testing.T, url string) transport.Transport {
	t.Helper()
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(conn.Close)
	rt, err := natstransport.NewJetStream(conn, natstransport.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("nats transport: %v", err)
	}
	return rt
}

// setupBackend wires a server backed by b's transport and returns a connected
// RemoteTransport, a per-test event name, and a context.
func setupBackend(t *testing.T, b backend) (*client.RemoteTransport, string, context.Context) {
	t.Helper()
	rt := b.newTransport(t)
	tr, cleanup := connectThroughServer(t, rt)
	t.Cleanup(cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	event := "evt_" + uniqueSuffix(t)
	if err := tr.RegisterEvent(ctx, event); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	return tr, event, ctx
}

// quietLogger discards transport logs to keep integration output clean.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(noopWriter{}, nil))
}

type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestBackends runs the shared scenario suite against every configured backend,
// proving the server is genuinely backend-agnostic.
func TestBackends(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			t.Run("RoundTrip", func(t *testing.T) { scenarioRoundTrip(t, b) })
			t.Run("Ordering", func(t *testing.T) { scenarioOrdering(t, b) })
			t.Run("WorkerPool", func(t *testing.T) { scenarioWorkerPool(t, b) })
			t.Run("Broadcast", func(t *testing.T) { scenarioBroadcast(t, b) })
		})
	}
}

func scenarioRoundTrip(t *testing.T, b backend) {
	tr, event, ctx := setupBackend(t, b)
	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()
	waitReady(t, tr, sub, event)

	if err := tr.Publish(ctx, event, newMessage("real-1", "publisher-A", []byte("hello"), map[string]string{"k": "v"})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := recvKeyed(t, sub, "real-1", 5*time.Second)
	if msg.Source() != "publisher-A" {
		t.Errorf("Source = %q, want publisher-A", msg.Source())
	}
	if string(msg.Payload()) != "hello" {
		t.Errorf("Payload = %q, want hello", msg.Payload())
	}
	if msg.Metadata()["k"] != "v" {
		t.Errorf("Metadata = %v, want k=v", msg.Metadata())
	}
	if err := msg.Ack(nil); err != nil {
		t.Errorf("Ack: %v", err)
	}
}

func scenarioOrdering(t *testing.T, b backend) {
	tr, event, ctx := setupBackend(t, b)
	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()
	waitReady(t, tr, sub, event)

	const n = 10
	for i := 0; i < n; i++ {
		if err := tr.Publish(ctx, event, newMessage(fmt.Sprintf("m%02d", i), "p", []byte("x"), nil)); err != nil {
			t.Fatalf("Publish m%02d: %v", i, err)
		}
	}
	deadline := time.After(10 * time.Second)
	got := 0
	for got < n {
		select {
		case m := <-sub.Messages():
			if m.ID() == "__probe__" {
				continue
			}
			want := fmt.Sprintf("m%02d", got)
			if m.ID() != want {
				t.Fatalf("out of order: got %q, want %q", m.ID(), want)
			}
			_ = m.Ack(nil)
			got++
		case <-deadline:
			t.Fatalf("received %d/%d in order before timeout", got, n)
		}
	}
}

func scenarioWorkerPool(t *testing.T, b backend) {
	tr, event, ctx := setupBackend(t, b)
	const consumers = 2
	subs := make([]transport.Subscription, consumers)
	for i := range subs {
		s, err := tr.Subscribe(ctx, event,
			transport.WithDeliveryMode(transport.WorkerPool),
			transport.WithWorkerGroup("g1"),
			transport.WithStartFrom(transport.StartFromLatest),
		)
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		defer func() { _ = s.Close(ctx) }()
		subs[i] = s
		waitReady(t, tr, s, event)
	}

	const n = 20
	want := map[string]bool{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("wp-%02d", i)
		want[id] = true
		if err := tr.Publish(ctx, event, newMessage(id, "p", []byte("x"), nil)); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
	}

	got := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	deadline := time.Now().Add(10 * time.Second)
	for _, s := range subs {
		wg.Add(1)
		go func(s transport.Subscription) {
			defer wg.Done()
			for {
				select {
				case m := <-s.Messages():
					if !want[m.ID()] {
						_ = m.Ack(nil)
						continue
					}
					mu.Lock()
					got[m.ID()]++
					done := len(got) == n
					mu.Unlock()
					_ = m.Ack(nil)
					if done {
						return
					}
				case <-time.After(time.Until(deadline)):
					return
				}
			}
		}(s)
	}
	wg.Wait()

	if len(got) != n {
		t.Fatalf("received %d distinct of %d", len(got), n)
	}
	for id, c := range got {
		if c != 1 {
			t.Errorf("%s delivered %d times, want exactly 1", id, c)
		}
	}
}

func scenarioBroadcast(t *testing.T, b backend) {
	tr, event, ctx := setupBackend(t, b)
	const subscribers = 2
	subs := make([]transport.Subscription, subscribers)
	for i := range subs {
		s, err := tr.Subscribe(ctx, event,
			transport.WithDeliveryMode(transport.Broadcast),
			transport.WithStartFrom(transport.StartFromLatest),
		)
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		defer func() { _ = s.Close(ctx) }()
		subs[i] = s
		waitReady(t, tr, s, event)
	}

	if err := tr.Publish(ctx, event, newMessage("bc-1", "p", []byte("hi"), nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for i, s := range subs {
		m := recvKeyed(t, s, "bc-1", 5*time.Second)
		if string(m.Payload()) != "hi" {
			t.Errorf("subscriber %d payload = %q, want hi", i, m.Payload())
		}
	}
}
