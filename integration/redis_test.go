//go:build integration

// Package integration holds end-to-end tests that require a real backend.
// Run with: just test-integration, which provisions a throwaway Redis (and a
// NATS JetStream server) — auto-detecting docker, falling back to podman — and
// sets EVENT_REDIS_ADDR / EVENT_NATS_URL. In CI, GitHub Actions service/step
// containers provide the addresses. Tests skip when the env vars are unset.
//
// Shared scenarios (round-trip, ordering, worker-pool, broadcast) run against
// every configured backend in backends_test.go (TestBackends). The tests below
// are Redis-specific (replay, redelivery, outage, gateway, concurrency).
package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	redistransport "github.com/rbaliyan/event/v3/transport/redis"
	"github.com/redis/go-redis/v9"
)

// silence go-redis's internal pool logging (e.g. expected conn drops during the
// backend-outage test) to keep diagnostics clean.
type noopRedisLogger struct{}

func (noopRedisLogger) Printf(context.Context, string, ...any) {}

func init() { redis.SetLogger(noopRedisLogger{}) }

func newMessage(id, source string, payload []byte, metadata map[string]string) transport.Message {
	return message.New(id, source, payload, metadata)
}

// newRedisTransport builds a Redis Streams transport against addr with a
// per-test consumer group (isolation) and the given extra options.
func newRedisTransport(t *testing.T, addr string, opts ...redistransport.Option) transport.Transport {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	base := []redistransport.Option{
		redistransport.WithConsumerGroup("grp_" + uniqueSuffix(t)),
		redistransport.WithLogger(quietLogger()),
	}
	rt, err := redistransport.New(rdb, append(base, opts...)...)
	if err != nil {
		t.Fatalf("redis transport: %v", err)
	}
	return rt
}

// setup wires a Redis-backed server and a connected RemoteTransport, returning
// the transport, a per-test event name, and a context.
func setup(t *testing.T, opts ...redistransport.Option) (*client.RemoteTransport, string, context.Context) {
	addr := redisAddr(t)
	rt := newRedisTransport(t, addr, opts...)
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

func TestRedis_ConcurrentPublishers(t *testing.T) {
	tr, event, ctx := setup(t)

	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()
	waitReady(t, tr, sub, event)

	const publishers, per = 4, 10
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := fmt.Sprintf("p%d-%02d", p, i)
				if err := tr.Publish(ctx, event, newMessage(id, "p", []byte("x"), nil)); err != nil {
					t.Errorf("Publish %s: %v", id, err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	total := publishers * per
	seen := map[string]bool{}
	deadline := time.After(10 * time.Second)
	for len(seen) < total {
		select {
		case m := <-sub.Messages():
			if m.ID() != "__probe__" {
				seen[m.ID()] = true
			}
			_ = m.Ack(nil)
		case <-deadline:
			t.Fatalf("received %d/%d before timeout", len(seen), total)
		}
	}
}

func TestRedis_StartFromBeginning(t *testing.T) {
	tr, event, ctx := setup(t)

	// Publish BEFORE any subscriber exists.
	for i := 0; i < 3; i++ {
		if err := tr.Publish(ctx, event, newMessage(fmt.Sprintf("pre-%d", i), "p", []byte("x"), nil)); err != nil {
			t.Fatalf("pre-publish %d: %v", i, err)
		}
	}

	// Replay requires a stable worker group; Broadcast always starts at latest
	// (per the event/v3 transport contract).
	sub, err := tr.Subscribe(ctx, event,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("replay"),
		transport.WithStartFrom(transport.StartFromBeginning),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	seen := map[string]bool{}
	deadline := time.After(10 * time.Second)
	for len(seen) < 3 {
		select {
		case m := <-sub.Messages():
			if m.ID() == "__probe__" {
				_ = m.Ack(nil)
				continue
			}
			seen[m.ID()] = true
			_ = m.Ack(nil)
		case <-deadline:
			t.Fatalf("backfilled %d/3 before timeout: %v", len(seen), seen)
		}
	}
	for i := 0; i < 3; i++ {
		if !seen[fmt.Sprintf("pre-%d", i)] {
			t.Errorf("missing backfilled message pre-%d", i)
		}
	}
}
