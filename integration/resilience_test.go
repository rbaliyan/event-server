//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	redistransport "github.com/rbaliyan/event/v3/transport/redis"
	"github.com/redis/go-redis/v9"
)

// TestRedis_ErrorMapping asserts that gRPC status codes are translated back to
// transport sentinels through the real client over the wire.
func TestRedis_ErrorMapping(t *testing.T) {
	tr, _, ctx := setup(t)

	// Publishing to an event that was never registered must surface as
	// transport.ErrEventNotRegistered (codes.NotFound mapped back by the client).
	err := tr.Publish(ctx, "evt_never_registered_"+uniqueSuffix(t), newMessage("x", "p", []byte("x"), nil))
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Fatalf("Publish to unregistered event = %v, want ErrEventNotRegistered", err)
	}
}

// TestRedis_NackRedelivery verifies that a message left un-acked (nacked) by one
// worker is redelivered to another worker in the same group via the Redis
// Streams pending-entries claim loop (XCLAIM of orphaned entries).
//
// Redis Streams only reclaims a pending entry on behalf of a *different*
// consumer: the claim loop deliberately skips entries it already owns, and a
// consumer's own un-acked entries are re-read only once at subscription start
// (restart recovery). So redelivery to a live consumer requires a second worker
// in the same group — the real "stalled/crashed worker, peer takes over"
// scenario.
func TestRedis_NackRedelivery(t *testing.T) {
	// Short claim interval and idle so the reclaim happens quickly.
	tr, event, ctx := setup(t, redistransport.WithClaimInterval(500*time.Millisecond, 500*time.Millisecond))

	// Publish before subscribing so the single backlog message is delivered to
	// the first worker deterministically (no consumer-group creation race, and
	// the second worker does not yet exist to compete for it).
	if err := tr.Publish(ctx, event, newMessage("nr-1", "p", []byte("x"), nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Worker 1 reads the backlog and nacks — the entry stays pending in the
	// shared group's PEL, owned by worker 1's consumer.
	worker1, err := tr.Subscribe(ctx, event,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("redeliver"),
		transport.WithStartFrom(transport.StartFromBeginning),
	)
	if err != nil {
		t.Fatalf("Subscribe worker 1: %v", err)
	}
	defer func() { _ = worker1.Close(ctx) }()

	first := recvKeyedNoAck(t, worker1, "nr-1", 5*time.Second)
	if err := first.Ack(context.DeadlineExceeded); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Worker 2 joins the same group. Its claim loop reclaims the entry worker 1
	// left idle in the PEL (XCLAIM transfers ownership across consumers) and
	// redelivers it.
	worker2, err := tr.Subscribe(ctx, event,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("redeliver"),
		transport.WithStartFrom(transport.StartFromBeginning),
	)
	if err != nil {
		t.Fatalf("Subscribe worker 2: %v", err)
	}
	defer func() { _ = worker2.Close(ctx) }()

	redelivered := recvKeyedNoAck(t, worker2, "nr-1", 10*time.Second)
	if err := redelivered.Ack(nil); err != nil {
		t.Fatalf("ack redelivered: %v", err)
	}
}

// recvKeyedNoAck waits for a message with the given ID without acking it,
// acking and skipping others.
func recvKeyedNoAck(t *testing.T, sub transport.Subscription, id string, timeout time.Duration) transport.Message {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case m, ok := <-sub.Messages():
			if !ok {
				t.Fatalf("subscription closed before %q", id)
			}
			if m.ID() == id {
				return m
			}
			_ = m.Ack(nil)
		case <-deadline:
			t.Fatalf("timed out waiting for %q", id)
		}
	}
}

// TestRedis_BackendOutage drives a real backend outage through an in-process
// TCP proxy: while the proxy is paused, Redis is unreachable and Publish must
// fail; once resumed, the server recovers and delivery resumes.
func TestRedis_BackendOutage(t *testing.T) {
	addr := redisAddr(t)
	proxy := newTCPProxy(t, addr)

	// Short timeouts so commands fail fast while the backend is unreachable.
	rdb := redis.NewClient(&redis.Options{
		Addr:         proxy.addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	rt, err := redistransport.New(rdb, redistransport.WithConsumerGroup("grp_"+uniqueSuffix(t)))
	if err != nil {
		t.Fatalf("redis transport: %v", err)
	}
	tr, cleanup := connectThroughServer(t, rt)
	t.Cleanup(cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)
	event := "evt_" + uniqueSuffix(t)
	if err := tr.RegisterEvent(ctx, event); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()
	waitReady(t, tr, sub, event)

	// Outage: publishes must fail while the backend is unreachable.
	proxy.pause()
	failed := false
	for i := 0; i < 5; i++ {
		if err := tr.Publish(ctx, event, message.New("down", "p", []byte("x"), nil)); err != nil {
			failed = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !failed {
		t.Fatal("expected Publish to fail while the Redis backend is unreachable")
	}

	// Recovery: restore the backend and assert delivery resumes.
	proxy.resume()
	deadline := time.After(20 * time.Second)
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
recovered:
	for {
		_ = tr.Publish(ctx, event, message.New("up", "p", []byte("x"), nil))
		select {
		case m := <-sub.Messages():
			_ = m.Ack(nil)
			if m.ID() == "up" {
				break recovered
			}
		case <-tick.C:
		case <-deadline:
			t.Fatal("backend did not recover after resume")
		}
	}
}
