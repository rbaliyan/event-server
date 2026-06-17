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

// TestRedis_NackRedelivery verifies that an un-acked (nacked) message is
// redelivered via the Redis Streams pending-entries claim loop.
func TestRedis_NackRedelivery(t *testing.T) {
	// Short claim interval so the reclaim happens quickly.
	tr, event, ctx := setup(t, redistransport.WithClaimInterval(500*time.Millisecond, 500*time.Millisecond))

	// Publish before subscribing, then read the backlog from a stable worker
	// group — this avoids racing the consumer-group creation and gives a single
	// deterministic message to nack and observe being redelivered.
	if err := tr.Publish(ctx, event, newMessage("nr-1", "p", []byte("x"), nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	sub, err := tr.Subscribe(ctx, event,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("redeliver"),
		transport.WithStartFrom(transport.StartFromBeginning),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	// First delivery: nack it (ack with an error leaves it pending).
	first := recvKeyedNoAck(t, sub, "nr-1", 5*time.Second)
	if err := first.Ack(context.DeadlineExceeded); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// The claim loop must redeliver the same message.
	redelivered := recvKeyedNoAck(t, sub, "nr-1", 10*time.Second)
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
