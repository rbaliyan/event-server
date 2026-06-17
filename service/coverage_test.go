package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"google.golang.org/grpc/metadata"
)

// streamReader pumps a server-streaming Subscribe into channels so tests can
// wait for messages with timeouts (gRPC's blocking Recv has no deadline). One
// reader goroutine per stream avoids concurrent-Recv races.
type streamReader struct {
	msgs chan *eventpb.Message
	errs chan error
}

func readStream(stream eventpb.EventService_SubscribeClient) *streamReader {
	r := &streamReader{
		msgs: make(chan *eventpb.Message, 256),
		errs: make(chan error, 1),
	}
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				r.errs <- err
				return
			}
			r.msgs <- m
		}
	}()
	return r
}

const probeID = "__probe__"

// waitReady publishes probe messages until this reader observes one, proving
// the subscription is registered server-side. Deterministic replacement for a
// fixed time.Sleep. Probes are keyed by probeID so recvUntil can skip them.
func waitReady(t *testing.T, client eventpb.EventServiceClient, r *streamReader, event string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := client.Publish(context.Background(), &eventpb.PublishRequest{
			Event: event, Id: probeID, Payload: []byte("probe"),
		}); err != nil {
			t.Fatalf("probe publish: %v", err)
		}
		select {
		case <-r.msgs:
			return
		case err := <-r.errs:
			t.Fatalf("stream error during readiness: %v", err)
		case <-tick.C:
		case <-deadline:
			t.Fatal("subscription did not become ready within 5s")
		}
	}
}

// recvUntil returns the first message whose Id == id, skipping probes/others.
func recvUntil(t *testing.T, r *streamReader, id string, timeout time.Duration) *eventpb.Message {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case m := <-r.msgs:
			if m.Id == id {
				return m
			}
		case err := <-r.errs:
			t.Fatalf("stream error waiting for %q: %v", id, err)
		case <-deadline:
			t.Fatalf("timed out waiting for message %q", id)
		}
	}
}

// TestBroadcast_MultiSubscriber asserts a single publish reaches every
// broadcast subscriber.
func TestBroadcast_MultiSubscriber(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	const event = "broadcast.evt"

	if _, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: event}); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	const n = 3
	readers := make([]*streamReader, n)
	for i := range readers {
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream, err := client.Subscribe(subCtx, &eventpb.SubscribeRequest{
			Event:        event,
			DeliveryMode: eventpb.DeliveryMode_DELIVERY_MODE_BROADCAST,
			StartFrom:    eventpb.StartPosition_START_POSITION_LATEST,
		})
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		readers[i] = readStream(stream)
		waitReady(t, client, readers[i], event)
	}

	if _, err := client.Publish(ctx, &eventpb.PublishRequest{
		Event: event, Id: "real-1", Payload: []byte("hello"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	for i, r := range readers {
		m := recvUntil(t, r, "real-1", 3*time.Second)
		if string(m.Payload) != "hello" {
			t.Fatalf("subscriber %d payload = %q, want hello", i, m.Payload)
		}
	}
}

// TestWorkerPool_DistributesAcrossGroup asserts that within one worker group
// each published message is delivered to exactly one consumer (no duplication).
func TestWorkerPool_DistributesAcrossGroup(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	const event = "workerpool.evt"

	if _, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: event}); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	const consumers = 2
	readers := make([]*streamReader, consumers)
	for i := range readers {
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream, err := client.Subscribe(subCtx, &eventpb.SubscribeRequest{
			Event:        event,
			DeliveryMode: eventpb.DeliveryMode_DELIVERY_MODE_WORKER_POOL,
			WorkerGroup:  "g1",
			StartFrom:    eventpb.StartPosition_START_POSITION_LATEST,
		})
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		readers[i] = readStream(stream)
	}
	// Readiness: both consumers must be registered before real publishes. Probe
	// until each reader has seen at least one probe.
	for _, r := range readers {
		waitReady(t, client, r, event)
	}

	const n = 20
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		id := "wp-" + itoa(i)
		want[id] = true
		if _, err := client.Publish(ctx, &eventpb.PublishRequest{Event: event, Id: id, Payload: []byte("x")}); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
	}

	got := make(map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	deadline := time.Now().Add(5 * time.Second)
	for _, r := range readers {
		wg.Add(1)
		go func(r *streamReader) {
			defer wg.Done()
			for {
				select {
				case m := <-r.msgs:
					if m.Id == probeID {
						continue
					}
					mu.Lock()
					got[m.Id]++
					done := len(got) == n
					mu.Unlock()
					if done {
						return
					}
				case <-time.After(time.Until(deadline)):
					return
				}
			}
		}(r)
	}
	wg.Wait()

	if len(got) != n {
		t.Fatalf("received %d distinct messages, want %d", len(got), n)
	}
	for id, c := range got {
		if c != 1 {
			t.Fatalf("message %s delivered %d times, want exactly 1 (worker-pool must not duplicate)", id, c)
		}
		if !want[id] {
			t.Fatalf("received unexpected id %s", id)
		}
	}
}

// TestPublish_SourceFromMetadata asserts the x-source header sets the message
// source, defaulting to "remote" when absent.
func TestPublish_SourceFromMetadata(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	const event = "source.evt"

	if _, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: event}); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Subscribe(subCtx, &eventpb.SubscribeRequest{
		Event: event, StartFrom: eventpb.StartPosition_START_POSITION_LATEST,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	r := readStream(stream)
	waitReady(t, client, r, event)

	// With x-source header.
	srcCtx := metadata.AppendToOutgoingContext(ctx, "x-source", "svc-A")
	if _, err := client.Publish(srcCtx, &eventpb.PublishRequest{Event: event, Id: "with-src", Payload: []byte("a")}); err != nil {
		t.Fatalf("Publish with source: %v", err)
	}
	if m := recvUntil(t, r, "with-src", 3*time.Second); m.Source != "svc-A" {
		t.Fatalf("Source = %q, want svc-A", m.Source)
	}

	// Without header -> default "remote".
	if _, err := client.Publish(ctx, &eventpb.PublishRequest{Event: event, Id: "no-src", Payload: []byte("b")}); err != nil {
		t.Fatalf("Publish without source: %v", err)
	}
	if m := recvUntil(t, r, "no-src", 3*time.Second); m.Source != "remote" {
		t.Fatalf("Source = %q, want remote (default)", m.Source)
	}
}

// TestConcurrentPublishers exercises concurrent Publish under -race and asserts
// all messages reach a broadcast subscriber.
func TestConcurrentPublishers(t *testing.T) {
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	const event = "concurrent.evt"

	if _, err := client.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: event}); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Subscribe(subCtx, &eventpb.SubscribeRequest{
		Event: event, StartFrom: eventpb.StartPosition_START_POSITION_LATEST,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	r := readStream(stream)
	waitReady(t, client, r, event)

	const publishers, perPublisher = 4, 10
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				id := "p" + itoa(p) + "-" + itoa(i)
				if _, err := client.Publish(ctx, &eventpb.PublishRequest{Event: event, Id: id, Payload: []byte("x")}); err != nil {
					t.Errorf("Publish %s: %v", id, err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	total := publishers * perPublisher
	seen := make(map[string]bool, total)
	deadline := time.After(5 * time.Second)
	for len(seen) < total {
		select {
		case m := <-r.msgs:
			if m.Id != probeID {
				seen[m.Id] = true
			}
		case err := <-r.errs:
			t.Fatalf("stream error: %v", err)
		case <-deadline:
			t.Fatalf("received %d/%d messages before timeout", len(seen), total)
		}
	}
}

// itoa is a tiny helper to avoid importing strconv just for labels.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
