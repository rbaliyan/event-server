package service_test

import (
	"context"
	"testing"
	"time"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// sumInt64 totals the int64 Sum data points for the named instrument.
func sumInt64(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is not an int64 Sum (%T)", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	t.Fatalf("metric %q not found in collected metrics", name)
	return 0
}

// TestMetrics_RecordedOnPublishSubscribeAck installs a manual-reader meter
// provider, drives a publish -> deliver -> ack round trip, and asserts the
// service instruments moved. Not parallel: it mutates the global MeterProvider.
func TestMetrics_RecordedOnPublishSubscribeAck(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = provider.Shutdown(context.Background())
	})

	// setup() builds the service, which reads the (now manual) global provider.
	client, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	const event = "metrics.evt"

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

	if _, err := client.Publish(ctx, &eventpb.PublishRequest{Event: event, Id: "m1", Payload: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := recvUntil(t, r, "m1", 3*time.Second)
	if _, err := client.Ack(ctx, &eventpb.AckRequest{Entries: []*eventpb.AckEntry{{AckId: msg.AckId}}}); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if got := sumInt64(t, &rm, "event_server.publishes_total"); got < 1 {
		t.Errorf("publishes_total = %d, want >= 1", got)
	}
	if got := sumInt64(t, &rm, "event_server.messages_sent_total"); got < 1 {
		t.Errorf("messages_sent_total = %d, want >= 1", got)
	}
	if got := sumInt64(t, &rm, "event_server.acks_total"); got < 1 {
		t.Errorf("acks_total = %d, want >= 1", got)
	}
	// Stream is still open, so the active-streams up/down counter must be > 0.
	if got := sumInt64(t, &rm, "event_server.subscribe_streams_active"); got < 1 {
		t.Errorf("subscribe_streams_active = %d, want >= 1 while a stream is open", got)
	}
}
