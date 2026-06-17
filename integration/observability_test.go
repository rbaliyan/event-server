//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

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
	t.Fatalf("metric %q not found", name)
	return 0
}

// TestRedis_MetricsRecorded asserts the server's OTel instruments move on a
// real-backend publish/deliver/ack round trip. Not parallel: it mutates the
// global MeterProvider.
func TestRedis_MetricsRecorded(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = provider.Shutdown(context.Background())
	})

	// setup() builds the service, which reads the (now manual) global provider.
	tr, event, ctx := setup(t)

	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()
	waitReady(t, tr, sub, event)

	if err := tr.Publish(ctx, event, newMessage("m1", "p", []byte("x"), nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := recvKeyed(t, sub, "m1", 5*time.Second)
	if err := msg.Ack(nil); err != nil {
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
	if got := sumInt64(t, &rm, "event_server.subscribe_streams_active"); got < 1 {
		t.Errorf("subscribe_streams_active = %d, want >= 1 while streaming", got)
	}
}
