package service

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// serviceMetrics holds OTel instruments for the service layer.
type serviceMetrics struct {
	publishTotal      metric.Int64Counter
	publishDuration   metric.Float64Histogram
	streamsActive     metric.Int64UpDownCounter
	messagesSentTotal metric.Int64Counter
	acksTotal         metric.Int64Counter
}

// newServiceMetrics initialises instruments using the global MeterProvider.
// If no exporter is registered, the global provider is a no-op and all
// recordings are silently discarded.
func newServiceMetrics() *serviceMetrics {
	m := otel.GetMeterProvider().Meter("event-server",
		metric.WithInstrumentationVersion("v1"))

	publishTotal, _ := m.Int64Counter("event_server.publishes_total",
		metric.WithDescription("Total number of publish requests received by the server"),
		metric.WithUnit("{request}"))

	publishDuration, _ := m.Float64Histogram("event_server.publish_duration_seconds",
		metric.WithDescription("End-to-end duration of Publish RPC calls"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0))

	streamsActive, _ := m.Int64UpDownCounter("event_server.subscribe_streams_active",
		metric.WithDescription("Number of currently active Subscribe streams"),
		metric.WithUnit("{stream}"))

	messagesSentTotal, _ := m.Int64Counter("event_server.messages_sent_total",
		metric.WithDescription("Total number of messages sent to Subscribe stream clients"),
		metric.WithUnit("{message}"))

	acksTotal, _ := m.Int64Counter("event_server.acks_total",
		metric.WithDescription("Total number of message acknowledgments processed"),
		metric.WithUnit("{ack}"))

	return &serviceMetrics{
		publishTotal:      publishTotal,
		publishDuration:   publishDuration,
		streamsActive:     streamsActive,
		messagesSentTotal: messagesSentTotal,
		acksTotal:         acksTotal,
	}
}

func (m *serviceMetrics) recordPublish(ctx context.Context, event string, start time.Time, failed bool) {
	statusVal := "success"
	if failed {
		statusVal = "error"
	}
	opts := metric.WithAttributes(
		attribute.String("event", event),
		attribute.String("status", statusVal),
	)
	m.publishTotal.Add(ctx, 1, opts)
	m.publishDuration.Record(ctx, time.Since(start).Seconds(), opts)
}

func (m *serviceMetrics) addStream(ctx context.Context, event string) {
	m.streamsActive.Add(ctx, 1, metric.WithAttributes(attribute.String("event", event)))
}

func (m *serviceMetrics) removeStream(ctx context.Context, event string) {
	m.streamsActive.Add(ctx, -1, metric.WithAttributes(attribute.String("event", event)))
}

func (m *serviceMetrics) recordMessageSent(ctx context.Context, event string) {
	m.messagesSentTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event", event)))
}

func (m *serviceMetrics) recordAck(ctx context.Context, nack bool) {
	statusVal := "ack"
	if nack {
		statusVal = "nack"
	}
	m.acksTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("status", statusVal)))
}
