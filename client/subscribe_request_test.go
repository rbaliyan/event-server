package client

import (
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
)

func TestBuildSubscribeRequest(t *testing.T) {
	t.Parallel()

	startTime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		opts  *transport.SubscribeOptions
		check func(t *testing.T, req *eventpb.SubscribeRequest)
	}{
		{
			name: "delivery mode broadcast",
			opts: transport.ApplySubscribeOptions(transport.WithDeliveryMode(transport.Broadcast)),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.DeliveryMode != eventpb.DeliveryMode_DELIVERY_MODE_BROADCAST {
					t.Fatalf("DeliveryMode = %v, want BROADCAST", req.DeliveryMode)
				}
				if req.WorkerGroup != "" {
					t.Fatalf("WorkerGroup = %q, want empty for broadcast", req.WorkerGroup)
				}
			},
		},
		{
			name: "delivery mode worker pool with group",
			opts: transport.ApplySubscribeOptions(
				transport.WithDeliveryMode(transport.WorkerPool),
				transport.WithWorkerGroup("group-A"),
			),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.DeliveryMode != eventpb.DeliveryMode_DELIVERY_MODE_WORKER_POOL {
					t.Fatalf("DeliveryMode = %v, want WORKER_POOL", req.DeliveryMode)
				}
				if req.WorkerGroup != "group-A" {
					t.Fatalf("WorkerGroup = %q, want %q", req.WorkerGroup, "group-A")
				}
			},
		},
		{
			name: "start from beginning",
			opts: transport.ApplySubscribeOptions(transport.WithStartFrom(transport.StartFromBeginning)),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.StartFrom != eventpb.StartPosition_START_POSITION_BEGINNING {
					t.Fatalf("StartFrom = %v, want BEGINNING", req.StartFrom)
				}
				if req.StartTime != nil {
					t.Fatalf("StartTime = %v, want nil", req.StartTime)
				}
			},
		},
		{
			name: "start from latest",
			opts: transport.ApplySubscribeOptions(transport.WithStartFrom(transport.StartFromLatest)),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.StartFrom != eventpb.StartPosition_START_POSITION_LATEST {
					t.Fatalf("StartFrom = %v, want LATEST", req.StartFrom)
				}
				if req.StartTime != nil {
					t.Fatalf("StartTime = %v, want nil", req.StartTime)
				}
			},
		},
		{
			name: "start from timestamp sets start time",
			opts: transport.ApplySubscribeOptions(
				transport.WithStartFrom(transport.StartFromTimestamp),
				transport.WithStartTime(startTime),
			),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.StartFrom != eventpb.StartPosition_START_POSITION_TIMESTAMP {
					t.Fatalf("StartFrom = %v, want TIMESTAMP", req.StartFrom)
				}
				if req.StartTime == nil {
					t.Fatal("StartTime = nil, want non-nil timestamp")
				}
				if got := req.StartTime.AsTime(); !got.Equal(startTime) {
					t.Fatalf("StartTime = %v, want %v", got, startTime)
				}
			},
		},
		{
			name: "start from timestamp with zero time leaves start time nil",
			opts: transport.ApplySubscribeOptions(
				transport.WithStartFrom(transport.StartFromTimestamp),
			),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.StartFrom != eventpb.StartPosition_START_POSITION_TIMESTAMP {
					t.Fatalf("StartFrom = %v, want TIMESTAMP", req.StartFrom)
				}
				if req.StartTime != nil {
					t.Fatalf("StartTime = %v, want nil for zero time", req.StartTime)
				}
			},
		},
		{
			name: "max age maps to duration",
			opts: transport.ApplySubscribeOptions(transport.WithMaxAge(90 * time.Second)),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.MaxAge == nil {
					t.Fatal("MaxAge = nil, want non-nil duration")
				}
				if got := req.MaxAge.AsDuration(); got != 90*time.Second {
					t.Fatalf("MaxAge = %v, want %v", got, 90*time.Second)
				}
			},
		},
		{
			name: "zero max age leaves duration nil",
			opts: transport.ApplySubscribeOptions(),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.MaxAge != nil {
					t.Fatalf("MaxAge = %v, want nil", req.MaxAge)
				}
			},
		},
		{
			name: "latest only",
			opts: transport.ApplySubscribeOptions(transport.WithLatestOnly()),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if !req.LatestOnly {
					t.Fatal("LatestOnly = false, want true")
				}
			},
		},
		{
			name: "buffer size maps to int32",
			opts: transport.ApplySubscribeOptions(transport.WithBufferSize(256)),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.BufferSize != 256 {
					t.Fatalf("BufferSize = %d, want 256", req.BufferSize)
				}
			},
		},
		{
			name: "consumer id",
			opts: transport.ApplySubscribeOptions(transport.WithConsumerID("consumer-7")),
			check: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.ConsumerId != "consumer-7" {
					t.Fatalf("ConsumerId = %q, want %q", req.ConsumerId, "consumer-7")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := buildSubscribeRequest("orders", tt.opts)
			if req.Event != "orders" {
				t.Fatalf("Event = %q, want %q", req.Event, "orders")
			}
			tt.check(t, req)
		})
	}
}
