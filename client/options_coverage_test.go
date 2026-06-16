package client_test

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	"github.com/rbaliyan/event/v3/transport"
)

// TestOptions_AllApplied constructs a transport with every option so the option
// closures run, and checks the ConnState stringer.
func TestOptions_AllApplied(t *testing.T) {
	tr, err := client.New("localhost:9090",
		client.WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		client.WithInsecure(),
		client.WithRetry(2, 10*time.Millisecond, time.Second),
		client.WithCallTimeout(time.Second),
		client.WithCircuitBreaker(3, time.Second),
		client.WithSubscribeBufferSize(8),
		client.WithSubscribeReconnect(true, 10*time.Millisecond),
		client.WithSubscribeMaxErrors(3),
		client.WithKeepalive(20*time.Second, 5*time.Second),
		client.WithStateCallback(func(client.ConnState) {}),
		client.WithStreamErrorCallback(func(error) {}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = tr.Close(context.Background()) }()

	if tr.State().String() != "disconnected" {
		t.Fatalf("State = %q, want disconnected", tr.State())
	}
}

func TestConnState_String(t *testing.T) {
	cases := map[client.ConnState]string{
		client.ConnStateDisconnected: "disconnected",
		client.ConnStateConnecting:   "connecting",
		client.ConnStateConnected:    "connected",
		client.ConnStateClosed:       "closed",
		client.ConnState(99):         "unknown",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Errorf("ConnState(%d).String() = %q, want %q", state, got, want)
		}
	}
}

// TestTransport_ReadyAndUnregister exercises Ready, UnregisterEvent, and
// subscription ID over a live in-process server.
func TestTransport_ReadyAndUnregister(t *testing.T) {
	tr, cleanup := newConnectedTransport(t)
	defer cleanup()
	ctx := context.Background()

	if !tr.Ready() {
		t.Fatal("Ready() = false after Connect, want true")
	}

	const event = "lifecycle.evt"
	if err := tr.RegisterEvent(ctx, event); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if sub.ID() == "" {
		t.Error("subscription ID must be non-empty")
	}
	_ = sub.Close(ctx)

	if err := tr.UnregisterEvent(ctx, event); err != nil {
		t.Fatalf("UnregisterEvent: %v", err)
	}

	_ = tr.Close(ctx)
	if tr.Ready() {
		t.Error("Ready() = true after Close, want false")
	}
}
