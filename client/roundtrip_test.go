package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

// newMessage builds a transport.Message for publishing in tests.
func newMessage(id, source string, payload []byte, metadata map[string]string) transport.Message {
	return message.New(id, source, payload, metadata)
}

// TestRemoteTransport_PublishPreservesSource drives a real RemoteTransport
// (not the raw gRPC stub) end to end and asserts that the Source set by the
// publisher survives the round trip. The client must forward msg.Source() to
// the server via the x-source metadata header.
func TestRemoteTransport_PublishPreservesSource(t *testing.T) {
	tr, cleanup := newConnectedTransport(t)
	defer cleanup()

	ctx := context.Background()
	const event = "orders"

	if err := tr.RegisterEvent(ctx, event); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	waitReady(t, tr, sub, event)

	if err := tr.Publish(ctx, event, newMessage("real-1", "publisher-A", []byte("hello"), nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := recvKeyed(t, sub, "real-1", 2*time.Second)
	if got := msg.Source(); got != "publisher-A" {
		t.Fatalf("Source = %q, want %q (publisher source dropped on the wire)", got, "publisher-A")
	}
	if string(msg.Payload()) != "hello" {
		t.Fatalf("Payload = %q, want %q", msg.Payload(), "hello")
	}
}
