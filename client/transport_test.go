package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event-server/client"
	"github.com/rbaliyan/event/v3/transport"
)

// bufSize is the in-process listener buffer size shared by client test helpers.
const bufSize = 1024 * 1024

// TestRemoteTransport_PublishSubscribeAck drives a real RemoteTransport end to
// end against an in-process server: register, subscribe, publish, receive, ack.
// (Previously this exercised the raw gRPC stub; it now covers the actual
// transport translation layer.)
func TestRemoteTransport_PublishSubscribeAck(t *testing.T) {
	tr, cleanup := newConnectedTransport(t)
	defer cleanup()

	ctx := context.Background()
	const event = "test.event"

	if err := tr.RegisterEvent(ctx, event); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	sub, err := tr.Subscribe(ctx, event, transport.WithStartFrom(transport.StartFromLatest))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	waitReady(t, tr, sub, event)

	if err := tr.Publish(ctx, event, newMessage("msg-1", "", []byte("hello"), nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := recvKeyed(t, sub, "msg-1", 2*time.Second)
	if string(msg.Payload()) != "hello" {
		t.Fatalf("payload = %q, want hello", msg.Payload())
	}
	if err := msg.Ack(nil); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestRemoteTransport_ImplementsInterface is a compile-time check that
// RemoteTransport satisfies transport.Transport. The buildSubscribeRequest
// option-mapping logic is covered in subscribe_request_test.go.
func TestRemoteTransport_ImplementsInterface(t *testing.T) {
	var _ transport.Transport = (*client.RemoteTransport)(nil)
}

func TestRemoteTransportNew(t *testing.T) {
	_, err := client.New("")
	if err == nil {
		t.Fatal("expected error for empty address")
	}

	rt, err := client.New("localhost:9090", client.WithInsecure())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if rt.State() != client.ConnStateDisconnected {
		t.Fatalf("expected disconnected, got %v", rt.State())
	}
}

func TestRemoteTransportClose(t *testing.T) {
	rt, err := client.New("localhost:9090", client.WithInsecure())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := rt.Close(context.Background()); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if rt.State() != client.ConnStateClosed {
		t.Fatalf("expected closed, got %v", rt.State())
	}

	// Close again should be safe.
	if err := rt.Close(context.Background()); err != nil {
		t.Fatalf("double Close failed: %v", err)
	}
}
