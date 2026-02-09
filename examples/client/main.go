// Example client using RemoteTransport with the event Bus.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rbaliyan/event-server/client"
	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel/trace"
)

var emptySpanContext trace.SpanContext

// Order is an example event payload.
type Order struct {
	ID    string  `json:"id"`
	Total float64 `json:"total"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create remote transport connecting to the event server.
	// No Redis, Kafka, or MongoDB access needed on the client side!
	remote, err := client.New("localhost:9090",
		client.WithInsecure(),
		client.WithRetry(3, 100*time.Millisecond, 5*time.Second),
	)
	if err != nil {
		log.Fatalf("failed to create remote transport: %v", err)
	}

	// Connect to the event server
	if err := remote.Connect(ctx); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer func() { _ = remote.Close(ctx) }()

	// Use the remote transport with the event Bus - same API as local transport!
	bus, err := event.NewBus("my-app", event.WithTransport(remote))
	if err != nil {
		log.Fatalf("failed to create bus: %v", err)
	}
	defer func() { _ = bus.Close(ctx) }()

	// Register and use events
	orderEvent := event.New[Order]("order.created")
	if err := event.Register(ctx, bus, orderEvent); err != nil {
		log.Fatalf("failed to register event: %v", err)
	}

	// Subscribe to orders
	orderEvent.Subscribe(ctx, func(ctx context.Context, ev event.Event[Order], order Order) error {
		fmt.Printf("received order: %s (total: $%.2f)\n", order.ID, order.Total)
		return nil // Automatically acks via gRPC Ack RPC
	})

	// Publish a test order
	if err := orderEvent.Publish(ctx, Order{ID: "order-1", Total: 99.99}); err != nil {
		log.Fatalf("failed to publish: %v", err)
	}
	fmt.Println("published order-1")

	// Also possible to publish raw messages directly via the transport
	msg := transport.NewMessage("msg-2", "cli", []byte(`{"id":"order-2","total":49.99}`), nil, emptySpanContext)
	if err := remote.Publish(ctx, "order.created", msg); err != nil {
		log.Fatalf("failed to publish raw: %v", err)
	}
	fmt.Println("published order-2 (raw)")

	// Wait a bit for messages to be received
	time.Sleep(2 * time.Second)
	fmt.Println("done")
}
