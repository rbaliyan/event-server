# Event Server

[![CI](https://github.com/rbaliyan/event-server/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event-server/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event-server.svg)](https://pkg.go.dev/github.com/rbaliyan/event-server)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event-server)](https://goreportcard.com/report/github.com/rbaliyan/event-server)
[![Release](https://img.shields.io/github/v/release/rbaliyan/event-server)](https://github.com/rbaliyan/event-server/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/event-server/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/event-server)

A **gRPC event server** that bridges the [event/v3](https://github.com/rbaliyan/event) pub-sub library to remote clients over gRPC, REST/JSON, WebSocket, and Server-Sent Events (SSE). Clients connect without needing direct access to the underlying transport (Redis, Kafka, NATS, etc.).

## Features

### Server
- **gRPC EventService**: Register/unregister events, publish, subscribe (server-streaming), batch ack, health check
- **HTTP Gateway**: REST/JSON via gRPC-Gateway with full OpenAPI mapping
- **WebSocket Streaming**: Bidirectional JSON protocol with ack support and heartbeats
- **SSE Streaming**: One-way server-sent events for browser-friendly subscriptions
- **Pluggable Authorization**: Interface-based auth with per-operation granularity
- **Ack Tracking**: Timeout-based acknowledgment with automatic nack on expiration

### Client
- **RemoteTransport**: Implements `transport.Transport` - drop-in replacement for any local transport
- **Circuit Breaker**: Configurable failure threshold and recovery timeout
- **Retry with Backoff**: Exponential backoff with jitter for transient failures
- **Subscribe Reconnect**: Automatic stream reconnection with configurable max errors
- **Connection Lifecycle**: State tracking with callbacks (disconnected, connecting, connected, closed)

### Observability
- Structured logging via `slog`
- Health endpoint with transport status and latency
- Panic recovery interceptors
- Request/error logging interceptors

## Installation

```bash
go get github.com/rbaliyan/event-server
```

## Quick Start

### Standalone Server

A complete gRPC + HTTP server with an in-memory transport:

```go
package main

import (
    "context"
    "log"
    "log/slog"
    "net"
    "net/http"
    "os"
    "os/signal"
    "syscall"

    "github.com/rbaliyan/event-server/gateway"
    eventpb "github.com/rbaliyan/event-server/proto/event/v1"
    "github.com/rbaliyan/event-server/service"
    "github.com/rbaliyan/event/v3/transport/channel"
    "google.golang.org/grpc"
)

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

    // Create transport (channel, redis, nats, kafka - any event/v3 transport)
    ch := channel.New()
    defer func() { _ = ch.Close(ctx) }()

    // Create event service
    svc := service.NewService(ch,
        service.WithAuthorizer(service.AllowAll()),
        service.WithLogger(logger),
    )
    defer svc.Stop()

    // Start gRPC server
    grpcServer := grpc.NewServer(
        grpc.ChainUnaryInterceptor(
            service.LoggingInterceptor(logger),
            service.RecoveryInterceptor(logger),
        ),
        grpc.ChainStreamInterceptor(
            service.StreamLoggingInterceptor(logger),
            service.StreamRecoveryInterceptor(logger),
        ),
    )
    eventpb.RegisterEventServiceServer(grpcServer, svc)

    lis, _ := net.Listen("tcp", ":9090")
    go grpcServer.Serve(lis)

    // Start HTTP gateway (REST + WebSocket + SSE)
    handler, _ := gateway.NewHandler(ctx, "localhost:9090", gateway.WithInsecure())
    httpServer := &http.Server{Addr: ":8080", Handler: handler}
    go httpServer.ListenAndServe()

    <-ctx.Done()
    grpcServer.GracefulStop()
    httpServer.Shutdown(context.Background())
}
```

### Remote Client

Connect to the server using `RemoteTransport` with the standard `event.Bus` API:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/rbaliyan/event-server/client"
    "github.com/rbaliyan/event/v3"
)

type Order struct {
    ID    string  `json:"id"`
    Total float64 `json:"total"`
}

func main() {
    ctx := context.Background()

    // Connect to event server - no Redis/Kafka/NATS needed on the client!
    remote, _ := client.New("localhost:9090",
        client.WithInsecure(),
        client.WithRetry(3, 100*time.Millisecond, 5*time.Second),
        client.WithCircuitBreaker(5, 30*time.Second),
    )
    remote.Connect(ctx)
    defer remote.Close(ctx)

    // Use with event.Bus - same API as any local transport
    bus, _ := event.NewBus("my-app", event.WithTransport(remote))
    defer bus.Close(ctx)

    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)

    orderEvent.Subscribe(ctx, func(ctx context.Context, ev event.Event[Order], order Order) error {
        fmt.Printf("order: %s ($%.2f)\n", order.ID, order.Total)
        return nil // Automatically acks via gRPC
    })

    orderEvent.Publish(ctx, Order{ID: "ORD-1", Total: 99.99})
}
```

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Event Server                          │
│                                                          │
│  ┌─────────────────────────────────────────────────────┐ │
│  │                 service.Service                     │ │
│  │  (gRPC EventService + Authorizer + AckTracker)      │ │
│  └────────────────────────┬────────────────────────────┘ │
│                           │                              │
│                    transport.Transport                   │
│              (channel/redis/nats/kafka)                  │
│                                                          │
│  ┌────────────┐ ┌──────────────────────────────────────┐ │
│  │   gRPC     │ │          gateway.Handler             │ │
│  │  :9090     │ │  REST (gRPC-Gateway) + WS + SSE      │ │
│  │            │ │  :8080                               │ │
│  └────────────┘ └──────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
        ▲                    ▲                 ▲
        │                    │                 │
   gRPC Client          HTTP/REST         WebSocket/SSE
  (RemoteTransport)     (curl, etc.)      (browser, etc.)
```

## API Reference

### gRPC API

| RPC | HTTP Mapping | Description |
|-----|-------------|-------------|
| `RegisterEvent` | `POST /v1/events/{name}` | Create transport resources for a named event |
| `UnregisterEvent` | `DELETE /v1/events/{name}` | Remove transport resources |
| `ListEvents` | `GET /v1/events` | List all registered event names |
| `Publish` | `POST /v1/events/{event}/messages` | Send a message to an event |
| `Subscribe` | *gRPC only* | Server-streaming subscription |
| `Ack` | `POST /v1/ack` | Batch acknowledge messages |
| `Health` | `GET /v1/health` | Server and transport health status |

### WebSocket API

Connect to `GET /v1/events/{name}/subscribe` for bidirectional JSON streaming.

**Query parameters:**

| Parameter | Values | Description |
|-----------|--------|-------------|
| `delivery_mode` | `broadcast`, `worker_pool` | Message distribution mode |
| `worker_group` | string | Worker group name (worker_pool mode) |
| `start_from` | `beginning`, `latest` | Where to start reading |
| `latest_only` | `true` | Only deliver most recent message |
| `consumer_id` | string | Stable consumer ID for checkpoint resume |

**Server sends:**
```json
{"type": "message", "id": "msg-1", "source": "my-app", "payload": "...", "ack_id": "ack-123"}
{"type": "heartbeat"}
{"type": "error", "error": "subscribe failed: ..."}
```

**Client sends:**
```json
{"type": "ack", "ack_id": "ack-123"}
{"type": "ack", "ack_id": "ack-456", "error": "processing failed"}
```

### SSE API

Connect to `GET /v1/events/{name}/stream` for one-way server-sent events. Supports the same query parameters as WebSocket.

```
event: message
data: {"id":"msg-1","source":"my-app","payload":"...","ack_id":"ack-123"}

event: heartbeat
data: {}
```

### REST Examples

```bash
# Register an event
curl -X POST http://localhost:8080/v1/events/order.created

# List events
curl http://localhost:8080/v1/events

# Publish a message
curl -X POST http://localhost:8080/v1/events/order.created/messages \
  -H 'Content-Type: application/json' \
  -d '{"payload":"eyJpZCI6Im9yZGVyLTEifQ==","metadata":{"source":"api"}}'

# Acknowledge a message
curl -X POST http://localhost:8080/v1/ack \
  -H 'Content-Type: application/json' \
  -d '{"entries":[{"ack_id":"ack-123"}]}'

# Health check
curl http://localhost:8080/v1/health
```

## Packages

### `service` - gRPC Service Implementation

```go
svc := service.NewService(transport,
    service.WithAuthorizer(myAuth),    // Default: DenyAll()
    service.WithLogger(logger),         // Default: slog.Default()
    service.WithAckTimeout(30*time.Second), // Default: 30s
)
defer svc.Stop()
```

**Authorizer interface:**

```go
type Authorizer interface {
    Authorize(ctx context.Context, req AuthRequest) error
}

type AuthRequest struct {
    Event     string
    Operation Operation // Publish, Subscribe, Register, Unregister, List
}
```

Built-in: `AllowAll()` (dev only), `DenyAll()` (default).

**Interceptors:**

```go
service.LoggingInterceptor(logger)        // Log method calls and errors
service.RecoveryInterceptor(logger)       // Catch panics, return Internal
service.StreamLoggingInterceptor(logger)  // Log stream lifecycle
service.StreamRecoveryInterceptor(logger) // Catch panics in streams
```

### `client` - RemoteTransport

```go
remote, err := client.New("localhost:9090",
    client.WithInsecure(),
    client.WithTLS(tlsConfig),
    client.WithRetry(3, 100*time.Millisecond, 5*time.Second),
    client.WithCallTimeout(10*time.Second),
    client.WithCircuitBreaker(5, 30*time.Second),
    client.WithSubscribeReconnect(true, time.Second),
    client.WithSubscribeMaxErrors(10),
    client.WithSubscribeBufferSize(100),
    client.WithKeepalive(30*time.Second, 10*time.Second),
    client.WithStateCallback(func(s client.ConnState) { /* ... */ }),
    client.WithStreamErrorCallback(func(err error) { /* ... */ }),
    client.WithDialOptions(grpc.WithBlock()),
)

remote.Connect(ctx) // Establish connection
remote.Ready()      // Check if connected
remote.State()      // ConnStateDisconnected|Connecting|Connected|Closed
remote.Close(ctx)   // Graceful shutdown
```

### `gateway` - HTTP Handler

```go
// Remote mode: connects to a gRPC server
handler, err := gateway.NewHandler(ctx, "localhost:9090",
    gateway.WithInsecure(),
    gateway.WithTLS(tlsConfig),
    gateway.WithHeartbeatInterval(30*time.Second),
    gateway.WithWSOriginPatterns("example.com", "*.example.com"),
    gateway.WithMuxOptions(runtime.WithMarshalerOption(...)),
    gateway.WithDialOptions(grpc.WithBlock()),
)

// In-process mode: calls service directly (no network hop)
handler, err := gateway.NewInProcessHandler(ctx, svc,
    gateway.WithHeartbeatInterval(30*time.Second),
    gateway.WithWSOriginPatterns("example.com"),
)
```

## Deployment Modes

### Standalone

gRPC server + HTTP gateway as a standalone service:

```
[Event Server] ── gRPC :9090 + HTTP :8080
       │
   transport (Redis/Kafka/NATS)
```

### Embedded

Add the EventService to an existing gRPC server with custom auth:

```go
eventpb.RegisterEventServiceServer(yourGRPCServer, eventSvc)
```

See [examples/embedded](examples/embedded) for a complete example with role-based authorization.

### Client-Side

Use `RemoteTransport` with `event.Bus` for transparent remote event handling:

```go
bus, _ := event.NewBus("my-app", event.WithTransport(remote))
// Use Publish/Subscribe exactly like a local transport
```

## Ecosystem

Part of the [event/v3](https://github.com/rbaliyan/event) ecosystem:

| Package | Description |
|---------|-------------|
| [event](https://github.com/rbaliyan/event) | Core event bus with transports (channel, Redis, NATS, Kafka) |
| **event-server** | gRPC server + HTTP gateway + remote client (this package) |
| [event-mongodb](https://github.com/rbaliyan/event-mongodb) | MongoDB Change Stream transport (CDC) |
| [event-dlq](https://github.com/rbaliyan/event-dlq) | Dead Letter Queue management |
| [event-scheduler](https://github.com/rbaliyan/event-scheduler) | Delayed/scheduled message delivery |
| [event-extras](https://github.com/rbaliyan/event-extras) | Rate limiting and saga orchestration |

## Development

### Prerequisites

Install tools via [mise](https://mise.jdx.dev):

```bash
mise install
```

### Commands

```bash
just build       # go build ./...
just test        # go test -v ./...
just test-race   # go test -race ./...
just test-cover  # go test -cover ./...
just lint        # golangci-lint run ./...
just fmt         # go fmt ./...
just tidy        # go mod tidy
just vulncheck   # govulncheck ./...
just proto       # Regenerate protobuf code
just release     # Tag and push a new patch release
```

### Protobuf Generation

```bash
just proto       # Generate to root (for development)
just proto-local # Generate to proto/ directory
```

Requires `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`, and `protoc-gen-grpc-gateway` (all managed by mise).

## License

MIT License - see [LICENSE](LICENSE) for details.
