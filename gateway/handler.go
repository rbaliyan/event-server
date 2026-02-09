package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"google.golang.org/grpc"
)

// Handler is an HTTP handler for the EventService gateway.
// For handlers created by NewHandler, Close releases the underlying gRPC
// connection. For handlers created by NewInProcessHandler, Close is a no-op.
type Handler struct {
	http.Handler
	closeOnce sync.Once
	closeErr  error
	conn      *grpc.ClientConn
	done      chan struct{}
}

// Close releases resources held by the handler.
func (h *Handler) Close() error {
	h.closeOnce.Do(func() {
		close(h.done)
		if h.conn != nil {
			h.closeErr = h.conn.Close()
		}
	})
	return h.closeErr
}

// NewHandler creates a Handler that proxies to the gRPC service.
// The handler can be mounted on any HTTP router.
//
// Example:
//
//	handler, _ := gateway.NewHandler(ctx, "localhost:9090", gateway.WithInsecure())
//	defer handler.Close()
//	http.Handle("/", handler)
func NewHandler(ctx context.Context, grpcAddr string, opts ...Option) (*Handler, error) {
	if grpcAddr == "" {
		return nil, errors.New("gateway: gRPC address must not be empty")
	}

	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	dialOpts := o.buildDialOpts()
	conn, err := grpc.NewClient(grpcAddr, dialOpts...)
	if err != nil {
		return nil, err
	}

	mux := runtime.NewServeMux(o.muxOpts...)
	if err := eventpb.RegisterEventServiceHandler(ctx, mux, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	heartbeat := o.heartbeatInterval
	if heartbeat <= 0 {
		heartbeat = defaultHeartbeatInterval
	}

	client := eventpb.NewEventServiceClient(conn)
	wsHandler := newRemoteWSHandler(client, heartbeat, o.wsOriginPatterns)
	sseHandler := newRemoteSSEHandler(client, heartbeat)

	h := &Handler{
		Handler: composeHandlers(mux, wsHandler, sseHandler),
		conn:    conn,
		done:    make(chan struct{}),
	}

	go func() {
		select {
		case <-ctx.Done():
		case <-h.done:
		}
		_ = h.Close()
	}()

	return h, nil
}

// NewInProcessHandler creates a handler that calls the service directly
// without going through a network connection.
//
// Example:
//
//	svc := service.NewService(transport, service.WithAuthorizer(auth))
//	handler, _ := gateway.NewInProcessHandler(ctx, svc)
//	http.Handle("/", handler)
func NewInProcessHandler(ctx context.Context, svc eventpb.EventServiceServer, opts ...Option) (*Handler, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	mux := runtime.NewServeMux(o.muxOpts...)
	if err := eventpb.RegisterEventServiceHandlerServer(ctx, mux, svc); err != nil {
		return nil, err
	}

	heartbeat := o.heartbeatInterval
	if heartbeat <= 0 {
		heartbeat = defaultHeartbeatInterval
	}

	wsHandler := newInProcessWSHandler(svc, heartbeat, o.wsOriginPatterns)
	sseHandler := newInProcessSSEHandler(svc, heartbeat)

	return &Handler{
		Handler: composeHandlers(mux, wsHandler, sseHandler),
		done:    make(chan struct{}),
	}, nil
}

// composeHandlers creates a single http.Handler that routes WebSocket
// and SSE subscribe requests to their respective handlers, and everything
// else to the gRPC-Gateway mux.
func composeHandlers(gwMux http.Handler, wsHandler http.Handler, sseHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	// WebSocket subscribe: GET /v1/events/{name}/subscribe
	mux.Handle("GET /v1/events/{name}/subscribe", wsHandler)

	// SSE subscribe: GET /v1/events/{name}/stream
	mux.Handle("GET /v1/events/{name}/stream", sseHandler)

	// Return 405 for non-GET methods on subscribe/stream endpoints
	mux.HandleFunc("/v1/events/{name}/subscribe", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})
	mux.HandleFunc("/v1/events/{name}/stream", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	// Everything else goes to gRPC-Gateway
	mux.Handle("/", gwMux)
	return mux
}
