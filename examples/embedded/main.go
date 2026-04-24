// Example showing how to embed the event service into an existing gRPC server
// with custom authorization.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"github.com/rbaliyan/event/v3/transport/channel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// adminActions lists the gRPC full method names that require admin privileges.
// Using a map (vs switch) scales cleanly as more admin actions are added and
// makes the permission surface trivial to audit.
var adminActions = map[string]struct{}{
	"/event.v1.EventService/RegisterEvent":   {},
	"/event.v1.EventService/UnregisterEvent": {},
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create transport
	ch := channel.New()
	defer func() { _ = ch.Close(ctx) }()

	// Create event service with custom security guard.
	// The guard's Authenticate extracts the caller's role from gRPC metadata;
	// Authorize then decides whether that role may call the requested method.
	eventSvc, err := service.NewService(ch,
		service.WithSecurityGuard(&roleGuard{
			// admins can register/unregister, everyone can pub/sub
			adminRoles: []string{"admin"},
		}),
		service.WithLogger(logger),
	)
	if err != nil {
		log.Fatal("failed to create event service:", err)
	}
	defer eventSvc.Stop()

	// Wire the service's auth interceptors alongside any other interceptors.
	// auditInterceptor runs AFTER the service's UnaryInterceptor so the
	// authenticated identity is already attached to the context.
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			eventSvc.UnaryInterceptor(),
			auditInterceptor(logger),
			service.LoggingInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			eventSvc.StreamInterceptor(),
			service.StreamLoggingInterceptor(logger),
		),
	)

	// Register event service alongside your other services
	eventpb.RegisterEventServiceServer(grpcServer, eventSvc)

	// Start server
	lis, err := net.Listen("tcp", ":9090") // #nosec G102 -- example code, bind to all interfaces is intentional
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		logger.Info("starting gRPC server", "addr", ":9090")
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")
	grpcServer.GracefulStop()
}

// roleGuard implements service.SecurityGuard.
// Authenticate extracts the caller's role from the "x-role" gRPC metadata header.
// Authorize permits all operations to everyone except register/unregister, which
// require an admin role.
type roleGuard struct {
	adminRoles []string
}

func (g *roleGuard) Authenticate(ctx context.Context) (service.Identity, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	roles := md.Get("x-role")
	if len(roles) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing x-role header")
	}
	return &roleIdentity{role: roles[0]}, nil
}

func (g *roleGuard) Authorize(_ context.Context, id service.Identity, action string) (service.Decision, error) {
	if _, adminOnly := adminActions[action]; !adminOnly {
		return service.Decision{Allowed: true, Scope: "all"}, nil
	}
	role := id.UserID()
	for _, admin := range g.adminRoles {
		if role == admin {
			return service.Decision{Allowed: true, Scope: "admin"}, nil
		}
	}
	return service.Decision{
		Allowed: false,
		Reason:  fmt.Sprintf("role %q cannot manage events", role),
	}, nil
}

// auditInterceptor demonstrates how downstream interceptors can retrieve the
// authenticated identity populated by the event service's guard. The identity
// is attached to the context by UnaryInterceptor/StreamInterceptor before the
// handler runs, so chained interceptors (logging, audit, tenant scoping) can
// inspect it via service.IdentityFromContext.
func auditInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if id, ok := service.IdentityFromContext(ctx); ok {
			logger.Info("rpc", "method", info.FullMethod, "caller", id.UserID())
		}
		return handler(ctx, req)
	}
}

type roleIdentity struct {
	role string
}

func (i *roleIdentity) UserID() string         { return i.role }
func (i *roleIdentity) Claims() map[string]any { return map[string]any{"role": i.role} }
