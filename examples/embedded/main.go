// Example showing how to embed the event service into an existing gRPC server
// with custom authorization.
package main

import (
	"context"
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

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create transport
	ch := channel.New()
	defer func() { _ = ch.Close(ctx) }()

	// Create event service with custom authorizer
	eventSvc, err := service.NewService(ch,
		service.WithAuthorizer(&roleAuthorizer{
			// admins can register/unregister, everyone can pub/sub
			adminRoles: []string{"admin"},
		}),
		service.WithLogger(logger),
	)
	if err != nil {
		log.Fatal("failed to create event service:", err)
	}
	defer eventSvc.Stop()

	// Create gRPC server with auth interceptor
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			authInterceptor,
			service.LoggingInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			streamAuthInterceptor,
			service.StreamLoggingInterceptor(logger),
		),
	)

	// Register event service alongside your other services
	eventpb.RegisterEventServiceServer(grpcServer, eventSvc)

	// Start server
	lis, err := net.Listen("tcp", ":9090")
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

// authInterceptor extracts the role from metadata and adds it to context.
func authInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, err := extractRole(ctx)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func streamAuthInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, err := extractRole(ss.Context())
	if err != nil {
		return err
	}
	wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
	return handler(srv, wrapped)
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func extractRole(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	roles := md.Get("x-role")
	if len(roles) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing role")
	}
	return context.WithValue(ctx, roleKey{}, roles[0]), nil
}

type roleKey struct{}

// roleAuthorizer allows admins to register/unregister events, everyone can pub/sub.
type roleAuthorizer struct {
	adminRoles []string
}

func (a *roleAuthorizer) Authorize(ctx context.Context, req service.AuthRequest) error {
	role, _ := ctx.Value(roleKey{}).(string)

	switch req.Operation {
	case service.OperationRegister, service.OperationUnregister:
		for _, admin := range a.adminRoles {
			if role == admin {
				return nil
			}
		}
		return status.Errorf(codes.PermissionDenied, "role %q cannot %s events", role, req.Operation)
	default:
		return nil
	}
}
