// Example standalone event server demonstrating full gRPC + HTTP setup
// with a channel transport.
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create an in-memory channel transport for demonstration
	ch := channel.New()
	defer func() { _ = ch.Close(ctx) }()

	// Create the event service with AllowAll guard (for demo only — configure a real guard in production)
	eventSvc, err := service.NewService(ch,
		service.WithSecurityGuard(service.AllowAll()),
		service.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create event service", "error", err)
		os.Exit(1)
	}
	defer eventSvc.Stop()

	// Create gRPC server with interceptors
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			eventSvc.UnaryInterceptor(),
			service.LoggingInterceptor(logger),
			service.RecoveryInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			eventSvc.StreamInterceptor(),
			service.StreamLoggingInterceptor(logger),
			service.StreamRecoveryInterceptor(logger),
		),
	)
	eventpb.RegisterEventServiceServer(grpcServer, eventSvc)

	// Start gRPC server
	grpcLis, err := net.Listen("tcp", ":9090") // #nosec G102 -- example code, bind to all interfaces is intentional
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		logger.Info("starting gRPC server", "addr", ":9090")
		if err := grpcServer.Serve(grpcLis); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// Create HTTP gateway
	httpHandler, err := gateway.NewHandler(ctx, "localhost:9090", gateway.WithInsecure())
	if err != nil {
		log.Fatalf("failed to create gateway: %v", err)
	}

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: httpHandler,
	}

	go func() {
		logger.Info("starting HTTP server", "addr", ":8080")
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutting down...")

	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(context.Background())
}
