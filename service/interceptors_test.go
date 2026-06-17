package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/rbaliyan/event-server/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeServerStream is a no-op grpc.ServerStream for interceptor tests; the
// handlers under test never touch its methods before returning/panicking.
type fakeServerStream struct{ grpc.ServerStream }

func TestLoggingInterceptor_PassesThrough(t *testing.T) {
	t.Parallel()
	ic := service.LoggingInterceptor(discardLogger())
	info := &grpc.UnaryServerInfo{FullMethod: "/event.v1.EventService/Publish"}

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		resp, err := ic(context.Background(), "req", info, func(context.Context, any) (any, error) {
			return "ok", nil
		})
		if err != nil || resp != "ok" {
			t.Fatalf("got (%v, %v), want (ok, nil)", resp, err)
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		want := errors.New("boom")
		_, err := ic(context.Background(), "req", info, func(context.Context, any) (any, error) {
			return nil, want
		})
		if !errors.Is(err, want) {
			t.Fatalf("err = %v, want %v (must propagate)", err, want)
		}
	})
}

func TestStreamLoggingInterceptor_PassesThrough(t *testing.T) {
	t.Parallel()
	ic := service.StreamLoggingInterceptor(discardLogger())
	info := &grpc.StreamServerInfo{FullMethod: "/event.v1.EventService/Subscribe"}

	if err := ic(nil, &fakeServerStream{}, info, func(any, grpc.ServerStream) error {
		return nil
	}); err != nil {
		t.Fatalf("success path err = %v, want nil", err)
	}

	want := errors.New("stream boom")
	if err := ic(nil, &fakeServerStream{}, info, func(any, grpc.ServerStream) error {
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v (must propagate)", err, want)
	}
}

func TestRecoveryInterceptor_RecoversPanic(t *testing.T) {
	t.Parallel()
	ic := service.RecoveryInterceptor(discardLogger())
	info := &grpc.UnaryServerInfo{FullMethod: "/event.v1.EventService/Publish"}

	t.Run("panic becomes Internal", func(t *testing.T) {
		t.Parallel()
		resp, err := ic(context.Background(), "req", info, func(context.Context, any) (any, error) {
			panic("kaboom")
		})
		if resp != nil {
			t.Fatalf("resp = %v, want nil on panic", resp)
		}
		if status.Code(err) != codes.Internal {
			t.Fatalf("code = %v, want Internal", status.Code(err))
		}
	})

	t.Run("no panic passes through", func(t *testing.T) {
		t.Parallel()
		resp, err := ic(context.Background(), "req", info, func(context.Context, any) (any, error) {
			return "ok", nil
		})
		if err != nil || resp != "ok" {
			t.Fatalf("got (%v, %v), want (ok, nil)", resp, err)
		}
	})
}

func TestStreamRecoveryInterceptor_RecoversPanic(t *testing.T) {
	t.Parallel()
	ic := service.StreamRecoveryInterceptor(discardLogger())
	info := &grpc.StreamServerInfo{FullMethod: "/event.v1.EventService/Subscribe"}

	err := ic(nil, &fakeServerStream{}, info, func(any, grpc.ServerStream) error {
		panic("stream kaboom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}

	if err := ic(nil, &fakeServerStream{}, info, func(any, grpc.ServerStream) error {
		return nil
	}); err != nil {
		t.Fatalf("no-panic path err = %v, want nil", err)
	}
}
