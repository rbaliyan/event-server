package client

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/event/v3/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFromGRPCError(t *testing.T) {
	t.Parallel()

	plainErr := errors.New("plain non-status error")

	tests := []struct {
		name string
		in   error
		// check validates the converted error. Exactly one mechanism is used
		// per case depending on what the branch produces.
		check func(t *testing.T, got error)
	}{
		{
			name:  "nil",
			in:    nil,
			check: func(t *testing.T, got error) { requireNil(t, got) },
		},
		{
			name:  "ok",
			in:    status.Error(codes.OK, ""),
			check: func(t *testing.T, got error) { requireNil(t, got) },
		},
		{
			name:  "not found",
			in:    status.Error(codes.NotFound, "missing"),
			check: func(t *testing.T, got error) { requireIs(t, got, transport.ErrEventNotRegistered) },
		},
		{
			name:  "already exists",
			in:    status.Error(codes.AlreadyExists, "dup"),
			check: func(t *testing.T, got error) { requireIs(t, got, transport.ErrEventAlreadyExists) },
		},
		{
			name:  "unavailable",
			in:    status.Error(codes.Unavailable, "down"),
			check: func(t *testing.T, got error) { requireIs(t, got, transport.ErrTransportClosed) },
		},
		{
			name:  "deadline exceeded",
			in:    status.Error(codes.DeadlineExceeded, "slow"),
			check: func(t *testing.T, got error) { requireIs(t, got, transport.ErrPublishTimeout) },
		},
		{
			name:  "aborted",
			in:    status.Error(codes.Aborted, "gone"),
			check: func(t *testing.T, got error) { requireIs(t, got, transport.ErrSubscriptionClosed) },
		},
		{
			name:  "failed precondition",
			in:    status.Error(codes.FailedPrecondition, "no subs"),
			check: func(t *testing.T, got error) { requireIs(t, got, transport.ErrNoSubscribers) },
		},
		{
			name: "permission denied",
			in:   status.Error(codes.PermissionDenied, "nope"),
			check: func(t *testing.T, got error) {
				requirePermissionDenied(t, got, "nope")
			},
		},
		{
			name: "unauthenticated",
			in:   status.Error(codes.Unauthenticated, "who"),
			check: func(t *testing.T, got error) {
				requirePermissionDenied(t, got, "who")
			},
		},
		{
			name:  "canceled",
			in:    status.Error(codes.Canceled, "stop"),
			check: func(t *testing.T, got error) { requireIs(t, got, context.Canceled) },
		},
		{
			name: "invalid argument",
			in:   status.Error(codes.InvalidArgument, "bad"),
			check: func(t *testing.T, got error) {
				requireRemoteError(t, got, codes.InvalidArgument, "bad")
			},
		},
		{
			name: "default code (internal)",
			in:   status.Error(codes.Internal, "boom"),
			check: func(t *testing.T, got error) {
				requireRemoteError(t, got, codes.Internal, "boom")
			},
		},
		{
			name: "non-status plain error returned unchanged",
			in:   plainErr,
			check: func(t *testing.T, got error) {
				if got != plainErr {
					t.Fatalf("expected the original error to be returned unchanged, got %v", got)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, fromGRPCError(tt.in))
		})
	}
}

func requireNil(t *testing.T, got error) {
	t.Helper()
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func requireIs(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("expected errors.Is(%v, %v), got false", got, want)
	}
}

func requirePermissionDenied(t *testing.T, got error, wantMsg string) {
	t.Helper()
	var permErr *PermissionDeniedError
	if !errors.As(got, &permErr) {
		t.Fatalf("expected *PermissionDeniedError, got %T (%v)", got, got)
	}
	if permErr.Message != wantMsg {
		t.Fatalf("Message = %q, want %q", permErr.Message, wantMsg)
	}
	if !errors.Is(got, ErrPermissionDenied) {
		t.Fatalf("expected errors.Is(err, ErrPermissionDenied) to be true")
	}
}

func requireRemoteError(t *testing.T, got error, wantCode codes.Code, wantMsg string) {
	t.Helper()
	var remoteErr *RemoteError
	if !errors.As(got, &remoteErr) {
		t.Fatalf("expected *RemoteError, got %T (%v)", got, got)
	}
	if remoteErr.Code != wantCode {
		t.Fatalf("Code = %v, want %v", remoteErr.Code, wantCode)
	}
	if remoteErr.Message != wantMsg {
		t.Fatalf("Message = %q, want %q", remoteErr.Message, wantMsg)
	}
}

func TestPermissionDeniedError(t *testing.T) {
	t.Parallel()

	t.Run("Error with message", func(t *testing.T) {
		t.Parallel()
		err := &PermissionDeniedError{Message: "blocked"}
		if got, want := err.Error(), "event: permission denied: blocked"; got != want {
			t.Fatalf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("Error without message", func(t *testing.T) {
		t.Parallel()
		err := &PermissionDeniedError{}
		if got, want := err.Error(), "event: permission denied"; got != want {
			t.Fatalf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("Is matches ErrPermissionDenied", func(t *testing.T) {
		t.Parallel()
		err := &PermissionDeniedError{Message: "x"}
		if !errors.Is(err, ErrPermissionDenied) {
			t.Fatal("expected errors.Is(err, ErrPermissionDenied) to be true")
		}
		if errors.Is(err, context.Canceled) {
			t.Fatal("expected errors.Is(err, context.Canceled) to be false")
		}
	})
}

func TestRemoteError_Error(t *testing.T) {
	t.Parallel()

	err := &RemoteError{Code: codes.Internal, Message: "kaboom"}
	if got, want := err.Error(), "event: remote error (Internal): kaboom"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
