// Package client provides a RemoteTransport that connects to an event server.
package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/rbaliyan/event/v3/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrPermissionDenied is returned when the server denies access to an event.
var ErrPermissionDenied = errors.New("event: permission denied")

// PermissionDeniedError provides details about a permission denial.
type PermissionDeniedError struct {
	Message string
}

func (e *PermissionDeniedError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("event: permission denied: %s", e.Message)
	}
	return "event: permission denied"
}

func (e *PermissionDeniedError) Is(target error) bool {
	return target == ErrPermissionDenied
}

// RemoteError wraps an error from the remote event server.
type RemoteError struct {
	Code    codes.Code
	Message string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("event: remote error (%s): %s", e.Code, e.Message)
}

// fromGRPCError converts gRPC status errors to transport errors.
func fromGRPCError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch st.Code() {
	case codes.OK:
		return nil
	case codes.NotFound:
		return transport.ErrEventNotRegistered
	case codes.AlreadyExists:
		return transport.ErrEventAlreadyExists
	case codes.Unavailable:
		return transport.ErrTransportClosed
	case codes.DeadlineExceeded:
		return transport.ErrPublishTimeout
	case codes.Aborted:
		return transport.ErrSubscriptionClosed
	case codes.FailedPrecondition:
		return transport.ErrNoSubscribers
	case codes.PermissionDenied, codes.Unauthenticated:
		return &PermissionDeniedError{Message: st.Message()}
	case codes.Canceled:
		return context.Canceled
	case codes.InvalidArgument:
		return &RemoteError{Code: st.Code(), Message: st.Message()}
	default:
		return &RemoteError{Code: st.Code(), Message: st.Message()}
	}
}
