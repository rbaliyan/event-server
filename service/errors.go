package service

import (
	"errors"

	"github.com/rbaliyan/event/v3/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toGRPCError converts transport errors to gRPC status errors.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, transport.ErrTransportClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, transport.ErrEventNotRegistered):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, transport.ErrEventAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, transport.ErrPublishTimeout):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, transport.ErrSubscriptionClosed):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, transport.ErrNoSubscribers):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
