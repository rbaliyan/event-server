package gateway

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func grpcNotFoundErr(msg string) error {
	return status.Error(codes.NotFound, msg)
}

func grpcPermDeniedErr(msg string) error {
	return status.Error(codes.PermissionDenied, msg)
}

func grpcInvalidArgErr(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}
