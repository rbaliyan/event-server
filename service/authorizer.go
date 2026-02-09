// Package service provides the gRPC EventService implementation.
package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Operation represents an event operation for authorization.
type Operation int

const (
	OperationPublish Operation = iota
	OperationSubscribe
	OperationRegister
	OperationUnregister
	OperationList
)

func (o Operation) String() string {
	switch o {
	case OperationPublish:
		return "publish"
	case OperationSubscribe:
		return "subscribe"
	case OperationRegister:
		return "register"
	case OperationUnregister:
		return "unregister"
	case OperationList:
		return "list"
	default:
		return "unknown"
	}
}

// AuthRequest contains information about an authorization request.
type AuthRequest struct {
	Event     string
	Operation Operation
}

// Authorizer defines the interface for authorization decisions.
// Implement this interface to integrate with your auth system.
//
// The context contains any authentication information extracted
// by your authentication middleware (e.g., user ID, roles, claims).
type Authorizer interface {
	// Authorize checks if the operation is allowed.
	// Return nil to allow, or an error (typically codes.PermissionDenied) to deny.
	Authorize(ctx context.Context, req AuthRequest) error
}

// AllowAll returns an authorizer that permits all operations.
// Use only for development/testing.
func AllowAll() Authorizer {
	return allowAllAuthorizer{}
}

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(context.Context, AuthRequest) error {
	return nil
}

// DenyAll returns an authorizer that denies all operations.
// Useful as a fallback when no authorizer is configured.
func DenyAll() Authorizer {
	return denyAllAuthorizer{}
}

type denyAllAuthorizer struct{}

func (denyAllAuthorizer) Authorize(context.Context, AuthRequest) error {
	return status.Errorf(codes.PermissionDenied, "no authorizer configured")
}
