// Package service provides the gRPC EventService implementation.
package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Identity represents an authenticated caller.
type Identity interface {
	UserID() string
	Claims() map[string]any
}

// Decision is the result of an authorization check.
type Decision struct {
	Allowed bool
	Scope   string // e.g. "all", "owned", "tenant"
	Reason  string // human-readable explanation for denied requests
}

// SecurityGuard handles authentication and authorization for incoming RPCs.
// Implement this interface to integrate with your auth system.
//
// Authenticate is called first to extract the caller's identity from the
// incoming context (e.g. from gRPC metadata or JWT tokens). Authorize is
// then called with the identity and the gRPC full method name as the action
// (e.g. "/event.v1.EventService/Publish").
//
// Wire the guard into gRPC with Service.UnaryInterceptor() and
// Service.StreamInterceptor().
type SecurityGuard interface {
	Authenticate(ctx context.Context) (Identity, error)
	Authorize(ctx context.Context, id Identity, action string) (Decision, error)
}

type identityKey struct{}

// contextWithIdentity stores the authenticated identity in the context.
func contextWithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFromContext retrieves the authenticated identity from the context.
// Returns false if no identity was stored (i.e. request did not pass through
// the service interceptors).
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	return id, ok
}

// AllowAll returns a SecurityGuard that permits all operations.
// Use only for development/testing.
func AllowAll() SecurityGuard {
	return allowAllGuard{}
}

type allowAllGuard struct{}

func (allowAllGuard) Authenticate(_ context.Context) (Identity, error) {
	return &simpleIdentity{claims: map[string]any{}}, nil
}

func (allowAllGuard) Authorize(_ context.Context, _ Identity, _ string) (Decision, error) {
	return Decision{Allowed: true, Scope: "all"}, nil
}

// DenyAll returns a SecurityGuard that denies all operations.
// This is the default when no guard is configured — forces explicit setup.
func DenyAll() SecurityGuard {
	return denyAllGuard{}
}

type denyAllGuard struct{}

func (denyAllGuard) Authenticate(_ context.Context) (Identity, error) {
	return &simpleIdentity{claims: map[string]any{}}, nil
}

func (denyAllGuard) Authorize(_ context.Context, _ Identity, _ string) (Decision, error) {
	return Decision{Allowed: false, Reason: "no security guard configured"}, nil
}

type simpleIdentity struct {
	userID string
	claims map[string]any
}

func (i *simpleIdentity) UserID() string         { return i.userID }
func (i *simpleIdentity) Claims() map[string]any { return i.claims }

func permissionDenied(d Decision) error {
	if d.Reason != "" {
		return status.Errorf(codes.PermissionDenied, "%s", d.Reason)
	}
	return status.Error(codes.PermissionDenied, "permission denied")
}
