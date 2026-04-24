package service

import (
	"log/slog"
	"time"
)

// serviceOptions holds configuration for the Service.
type serviceOptions struct {
	guard      SecurityGuard
	logger     *slog.Logger
	ackTimeout time.Duration
}

// Option configures the Service.
type Option func(*serviceOptions)

// WithSecurityGuard sets the security guard for the service.
// The guard handles both authentication (extracting identity from context)
// and authorization (checking if the identity may perform the action).
func WithSecurityGuard(g SecurityGuard) Option {
	return func(o *serviceOptions) {
		o.guard = g
	}
}

// WithLogger sets the logger for the service.
func WithLogger(l *slog.Logger) Option {
	return func(o *serviceOptions) {
		o.logger = l
	}
}

// WithAckTimeout sets the timeout for message acknowledgment.
// Messages not acknowledged within this duration are automatically nacked.
// Default is 30 seconds.
func WithAckTimeout(d time.Duration) Option {
	return func(o *serviceOptions) {
		if d > 0 {
			o.ackTimeout = d
		}
	}
}
