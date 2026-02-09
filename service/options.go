package service

import (
	"log/slog"
	"time"
)

// serviceOptions holds configuration for the Service.
type serviceOptions struct {
	authorizer Authorizer
	logger     *slog.Logger
	ackTimeout time.Duration
}

// Option configures the Service.
type Option func(*serviceOptions)

// WithAuthorizer sets the authorizer for the service.
func WithAuthorizer(a Authorizer) Option {
	return func(o *serviceOptions) {
		o.authorizer = a
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
