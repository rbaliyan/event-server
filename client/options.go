package client

import (
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type options struct {
	// Connection options
	dialOpts  []grpc.DialOption
	secure    bool
	tlsConfig *tls.Config

	// Retry/resilience options
	maxRetries       int
	retryBackoff     time.Duration
	maxBackoff       time.Duration
	callTimeout      time.Duration
	enableCircuit    bool
	circuitThreshold int
	circuitTimeout   time.Duration

	// Subscribe options
	subscribeBufferSize    int
	subscribeReconnect     bool
	subscribeReconnectWait time.Duration
	subscribeMaxErrors     int

	// Keepalive options
	keepaliveTime    time.Duration
	keepaliveTimeout time.Duration

	// Observability
	onStateChange func(state ConnState)
	onStreamError func(err error)
}

// ConnState represents the connection state.
type ConnState int

const (
	ConnStateDisconnected ConnState = iota
	ConnStateConnecting
	ConnStateConnected
	_ // reserved
	ConnStateClosed
)

func (s ConnState) String() string {
	switch s {
	case ConnStateDisconnected:
		return "disconnected"
	case ConnStateConnecting:
		return "connecting"
	case ConnStateConnected:
		return "connected"
	case ConnStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

func defaultOptions() *options {
	return &options{
		maxRetries:             3,
		retryBackoff:           100 * time.Millisecond,
		maxBackoff:             5 * time.Second,
		subscribeBufferSize:    100,
		subscribeReconnect:     true,
		subscribeReconnectWait: time.Second,
		subscribeMaxErrors:     10,
		keepaliveTime:          30 * time.Second,
		keepaliveTimeout:       10 * time.Second,
		circuitThreshold:       5,
		circuitTimeout:         30 * time.Second,
	}
}

// Option configures the RemoteTransport.
type Option func(*options)

// WithDialOptions appends additional gRPC dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOpts = append(o.dialOpts, opts...)
	}
}

// WithTLS enables TLS with the given config.
// If config is nil, uses system default TLS config.
func WithTLS(config *tls.Config) Option {
	return func(o *options) {
		o.secure = true
		o.tlsConfig = config
	}
}

// WithInsecure explicitly enables insecure connections (no TLS).
// This should only be used for development/testing.
func WithInsecure() Option {
	return func(o *options) {
		o.secure = false
		o.tlsConfig = nil
	}
}

// WithRetry configures retry behavior for failed operations.
func WithRetry(maxRetries int, initialBackoff, maxBackoff time.Duration) Option {
	return func(o *options) {
		if maxRetries < 0 {
			maxRetries = 0
		}
		if initialBackoff <= 0 {
			initialBackoff = 100 * time.Millisecond
		}
		if maxBackoff <= 0 {
			maxBackoff = 5 * time.Second
		}
		o.maxRetries = maxRetries
		o.retryBackoff = initialBackoff
		o.maxBackoff = maxBackoff
	}
}

// WithCallTimeout sets a per-attempt timeout for each gRPC call within a retry loop.
func WithCallTimeout(d time.Duration) Option {
	return func(o *options) {
		if d < 0 {
			d = 0
		}
		o.callTimeout = d
	}
}

// WithCircuitBreaker enables circuit breaker for resilience.
func WithCircuitBreaker(threshold int, timeout time.Duration) Option {
	return func(o *options) {
		if threshold < 1 {
			threshold = 1
		}
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		o.enableCircuit = true
		o.circuitThreshold = threshold
		o.circuitTimeout = timeout
	}
}

// WithSubscribeBufferSize sets the buffer size for subscribe message channels.
func WithSubscribeBufferSize(size int) Option {
	return func(o *options) {
		if size < 0 {
			size = 0
		}
		o.subscribeBufferSize = size
	}
}

// WithSubscribeReconnect enables automatic reconnection for subscribe streams.
func WithSubscribeReconnect(enabled bool, waitTime time.Duration) Option {
	return func(o *options) {
		o.subscribeReconnect = enabled
		o.subscribeReconnectWait = waitTime
	}
}

// WithSubscribeMaxErrors sets the maximum number of consecutive subscribe errors
// before giving up. Default is 10.
func WithSubscribeMaxErrors(n int) Option {
	return func(o *options) {
		if n < 1 {
			n = 1
		}
		o.subscribeMaxErrors = n
	}
}

// WithKeepalive configures gRPC keepalive parameters.
func WithKeepalive(time, timeout time.Duration) Option {
	return func(o *options) {
		o.keepaliveTime = time
		o.keepaliveTimeout = timeout
	}
}

// WithStateCallback sets a callback for connection state changes.
func WithStateCallback(fn func(ConnState)) Option {
	return func(o *options) {
		o.onStateChange = fn
	}
}

// WithStreamErrorCallback sets a callback for subscribe stream errors.
func WithStreamErrorCallback(fn func(error)) Option {
	return func(o *options) {
		o.onStreamError = fn
	}
}

// buildDialOpts constructs the gRPC dial options from configuration.
func (o *options) buildDialOpts() []grpc.DialOption {
	opts := make([]grpc.DialOption, 0, len(o.dialOpts)+3)

	// Credentials
	if o.secure {
		if o.tlsConfig != nil {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(o.tlsConfig)))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
		}
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Keepalive
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                o.keepaliveTime,
		Timeout:             o.keepaliveTimeout,
		PermitWithoutStream: true,
	}))

	// User-provided options (can override defaults)
	opts = append(opts, o.dialOpts...)

	return opts
}
