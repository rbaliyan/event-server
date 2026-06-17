package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// fakeClock is a controllable time source for deterministic circuit-breaker tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(0, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestCircuitBreaker_OpensAndResets(t *testing.T) {
	t.Parallel()

	const timeout = 30 * time.Second
	tr, err := New("localhost:9090", WithInsecure(), WithCircuitBreaker(3, timeout))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	clk := newFakeClock()
	tr.now = clk.Now

	// Below threshold: circuit stays closed.
	tr.recordFailure()
	tr.recordFailure()
	if tr.isCircuitOpen() {
		t.Fatal("circuit should not be open before reaching threshold")
	}

	// Third failure trips the breaker.
	tr.recordFailure()
	if !tr.isCircuitOpen() {
		t.Fatal("circuit should be open after reaching threshold")
	}

	// getClient must surface the open circuit.
	if _, err := tr.getClient(); err == nil || !strings.Contains(err.Error(), "circuit breaker open") {
		t.Fatalf("getClient error = %v, want one mentioning %q", err, "circuit breaker open")
	}

	// Not enough time elapsed: still open.
	clk.Advance(timeout)
	if !tr.isCircuitOpen() {
		t.Fatal("circuit should still be open at exactly the timeout boundary")
	}

	// Advance past the timeout: breaker resets on next check.
	clk.Advance(time.Nanosecond)
	if tr.isCircuitOpen() {
		t.Fatal("circuit should have reset after the timeout elapsed")
	}
}

func TestCircuitBreaker_RecordSuccessResetsFailures(t *testing.T) {
	t.Parallel()

	tr, err := New("localhost:9090", WithInsecure(), WithCircuitBreaker(3, 30*time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	clk := newFakeClock()
	tr.now = clk.Now

	tr.recordFailure()
	tr.recordFailure()
	tr.recordSuccess()

	tr.circuitMu.Lock()
	consecutive := tr.consecutiveFail
	tr.circuitMu.Unlock()
	if consecutive != 0 {
		t.Fatalf("consecutiveFail = %d, want 0 after recordSuccess", consecutive)
	}

	// After a reset the next two failures must not trip a threshold-3 breaker.
	tr.recordFailure()
	tr.recordFailure()
	if tr.isCircuitOpen() {
		t.Fatal("circuit should not be open: recordSuccess should have reset the failure count")
	}
}

func TestIsNonRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"event not registered", transport.ErrEventNotRegistered, true},
		{"event already exists", transport.ErrEventAlreadyExists, true},
		{"no subscribers", transport.ErrNoSubscribers, true},
		{"permission denied", &PermissionDeniedError{Message: "no"}, true},
		{"wrapped permission denied", errors.Join(errors.New("ctx"), &PermissionDeniedError{}), true},
		{"transport closed is retryable", transport.ErrTransportClosed, false},
		{"publish timeout is retryable", transport.ErrPublishTimeout, false},
		{"arbitrary error is retryable", errors.New("transient"), false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isNonRetryable(tt.err); got != tt.want {
				t.Fatalf("isNonRetryable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestRetry_StopsOnNonRetryable(t *testing.T) {
	t.Parallel()

	tr, err := New("localhost:9090", WithInsecure(), WithRetry(5, time.Millisecond, time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	calls := 0
	got := tr.retry(context.Background(), func(context.Context) error {
		calls++
		return transport.ErrEventNotRegistered
	})

	if calls != 1 {
		t.Fatalf("fn called %d times, want 1 (non-retryable should not retry)", calls)
	}
	if !errors.Is(got, transport.ErrEventNotRegistered) {
		t.Fatalf("retry error = %v, want ErrEventNotRegistered", got)
	}
}

func TestRetry_RetriesOnRetryable(t *testing.T) {
	t.Parallel()

	const maxRetries = 2
	tr, err := New("localhost:9090", WithInsecure(), WithRetry(maxRetries, time.Millisecond, time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	retryable := errors.New("transient failure")
	calls := 0
	got := tr.retry(context.Background(), func(context.Context) error {
		calls++
		return retryable
	})

	// One initial attempt plus maxRetries retries.
	if want := maxRetries + 1; calls != want {
		t.Fatalf("fn called %d times, want %d", calls, want)
	}
	if !errors.Is(got, retryable) {
		t.Fatalf("retry error = %v, want %v", got, retryable)
	}
}

func TestRetry_SucceedsAfterTransientFailure(t *testing.T) {
	t.Parallel()

	tr, err := New("localhost:9090", WithInsecure(), WithRetry(3, time.Millisecond, time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	calls := 0
	got := tr.retry(context.Background(), func(context.Context) error {
		calls++
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	})

	if got != nil {
		t.Fatalf("retry error = %v, want nil", got)
	}
	if calls != 2 {
		t.Fatalf("fn called %d times, want 2", calls)
	}
}
