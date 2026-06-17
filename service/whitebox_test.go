package service

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeClock is a controllable time source for deterministic time-based tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestTracker builds an ackTracker WITHOUT its background reap goroutine so
// reaping can be driven deterministically via reap() against a fake clock.
// Do not call Stop() on the returned tracker (no goroutine to join).
func newTestTracker(clk *fakeClock, timeout time.Duration) *ackTracker {
	return &ackTracker{
		entries: make(map[string]*ackEntry),
		timeout: timeout,
		logger:  quietLogger(),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
		now:     clk.now,
	}
}

func TestAckTracker_ReapStaleEntries(t *testing.T) {
	t.Parallel()
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	tr := newTestTracker(clk, time.Hour)

	var gotErr error
	var called bool
	id := tr.Track("s1", func(e error) error {
		called = true
		gotErr = e
		return nil
	})

	tr.reap()
	if called {
		t.Fatal("entry reaped before it was stale")
	}

	clk.advance(time.Hour + time.Minute)
	tr.reap()
	if !called {
		t.Fatal("expected stale entry to be nacked")
	}
	if !errors.Is(gotErr, errAckTimeout) {
		t.Fatalf("nack error = %v, want errAckTimeout", gotErr)
	}
	if tr.Ack(id, nil) {
		t.Fatal("Ack of a reaped id must return false")
	}
}

func TestAckTracker_AckSuccess(t *testing.T) {
	t.Parallel()
	clk := &fakeClock{t: time.Unix(0, 0)}
	tr := newTestTracker(clk, time.Hour)

	var gotErr error
	gotErr = errors.New("sentinel")
	id := tr.Track("s1", func(e error) error {
		gotErr = e
		return nil
	})

	if !tr.Ack(id, nil) {
		t.Fatal("Ack of a tracked id must return true")
	}
	if gotErr != nil {
		t.Fatalf("ack fn error = %v, want nil", gotErr)
	}
	if tr.Ack(id, nil) {
		t.Fatal("second Ack of same id must return false")
	}
}

func TestAckTracker_NackStreamScopesToStream(t *testing.T) {
	t.Parallel()
	clk := &fakeClock{t: time.Unix(0, 0)}
	tr := newTestTracker(clk, time.Hour)

	var s1Err, s2Called = error(nil), false
	id1 := tr.Track("s1", func(e error) error { s1Err = e; return nil })
	id2 := tr.Track("s2", func(error) error { s2Called = true; return nil })

	reason := errors.New("stream gone")
	tr.NackStream("s1", reason)

	if !errors.Is(s1Err, reason) {
		t.Fatalf("s1 nack error = %v, want %v", s1Err, reason)
	}
	if s2Called {
		t.Fatal("NackStream must not touch entries from other streams")
	}
	if tr.Ack(id1, nil) {
		t.Fatal("s1 entry should have been removed by NackStream")
	}
	if !tr.Ack(id2, nil) {
		t.Fatal("s2 entry should still be ackable")
	}
}

func TestAckTracker_AckUnknown(t *testing.T) {
	t.Parallel()
	clk := &fakeClock{t: time.Unix(0, 0)}
	tr := newTestTracker(clk, time.Hour)
	if tr.Ack("does-not-exist", nil) {
		t.Fatal("Ack of unknown id must return false")
	}
}

func TestAckTracker_StopNacksRemaining(t *testing.T) {
	t.Parallel()
	// Uses the real constructor (with the reap goroutine); Stop must nack
	// everything still pending. Large timeout ensures the reaper itself does
	// not fire during the test.
	tr := newAckTracker(time.Hour, quietLogger())

	var gotErr error
	tr.Track("s1", func(e error) error { gotErr = e; return nil })
	tr.Stop()

	if !errors.Is(gotErr, errAckTimeout) {
		t.Fatalf("Stop nack error = %v, want errAckTimeout", gotErr)
	}
}

func TestToGRPCError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"nil", nil, codes.OK},
		{"transport closed", transport.ErrTransportClosed, codes.Unavailable},
		{"not registered", transport.ErrEventNotRegistered, codes.NotFound},
		{"already exists", transport.ErrEventAlreadyExists, codes.AlreadyExists},
		{"publish timeout", transport.ErrPublishTimeout, codes.DeadlineExceeded},
		{"subscription closed", transport.ErrSubscriptionClosed, codes.Aborted},
		{"no subscribers", transport.ErrNoSubscribers, codes.FailedPrecondition},
		{"arbitrary", errors.New("boom"), codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := toGRPCError(tt.err)
			if tt.err == nil {
				if got != nil {
					t.Fatalf("toGRPCError(nil) = %v, want nil", got)
				}
				return
			}
			if status.Code(got) != tt.want {
				t.Fatalf("toGRPCError(%v) code = %v, want %v", tt.err, status.Code(got), tt.want)
			}
		})
	}
}
