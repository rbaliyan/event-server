package service

import (
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ackEntry holds a pending message acknowledgment.
type ackEntry struct {
	ackFn     func(error) error
	createdAt time.Time
	streamID  string // identifies the subscribe stream that owns this ack
}

// ackTracker maps ack IDs to transport-level message Ack functions.
// When a message is streamed to a remote client, the server stores the
// message's Ack function here. When the client sends an Ack RPC, the
// server looks up and invokes the stored function.
type ackTracker struct {
	mu      sync.Mutex
	entries map[string]*ackEntry
	timeout time.Duration
	logger  *slog.Logger
	stopCh  chan struct{}
	done    chan struct{}
}

// newAckTracker creates a new ack tracker with the given timeout.
// The tracker runs a background goroutine that nacks stale entries.
func newAckTracker(timeout time.Duration, logger *slog.Logger) *ackTracker {
	t := &ackTracker{
		entries: make(map[string]*ackEntry),
		timeout: timeout,
		logger:  logger,
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	go t.reapLoop()
	return t
}

// Track stores an ack function and returns a unique ack ID.
func (t *ackTracker) Track(streamID string, ackFn func(error) error) string {
	id := uuid.New().String()
	t.mu.Lock()
	t.entries[id] = &ackEntry{
		ackFn:     ackFn,
		createdAt: time.Now(),
		streamID:  streamID,
	}
	t.mu.Unlock()
	return id
}

// Ack acknowledges or nacks a message by ack ID.
// Pass nil for success, or an error for nack (triggers redelivery).
// Returns false if the ack ID was not found (expired or already acked).
func (t *ackTracker) Ack(ackID string, err error) bool {
	t.mu.Lock()
	entry, ok := t.entries[ackID]
	if ok {
		delete(t.entries, ackID)
	}
	t.mu.Unlock()

	if !ok {
		return false
	}

	if ackErr := entry.ackFn(err); ackErr != nil {
		t.logger.Warn("ack function returned error",
			"ack_id", ackID,
			"error", ackErr)
	}
	return true
}

// NackStream nacks all pending acks for a given stream.
// Called when a subscribe stream disconnects.
func (t *ackTracker) NackStream(streamID string, reason error) {
	t.mu.Lock()
	var toNack []*ackEntry
	for id, entry := range t.entries {
		if entry.streamID == streamID {
			toNack = append(toNack, entry)
			delete(t.entries, id)
		}
	}
	t.mu.Unlock()

	for _, entry := range toNack {
		if err := entry.ackFn(reason); err != nil {
			t.logger.Warn("nack on stream disconnect failed", "error", err)
		}
	}

	if len(toNack) > 0 {
		t.logger.Debug("nacked pending acks on stream disconnect",
			"stream_id", streamID,
			"count", len(toNack))
	}
}

// Stop stops the background reap goroutine and nacks all pending entries.
func (t *ackTracker) Stop() {
	close(t.stopCh)
	<-t.done

	// Nack all remaining entries
	t.mu.Lock()
	remaining := make([]*ackEntry, 0, len(t.entries))
	for _, entry := range t.entries {
		remaining = append(remaining, entry)
	}
	t.entries = make(map[string]*ackEntry)
	t.mu.Unlock()

	for _, entry := range remaining {
		_ = entry.ackFn(errAckTimeout)
	}
}

// reapLoop periodically removes and nacks stale entries.
func (t *ackTracker) reapLoop() {
	defer close(t.done)
	ticker := time.NewTicker(t.timeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.reap()
		}
	}
}

func (t *ackTracker) reap() {
	now := time.Now()
	t.mu.Lock()
	var stale []*ackEntry
	for id, entry := range t.entries {
		if now.Sub(entry.createdAt) > t.timeout {
			stale = append(stale, entry)
			delete(t.entries, id)
		}
	}
	t.mu.Unlock()

	for _, entry := range stale {
		if err := entry.ackFn(errAckTimeout); err != nil {
			t.logger.Warn("stale ack nack failed", "error", err)
		}
	}

	if len(stale) > 0 {
		t.logger.Debug("reaped stale acks", "count", len(stale))
	}
}
