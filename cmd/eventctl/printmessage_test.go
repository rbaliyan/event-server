package main

import (
	"strings"
	"testing"
)

func TestPrintMessage(t *testing.T) {
	// Invalid JSON falls back to printing the raw line.
	if out := captureStdout(t, func() { printMessage("not json at all") }); !strings.Contains(out, "not json at all") {
		t.Errorf("invalid JSON: expected raw passthrough, got %q", out)
	}

	// Heartbeat and empty-type frames produce no output.
	if out := captureStdout(t, func() { printMessage(`{"type":"heartbeat"}`) }); strings.TrimSpace(out) != "" {
		t.Errorf("heartbeat should print nothing, got %q", out)
	}

	// A full message frame is formatted with payload, timestamp, and metadata.
	msg := `{"type":"message","id":"m1","source":"svc","payload":"aGk=","retry_count":1,` +
		`"timestamp":"2026-01-01T00:00:00Z","metadata":{"k":"v"}}`
	out := captureStdout(t, func() { printMessage(msg) })
	for _, want := range []string{"m1", "svc", "hi", "2026-01-01T00:00:00Z", `"k"`} {
		if !strings.Contains(out, want) {
			t.Errorf("message output missing %q, got:\n%s", want, out)
		}
	}

	// Error frames go to stderr and must not panic (covers the error branch).
	captureStdout(t, func() { printMessage(`{"type":"error","error":"boom"}`) })
}
