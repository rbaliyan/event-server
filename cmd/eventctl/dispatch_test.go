package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

// ── combined fake server (events + scheduler + schema) ─────────────────────────

// newFakeAllServer serves the event, scheduler, and schema endpoints from a
// single mux so dispatch-level tests can exercise any subcommand against one
// base URL. Publish requests have their decoded payload recorded so callers can
// assert the request shape (e.g. the stdin "-" path).
func newFakeAllServer(t *testing.T) (*httptest.Server, *recorder) {
	t.Helper()
	rec := &recorder{}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"events": []string{"orders.created"}})
	})

	mux.HandleFunc("POST /v1/events/{name}/messages", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Payload []byte `json:"payload"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		rec.record(r.PathValue("name"), string(body.Payload))
		_ = json.NewEncoder(w).Encode(map[string]string{"id": "msg-dispatch-1"})
	})

	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 1, "message": "healthy"})
	})

	mux.HandleFunc("GET /v1/messages", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages":    []map[string]any{{"id": "sched-1"}},
			"total_count": 1,
		})
	})
	mux.HandleFunc("GET /v1/messages/{id}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message": map[string]any{"id": r.PathValue("id")},
		})
	})

	schemas := map[string]map[string]any{}
	mux.HandleFunc("GET /v1/schemas", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"schemas": []any{}})
	})
	mux.HandleFunc("GET /v1/schemas/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		s, ok := schemas[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(s)
	})
	mux.HandleFunc("PUT /v1/schemas/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		body["name"] = name
		rec.recordSchema(name, body)
		schemas[name] = body
		_ = json.NewEncoder(w).Encode(body)
	})
	mux.HandleFunc("DELETE /v1/schemas/{name}", func(w http.ResponseWriter, r *http.Request) {
		delete(schemas, r.PathValue("name"))
		w.WriteHeader(http.StatusNoContent)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, rec
}

// recorder captures publish payloads and schema bodies for assertions.
type recorder struct {
	mu      sync.Mutex
	pubs    []pubRecord
	schemas []schemaRecord
}

type pubRecord struct {
	event   string
	payload string
}

type schemaRecord struct {
	name string
	body map[string]any
}

func (r *recorder) record(event, payload string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pubs = append(r.pubs, pubRecord{event: event, payload: payload})
}

func (r *recorder) recordSchema(name string, body map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas = append(r.schemas, schemaRecord{name: name, body: body})
}

func (r *recorder) lastPub() (pubRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pubs) == 0 {
		return pubRecord{}, false
	}
	return r.pubs[len(r.pubs)-1], true
}

func (r *recorder) lastSchema() (schemaRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.schemas) == 0 {
		return schemaRecord{}, false
	}
	return r.schemas[len(r.schemas)-1], true
}

// ── dispatcher coverage ────────────────────────────────────────────────────────

// TestRun_Dispatch drives the top-level run() dispatcher and the per-family
// dispatchers (eventsCmd/schedulerCmd/schemaCmd) through both their success and
// usage-error paths. Cases that touch the network point at a single fake server.
func TestRun_Dispatch(t *testing.T) {

	srv, _ := newFakeAllServer(t)

	tests := []struct {
		name      string
		args      []string
		wantErr   bool
		errSubstr string
	}{
		// top-level dispatch
		{name: "no subcommand", args: []string{"events"}, wantErr: true, errSubstr: "requires a subcommand"},
		{name: "unknown top-level", args: []string{"bogus", "x"}, wantErr: true, errSubstr: "unknown command"},

		// events family
		{name: "events list ok", args: []string{"events", "list"}},
		{name: "events health ok", args: []string{"events", "health"}},
		{name: "events pub missing args", args: []string{"events", "pub"}, wantErr: true, errSubstr: "usage: events pub"},
		{name: "events sub missing args", args: []string{"events", "sub"}, wantErr: true, errSubstr: "usage: events sub"},
		{name: "events unknown sub", args: []string{"events", "frobnicate"}, wantErr: true, errSubstr: "frobnicate"},

		// scheduler family
		{name: "scheduler list ok", args: []string{"scheduler", "list"}},
		{name: "scheduler get missing args", args: []string{"scheduler", "get"}, wantErr: true, errSubstr: "usage: scheduler get"},
		{name: "scheduler health ok", args: []string{"scheduler", "health"}},
		{name: "scheduler unknown sub", args: []string{"scheduler", "wibble"}, wantErr: true, errSubstr: "wibble"},

		// schema family
		{name: "schema list ok", args: []string{"schema", "list"}},
		{name: "schema get missing args", args: []string{"schema", "get"}, wantErr: true, errSubstr: "usage: schema get"},
		{name: "schema set missing args", args: []string{"schema", "set"}, wantErr: true, errSubstr: "usage: schema set"},
		{name: "schema delete missing args", args: []string{"schema", "delete"}, wantErr: true, errSubstr: "usage: schema delete"},
		{name: "schema unknown sub", args: []string{"schema", "splork"}, wantErr: true, errSubstr: "splork"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}
			// Swallow stdout: the success cases print JSON we don't care about here.
			var err error
			withDevNullStdout(t, func() { err = c.run(tt.args) })

			switch {
			case tt.wantErr && err == nil:
				t.Fatalf("expected error for args %v, got nil", tt.args)
			case !tt.wantErr && err != nil:
				t.Fatalf("unexpected error for args %v: %v", tt.args, err)
			case tt.wantErr && tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr):
				t.Errorf("expected error containing %q, got: %v", tt.errSubstr, err)
			}
		})
	}
}

// TestRun_SchemaDispatch verifies that `schema set`/`get`/`delete` routed
// through run() reach the server and round-trip the event name.
func TestRun_SchemaDispatch(t *testing.T) {

	srv, rec := newFakeAllServer(t)
	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}

	withDevNullStdout(t, func() {
		if err := c.run([]string{"schema", "set", "orders.created", "-description", "d", "-monitor"}); err != nil {
			t.Fatalf("schema set: %v", err)
		}
		if err := c.run([]string{"schema", "get", "orders.created"}); err != nil {
			t.Fatalf("schema get: %v", err)
		}
		if err := c.run([]string{"schema", "delete", "orders.created"}); err != nil {
			t.Fatalf("schema delete: %v", err)
		}
	})

	sr, ok := rec.lastSchema()
	if !ok {
		t.Fatal("expected a schema PUT to be recorded")
	}
	if sr.name != "orders.created" {
		t.Errorf("expected schema name orders.created, got %q", sr.name)
	}
	if sr.body["enable_monitor"] != true {
		t.Errorf("expected enable_monitor=true in PUT body, got: %v", sr.body["enable_monitor"])
	}
}

// ── schema set: full middleware-flag coverage ──────────────────────────────────

// TestSchemaSet_AllFlags asserts every middleware/config flag is marshalled into
// the PUT body with the expected shape.
func TestSchemaSet_AllFlags(t *testing.T) {

	srv, rec := newFakeAllServer(t)
	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}

	withDevNullStdout(t, func() {
		err := c.schemaSet("orders.shipped", []string{
			"-description", "Shipped event",
			"-timeout", "30s",
			"-retries", "5",
			"-backoff", "2s",
			"-monitor",
			"-idempotency",
			"-poison",
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	sr, ok := rec.lastSchema()
	if !ok {
		t.Fatal("expected a schema PUT to be recorded")
	}

	checks := map[string]any{
		"description":        "Shipped event",
		"sub_timeout":        float64(30 * 1e9),
		"max_retries":        float64(5),
		"retry_backoff":      float64(2 * 1e9),
		"enable_monitor":     true,
		"enable_idempotency": true,
		"enable_poison":      true,
	}
	for k, want := range checks {
		if got := sr.body[k]; got != want {
			t.Errorf("body[%q] = %v (%T), want %v (%T)", k, got, got, want, want)
		}
	}
}

// ── stdin "-" payload path for events pub ──────────────────────────────────────

// TestEventsPub_Stdin swaps os.Stdin for a pipe carrying a payload and verifies
// `events pub <event> -` reads it and sends it to the server. Not parallel: it
// mutates the os.Stdin and os.Stdout globals.
func TestEventsPub_Stdin(t *testing.T) {
	srv, rec := newFakeAllServer(t)
	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}

	const want = `{"order_id":"from-stdin"}`

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldStdin := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = oldStdin })

	go func() {
		_, _ = io.WriteString(w, want)
		_ = w.Close()
	}()

	out := captureStdout(t, func() {
		if err := c.run([]string{"events", "pub", "orders.created", "-"}); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "msg-dispatch-1") {
		t.Errorf("expected published id in output, got: %s", out)
	}

	pr, ok := rec.lastPub()
	if !ok {
		t.Fatal("expected a publish to be recorded")
	}
	if pr.event != "orders.created" {
		t.Errorf("expected event orders.created, got %q", pr.event)
	}
	if pr.payload != want {
		t.Errorf("expected stdin payload %q to be sent, got %q", want, pr.payload)
	}
}

// ── error propagation through dispatch (4xx/5xx) ───────────────────────────────

// TestRun_ErrorPropagation confirms that subcommands hitting an error status
// return a non-nil error (the checkStatus path) for events, scheduler, and
// schema families.
func TestRun_ErrorPropagation(t *testing.T) {

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "events boom"})
	})
	mux.HandleFunc("GET /v1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "scheduler down"})
	})
	mux.HandleFunc("GET /v1/schemas", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "schema bad"})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}

	tests := []struct {
		name      string
		args      []string
		errSubstr string
	}{
		{name: "events list 500", args: []string{"events", "list"}, errSubstr: "events boom"},
		{name: "scheduler list 503", args: []string{"scheduler", "list"}, errSubstr: "scheduler down"},
		{name: "schema list 400", args: []string{"schema", "list"}, errSubstr: "schema bad"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var err error
			withDevNullStdout(t, func() { err = c.run(tt.args) })
			if err == nil {
				t.Fatalf("expected error for %v, got nil", tt.args)
			}
			if !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("expected error containing %q, got: %v", tt.errSubstr, err)
			}
		})
	}
}

// ── helper: discard stdout for the duration of fn ──────────────────────────────

// withDevNullStdout redirects os.Stdout to a sink while fn runs, then restores
// it. Used by dispatch tests whose success paths print JSON we don't assert on.
// Tests using this MUST NOT be parallel with each other while mutating the
// global; here they run within their own subtests that do not interleave the
// swap because each restores before returning. The drain goroutine prevents the
// pipe buffer from blocking writers.
func withDevNullStdout(t *testing.T, fn func()) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, r)
		close(done)
	}()
	old := os.Stdout
	os.Stdout = w
	defer func() {
		os.Stdout = old
		_ = w.Close()
		<-done
		_ = r.Close()
	}()
	fn()
}
