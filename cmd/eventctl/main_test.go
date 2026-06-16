package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// ── helper: tiny fake server ──────────────────────────────────────────────────

func newFakeServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	// events list
	mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"events": []string{"orders.created", "orders.shipped"}})
	})

	// events pub
	mux.HandleFunc("POST /v1/events/{name}/messages", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"id": "msg-test-123"})
	})

	// events health
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"status": 1, "message": "healthy"})
	})

	// events sub (SSE) — sends one message then closes
	mux.HandleFunc("GET /v1/events/{name}/stream", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		payload, _ := json.Marshal(sseMsg{
			Type:    "message",
			ID:      "msg-sse-1",
			Source:  "test",
			Payload: []byte("hello"),
		})
		w.Write([]byte("event: message\ndata: " + string(payload) + "\n\n"))
	})

	// scheduler list
	mux.HandleFunc("GET /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		msgs := []map[string]any{
			{"id": "sched-1", "event_name": "orders.reminder", "retry_count": 0},
		}
		json.NewEncoder(w).Encode(map[string]any{"messages": msgs, "total_count": 1})
	})

	// scheduler get
	mux.HandleFunc("GET /v1/messages/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "missing" {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"message": "not found", "code": 5})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"message": map[string]any{"id": id, "event_name": "orders.reminder"},
		})
	})

	return httptest.NewServer(mux)
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestEventsList(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	out := captureStdout(t, func() {
		if err := c.eventsList(); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "orders.created") {
		t.Errorf("expected orders.created in output, got: %s", out)
	}
	if !strings.Contains(out, "orders.shipped") {
		t.Errorf("expected orders.shipped in output, got: %s", out)
	}
}

func TestEventsPub(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	out := captureStdout(t, func() {
		if err := c.eventsPub("orders.created", `{"order_id":"1"}`, nil); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "msg-test-123") {
		t.Errorf("expected message ID in output, got: %s", out)
	}
}

func TestEventsSub(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	out := captureStdout(t, func() {
		if err := c.eventsSub("orders.created", "latest"); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "msg-sse-1") {
		t.Errorf("expected msg-sse-1 in output, got: %s", out)
	}
	if !strings.Contains(out, "hello") {
		t.Errorf("expected payload 'hello' in output, got: %s", out)
	}
}

// TestEventsHealth_Unhealthy asserts that a non-2xx health response is surfaced
// as an error rather than printed as if successful.
func TestEventsHealth_Unhealthy(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": "transport down"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	err := c.eventsHealth()
	if err == nil {
		t.Fatal("expected error for 503 health response, got nil")
	}
	if !strings.Contains(err.Error(), "transport down") {
		t.Errorf("expected server error message, got: %v", err)
	}
}

func TestEventsHealth(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	out := captureStdout(t, func() {
		if err := c.eventsHealth(); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "healthy") {
		t.Errorf("expected 'healthy' in output, got: %s", out)
	}
}

func TestSchedulerList(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	out := captureStdout(t, func() {
		if err := c.schedulerList([]string{"-event", "orders.reminder"}); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "sched-1") {
		t.Errorf("expected sched-1 in output, got: %s", out)
	}
}

func TestSchedulerGet(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	out := captureStdout(t, func() {
		if err := c.schedulerGet("sched-42"); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "sched-42") {
		t.Errorf("expected sched-42 in output, got: %s", out)
	}
}

func TestSchedulerGet_NotFound(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	err := c.schedulerGet("missing")
	if err == nil {
		t.Fatal("expected error for missing ID, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestSchedulerHealth(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL}

	out := captureStdout(t, func() {
		if err := c.schedulerHealth(); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "healthy") {
		t.Errorf("expected 'healthy' in output, got: %s", out)
	}
}

func TestReadSSE(t *testing.T) {
	body := "event: message\ndata: hello\n\nevent: message\ndata: world\n\n"
	var collected []string
	err := readSSE(strings.NewReader(body), func(data string) {
		collected = append(collected, data)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(collected) != 2 {
		t.Fatalf("expected 2 SSE events, got %d", len(collected))
	}
	if collected[0] != "hello" || collected[1] != "world" {
		t.Errorf("unexpected SSE payloads: %v", collected)
	}
}

func TestReadSSE_SkipsHeartbeats(t *testing.T) {
	body := ": heartbeat\n\nevent: message\ndata: actual\n\n"
	var collected []string
	_ = readSSE(strings.NewReader(body), func(data string) {
		collected = append(collected, data)
	})
	// The comment line produces no data: prefix, so only the "actual" event fires.
	if len(collected) != 1 || collected[0] != "actual" {
		t.Errorf("expected 1 event, got %v", collected)
	}
}

func TestCheckStatus_Error(t *testing.T) {
	resp := &http.Response{
		StatusCode: 404,
		Body:       http.NoBody,
	}
	err := checkStatus(resp)
	if err == nil {
		t.Fatal("expected error for 404")
	}
}

// ── schema tests ──────────────────────────────────────────────────────────────

func newFakeSchemaServer(t *testing.T) *httptest.Server {
	t.Helper()
	schemas := map[string]map[string]any{}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/schemas", func(w http.ResponseWriter, _ *http.Request) {
		list := make([]any, 0, len(schemas))
		for _, s := range schemas {
			list = append(list, s)
		}
		json.NewEncoder(w).Encode(map[string]any{"schemas": list})
	})

	mux.HandleFunc("GET /v1/schemas/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		s, ok := schemas[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
			return
		}
		json.NewEncoder(w).Encode(s)
	})

	mux.HandleFunc("PUT /v1/schemas/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		body["name"] = name
		version := 1
		if existing, ok := schemas[name]; ok {
			if v, ok := existing["version"].(float64); ok {
				version = int(v) + 1
			}
		}
		body["version"] = version
		schemas[name] = body
		json.NewEncoder(w).Encode(body)
	})

	mux.HandleFunc("DELETE /v1/schemas/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		delete(schemas, name)
		w.WriteHeader(http.StatusNoContent)
	})

	return httptest.NewServer(mux)
}

func TestSchemaList(t *testing.T) {
	srv := newFakeSchemaServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}

	out := captureStdout(t, func() {
		if err := c.schemaList(); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "schemas") {
		t.Errorf("expected 'schemas' key in output, got: %s", out)
	}
}

func TestSchemaSet(t *testing.T) {
	srv := newFakeSchemaServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}

	out := captureStdout(t, func() {
		if err := c.schemaSet("orders.created", []string{
			"-description", "Order creation",
			"-timeout", "30s",
			"-retries", "3",
			"-monitor",
		}); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "orders.created") {
		t.Errorf("expected event name in output, got: %s", out)
	}
	if !strings.Contains(out, `"version"`) {
		t.Errorf("expected version field in output, got: %s", out)
	}
}

func TestSchemaGet(t *testing.T) {
	srv := newFakeSchemaServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}

	// Set first
	_ = c.schemaSet("orders.shipped", []string{"-description", "Shipped"})

	out := captureStdout(t, func() {
		if err := c.schemaGet("orders.shipped"); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "orders.shipped") {
		t.Errorf("expected event name in output, got: %s", out)
	}
}

func TestSchemaGet_NotFound(t *testing.T) {
	srv := newFakeSchemaServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}
	err := c.schemaGet("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing schema")
	}
}

func TestSchemaDelete(t *testing.T) {
	srv := newFakeSchemaServer(t)
	defer srv.Close()

	c := &cli{server: srv.URL, scheduler: srv.URL, schema: srv.URL}

	_ = c.schemaSet("orders.cancelled", []string{})

	out := captureStdout(t, func() {
		if err := c.schemaDelete("orders.cancelled"); err != nil {
			t.Fatal(err)
		}
	})

	if !strings.Contains(out, "deleted") {
		t.Errorf("expected 'deleted' in output, got: %s", out)
	}
}

// ── stdout capture ────────────────────────────────────────────────────────────

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()

	w.Close()
	var buf strings.Builder
	buf2 := make([]byte, 4096)
	for {
		n, err := r.Read(buf2)
		if n > 0 {
			buf.Write(buf2[:n])
		}
		if err != nil {
			break
		}
	}
	return buf.String()
}
