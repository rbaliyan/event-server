package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"github.com/rbaliyan/event/v3/transport/channel"
	"github.com/coder/websocket"
)

func TestParseSubscribeQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		checkFn  func(t *testing.T, req *eventpb.SubscribeRequest)
	}{
		{
			name:  "defaults",
			query: "",
			checkFn: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.Event != "test" {
					t.Errorf("event = %q, want %q", req.Event, "test")
				}
				if req.DeliveryMode != eventpb.DeliveryMode_DELIVERY_MODE_UNSPECIFIED {
					t.Errorf("delivery_mode = %v, want UNSPECIFIED", req.DeliveryMode)
				}
			},
		},
		{
			name:  "worker_pool",
			query: "delivery_mode=worker_pool&worker_group=mygroup",
			checkFn: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.DeliveryMode != eventpb.DeliveryMode_DELIVERY_MODE_WORKER_POOL {
					t.Errorf("delivery_mode = %v, want WORKER_POOL", req.DeliveryMode)
				}
				if req.WorkerGroup != "mygroup" {
					t.Errorf("worker_group = %q, want %q", req.WorkerGroup, "mygroup")
				}
			},
		},
		{
			name:  "broadcast",
			query: "delivery_mode=broadcast",
			checkFn: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.DeliveryMode != eventpb.DeliveryMode_DELIVERY_MODE_BROADCAST {
					t.Errorf("delivery_mode = %v, want BROADCAST", req.DeliveryMode)
				}
			},
		},
		{
			name:  "start_from_beginning",
			query: "start_from=beginning",
			checkFn: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.StartFrom != eventpb.StartPosition_START_POSITION_BEGINNING {
					t.Errorf("start_from = %v, want BEGINNING", req.StartFrom)
				}
			},
		},
		{
			name:  "start_from_latest",
			query: "start_from=latest",
			checkFn: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.StartFrom != eventpb.StartPosition_START_POSITION_LATEST {
					t.Errorf("start_from = %v, want LATEST", req.StartFrom)
				}
			},
		},
		{
			name:  "latest_only",
			query: "latest_only=true",
			checkFn: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if !req.LatestOnly {
					t.Error("latest_only should be true")
				}
			},
		},
		{
			name:  "consumer_id",
			query: "consumer_id=consumer-1",
			checkFn: func(t *testing.T, req *eventpb.SubscribeRequest) {
				if req.ConsumerId != "consumer-1" {
					t.Errorf("consumer_id = %q, want %q", req.ConsumerId, "consumer-1")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/v1/events/test/subscribe?"+tt.query, nil)
			req := parseSubscribeQuery("test", r)
			tt.checkFn(t, req)
		})
	}
}

func TestSanitizeSSEField(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"hello\nworld", "helloworld"},
		{"hello\r\nworld", "helloworld"},
		{"hello\rworld", "helloworld"},
		{"no special chars", "no special chars"},
	}
	for _, tt := range tests {
		got := sanitizeSSEField(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeSSEField(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsForwardableHeader(t *testing.T) {
	tests := []struct {
		header string
		want   bool
	}{
		{"authorization", true},
		{"x-custom-header", true},
		{"x-request-id", true},
		{"x-forwarded-for", false},
		{"x-forwarded-host", false},
		{"x-forwarded-proto", false},
		{"x-forwarded-port", false},
		{"x-real-ip", false},
		{"content-type", false},
		{"accept", false},
	}
	for _, tt := range tests {
		got := isForwardableHeader(tt.header)
		if got != tt.want {
			t.Errorf("isForwardableHeader(%q) = %v, want %v", tt.header, got, tt.want)
		}
	}
}

func TestWriteHTTPError(t *testing.T) {
	tests := []struct {
		name       string
		errMsg     string
		wantCode   int
		wantBody   string
	}{
		{"not_found", "event not found", http.StatusNotFound, "event not found"},
		{"permission_denied", "access denied", http.StatusForbidden, "access denied"},
		{"invalid_argument", "bad request", http.StatusBadRequest, "bad request"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			var err error
			switch tt.wantCode {
			case http.StatusNotFound:
				err = grpcNotFoundErr(tt.errMsg)
			case http.StatusForbidden:
				err = grpcPermDeniedErr(tt.errMsg)
			case http.StatusBadRequest:
				err = grpcInvalidArgErr(tt.errMsg)
			}
			writeHTTPError(w, err)
			if w.Code != tt.wantCode {
				t.Errorf("status = %d, want %d", w.Code, tt.wantCode)
			}
			body := strings.TrimSpace(w.Body.String())
			if body != tt.wantBody {
				t.Errorf("body = %q, want %q", body, tt.wantBody)
			}
		})
	}
}

func TestWSAcceptOptions(t *testing.T) {
	// No patterns: insecure
	opts := wsAcceptOptions(nil)
	if !opts.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify with no patterns")
	}

	// With patterns: check origin
	opts = wsAcceptOptions([]string{"example.com"})
	if opts.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=false with patterns")
	}
	if len(opts.OriginPatterns) != 1 || opts.OriginPatterns[0] != "example.com" {
		t.Errorf("OriginPatterns = %v, want [example.com]", opts.OriginPatterns)
	}
}

func TestMustJSON(t *testing.T) {
	data := mustJSON(map[string]string{"key": "value"})
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["key"] != "value" {
		t.Errorf("key = %q, want %q", m["key"], "value")
	}
}

// setupInProcess creates an in-process event service and gateway handler for integration tests.
func setupInProcess(t *testing.T) (*Handler, eventpb.EventServiceServer, func()) {
	t.Helper()

	ch := channel.New()
	svc := service.NewService(ch,
		service.WithAuthorizer(service.AllowAll()),
		service.WithLogger(slog.Default()),
	)

	ctx := context.Background()
	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler: %v", err)
	}

	cleanup := func() {
		_ = handler.Close()
		svc.Stop()
		_ = ch.Close(context.Background())
	}

	return handler, svc, cleanup
}

func TestSSESubscribe(t *testing.T) {
	handler, svc, cleanup := setupInProcess(t)
	defer cleanup()

	ctx := context.Background()

	// Register event
	_, err := svc.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "sse-test"})
	if err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	// Start SSE request
	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/v1/events/sse-test/stream?start_from=latest", nil)
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Give subscription time to establish
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	_, err = svc.Publish(ctx, &eventpb.PublishRequest{
		Event:   "sse-test",
		Id:      "msg-sse-1",
		Payload: []byte("hello-sse"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Read SSE events (preamble + message)
	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	body := string(buf[:n])

	// Should contain the preamble
	if !strings.Contains(body, "retry: 5000") {
		t.Errorf("missing SSE preamble, got: %s", body)
	}
}

func TestWebSocketSubscribe(t *testing.T) {
	handler, svc, cleanup := setupInProcess(t)
	defer cleanup()

	ctx := context.Background()

	// Register event
	_, err := svc.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "ws-test"})
	if err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Connect WebSocket
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/events/ws-test/subscribe?start_from=latest"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	// Give subscription time to establish
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	_, err = svc.Publish(ctx, &eventpb.PublishRequest{
		Event:   "ws-test",
		Id:      "msg-ws-1",
		Payload: []byte("hello-ws"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Read the message from WebSocket
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, data, err := conn.Read(readCtx)
	if err != nil {
		t.Fatalf("WebSocket read: %v", err)
	}

	var msg wsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg.Type != "message" {
		t.Errorf("type = %q, want message", msg.Type)
	}
	if msg.ID != "msg-ws-1" {
		t.Errorf("id = %q, want msg-ws-1", msg.ID)
	}
	if string(msg.Payload) != "hello-ws" {
		t.Errorf("payload = %q, want hello-ws", string(msg.Payload))
	}
	if msg.AckID == "" {
		t.Error("expected non-empty ack_id")
	}

	// Send ack
	ackMsg := wsMessage{Type: "ack", AckID: msg.AckID}
	ackData, _ := json.Marshal(ackMsg)
	if err := conn.Write(ctx, websocket.MessageText, ackData); err != nil {
		t.Fatalf("WebSocket write ack: %v", err)
	}
}

func TestSSEMissingEventName(t *testing.T) {
	handler, _, cleanup := setupInProcess(t)
	defer cleanup()

	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Path with empty segment falls through to gRPC-Gateway (501).
	// A real missing event name would be caught by the handler itself.
	resp, err := http.Get(srv.URL + "/v1/events//stream")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	// Not routed to SSE handler, so gRPC-Gateway handles it
	if resp.StatusCode == http.StatusOK {
		t.Error("expected non-200 for empty event name path")
	}
}

func TestWSMissingEventName(t *testing.T) {
	handler, _, cleanup := setupInProcess(t)
	defer cleanup()

	srv := httptest.NewServer(handler)
	defer srv.Close()

	// The path has empty event name — handler should reject
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/events//subscribe"
	conn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err == nil {
		// If connect succeeded, server should have sent error and closed
		conn.Close(websocket.StatusNormalClosure, "done")
	}
}

func TestMethodNotAllowed(t *testing.T) {
	handler, _, cleanup := setupInProcess(t)
	defer cleanup()

	srv := httptest.NewServer(handler)
	defer srv.Close()

	// POST to subscribe endpoint should return 405
	resp, err := http.Post(srv.URL+"/v1/events/test/subscribe", "application/json", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}
