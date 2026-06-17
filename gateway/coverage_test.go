package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/rbaliyan/event-server/service"
	"github.com/rbaliyan/event/v3/transport/channel"
	"google.golang.org/grpc"
)

// setupRemote stands up a real gRPC server (channel transport + the given
// guard, with the auth interceptors wired) on a localhost listener, and a
// gateway Handler in REMOTE mode pointed at it. This exercises the full
// REST -> gRPC client -> server hop, including gRPC-status -> HTTP-status
// mapping, which the in-process handler bypasses.
func setupRemote(t *testing.T, guard service.SecurityGuard) (*Handler, eventpb.EventServiceServer, func()) {
	t.Helper()

	ch := channel.New()
	svc, err := service.NewService(ch, service.WithSecurityGuard(guard))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(svc.UnaryInterceptor()),
		grpc.StreamInterceptor(svc.StreamInterceptor()),
	)
	eventpb.RegisterEventServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	handler, err := NewHandler(context.Background(), lis.Addr().String(), WithInsecure())
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	cleanup := func() {
		_ = handler.Close()
		srv.Stop()
		svc.Stop()
		_ = ch.Close(context.Background())
	}
	return handler, svc, cleanup
}

// TestRemoteHandler_RESTErrorMapping verifies gRPC status codes from the server
// are mapped to the expected HTTP status codes through a real remote hop.
func TestRemoteHandler_RESTErrorMapping(t *testing.T) {
	t.Run("OK health -> 200", func(t *testing.T) {
		handler, _, cleanup := setupRemote(t, service.AllowAll())
		defer cleanup()
		srv := httptest.NewServer(handler)
		defer srv.Close()

		resp, err := http.Get(srv.URL + "/v1/health")
		if err != nil {
			t.Fatalf("GET health: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("health status = %d, want 200", resp.StatusCode)
		}
	})

	t.Run("NotFound publish unregistered -> 404", func(t *testing.T) {
		handler, _, cleanup := setupRemote(t, service.AllowAll())
		defer cleanup()
		srv := httptest.NewServer(handler)
		defer srv.Close()

		// Publish to an event that was never registered.
		resp, err := http.Post(srv.URL+"/v1/events/never-registered/messages",
			"application/json", strings.NewReader(`{"payload":"aGk="}`))
		if err != nil {
			t.Fatalf("POST publish: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("publish-unregistered status = %d, want 404; body=%s", resp.StatusCode, body)
		}
	})

	t.Run("PermissionDenied under DenyAll -> 403", func(t *testing.T) {
		handler, _, cleanup := setupRemote(t, service.DenyAll())
		defer cleanup()
		srv := httptest.NewServer(handler)
		defer srv.Close()

		// RegisterEvent is guarded; DenyAll -> PermissionDenied -> 403.
		resp, err := http.Post(srv.URL+"/v1/events/some-event", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("POST register: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("register-under-denyall status = %d, want 403; body=%s", resp.StatusCode, body)
		}
	})
}

// sseTap reads an SSE response body in the background into a thread-safe buffer
// so tests can assert on its contents without blocking on Read.
type sseTap struct {
	mu  sync.Mutex
	buf strings.Builder
}

func tapSSE(body io.Reader) *sseTap {
	tp := &sseTap{}
	go func() {
		b := make([]byte, 4096)
		for {
			n, err := body.Read(b)
			if n > 0 {
				tp.mu.Lock()
				tp.buf.Write(b[:n])
				tp.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	return tp
}

func (t *sseTap) contains(s string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Contains(t.buf.String(), s)
}

func (t *sseTap) snapshot() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buf.String()
}

// publishUntil publishes probe messages until cond is satisfied or the deadline
// elapses. Deterministic replacement for time.Sleep-based readiness.
func publishUntil(t *testing.T, svc eventpb.EventServiceServer, event string, cond func() bool, deadline time.Duration) {
	t.Helper()
	end := time.After(deadline)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		_, _ = svc.Publish(context.Background(), &eventpb.PublishRequest{
			Event: event, Id: "__probe__", Payload: []byte("probe"),
		})
		select {
		case <-tick.C:
		case <-end:
			if cond() {
				return
			}
			t.Fatal("condition not met before deadline")
		}
	}
}

// TestSSE_DeliversMessage asserts the SSE stream delivers an actual message
// data frame (not just the preamble), deterministically.
func TestSSE_DeliversMessage(t *testing.T) {
	handler, svc, cleanup := setupInProcess(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := svc.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "sse-deliver"}); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/v1/events/sse-deliver/stream?start_from=latest", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	tap := tapSSE(resp.Body)
	// Readiness: publish probes until the stream is live (a probe frame shows up).
	publishUntil(t, svc, "sse-deliver", func() bool { return tap.contains("probe") }, 5*time.Second)

	// Publish the real message and wait for its frame.
	if _, err := svc.Publish(ctx, &eventpb.PublishRequest{
		Event: "sse-deliver", Id: "msg-real", Payload: []byte("hello-sse"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The frame carries the id verbatim and the payload base64-encoded
	// ("hello-sse" -> "aGVsbG8tc3Nl"); assert on both forms.
	deadline := time.After(5 * time.Second)
	for !tap.contains("msg-real") {
		select {
		case <-deadline:
			t.Fatalf("SSE message frame not delivered; got:\n%s", tap.snapshot())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !tap.contains("event: message") {
		t.Errorf("expected an 'event: message' frame, got:\n%s", tap.snapshot())
	}
	if !tap.contains("aGVsbG8tc3Nl") {
		t.Errorf("expected base64 payload of 'hello-sse', got:\n%s", tap.snapshot())
	}
}

// TestRemoteHandler_SSEDeliversMessage exercises the remote (gRPC-backed) SSE
// handler end to end.
func TestRemoteHandler_SSEDeliversMessage(t *testing.T) {
	handler, svc, cleanup := setupRemote(t, service.AllowAll())
	defer cleanup()
	ctx := context.Background()

	if _, err := svc.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "rsse"}); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/v1/events/rsse/stream?start_from=latest", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("SSE request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	tap := tapSSE(resp.Body)
	publishUntil(t, svc, "rsse", func() bool { return tap.contains("probe") }, 5*time.Second)

	if _, err := svc.Publish(ctx, &eventpb.PublishRequest{Event: "rsse", Id: "rmsg", Payload: []byte("rp")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for !tap.contains("rmsg") {
		select {
		case <-deadline:
			t.Fatalf("remote SSE frame not delivered; got:\n%s", tap.snapshot())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestRemoteHandler_WSDeliversMessage exercises the remote (gRPC-backed)
// WebSocket handler end to end, including the ack path.
func TestRemoteHandler_WSDeliversMessage(t *testing.T) {
	handler, svc, cleanup := setupRemote(t, service.AllowAll())
	defer cleanup()
	ctx := context.Background()

	if _, err := svc.RegisterEvent(ctx, &eventpb.RegisterEventRequest{Name: "rws"}); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	srv := httptest.NewServer(handler)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/events/rws/subscribe?start_from=latest"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	frames := make(chan wsMessage, 64)
	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var m wsMessage
			if json.Unmarshal(data, &m) == nil {
				frames <- m
			}
		}
	}()

	readyDeadline := time.After(5 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
ready:
	for {
		_, _ = svc.Publish(ctx, &eventpb.PublishRequest{Event: "rws", Id: "__probe__", Payload: []byte("probe")})
		select {
		case <-frames:
			break ready
		case <-tick.C:
		case <-readyDeadline:
			t.Fatal("remote WS subscription did not become ready within 5s")
		}
	}

	if _, err := svc.Publish(ctx, &eventpb.PublishRequest{Event: "rws", Id: "rws-1", Payload: []byte("hi")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var msg wsMessage
	deadline := time.After(3 * time.Second)
	for msg.ID != "rws-1" {
		select {
		case m := <-frames:
			if m.Type == "message" && m.ID == "rws-1" {
				msg = m
			}
		case <-deadline:
			t.Fatal("did not receive rws-1 within 3s")
		}
	}
	if msg.AckID == "" {
		t.Error("expected non-empty ack_id from remote WS handler")
	}
	ackData, _ := json.Marshal(wsMessage{Type: "ack", AckID: msg.AckID})
	if err := conn.Write(ctx, websocket.MessageText, ackData); err != nil {
		t.Fatalf("WS write ack: %v", err)
	}
}

// TestOptions_Applied constructs handlers with each option so the option
// closures (and dial-option assembly) are exercised.
func TestOptions_Applied(t *testing.T) {
	ch := channel.New()
	svc, err := service.NewService(ch, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	defer func() { svc.Stop(); _ = ch.Close(context.Background()) }()

	ip, err := NewInProcessHandler(context.Background(), svc,
		WithHeartbeatInterval(time.Second),
		WithWSOriginPatterns("example.com", "*.example.com"),
	)
	if err != nil {
		t.Fatalf("NewInProcessHandler with options: %v", err)
	}
	_ = ip.Close()

	// Remote construction is lazy (grpc.NewClient does not dial here), so this
	// exercises WithDialOptions/WithTLS/WithMuxOptions without a live server.
	rh, err := NewHandler(context.Background(), "127.0.0.1:1",
		WithTLS(nil),
		WithMuxOptions(),
		WithDialOptions(grpc.WithUserAgent("eventctl-test")),
		WithHeartbeatInterval(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewHandler with options: %v", err)
	}
	_ = rh.Close()
}
