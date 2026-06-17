package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"github.com/coder/websocket"
)

// wsStubService is a minimal EventServiceServer for driving the WebSocket
// handler's error and heartbeat branches deterministically.
type wsStubService struct {
	eventpb.UnimplementedEventServiceServer
	subErr error
	block  bool
}

func (s *wsStubService) Subscribe(_ *eventpb.SubscribeRequest, stream eventpb.EventService_SubscribeServer) error {
	if s.block {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return s.subErr
}

func wsServer(t *testing.T, svc eventpb.EventServiceServer, heartbeat time.Duration) (*httptest.Server, string) {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/v1/events/{name}/subscribe", newInProcessWSHandler(svc, heartbeat, nil))
	srv := httptest.NewServer(mux)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/events/evt/subscribe"
}

func readFrames(t *testing.T, conn *websocket.Conn, ctx context.Context) <-chan wsMessage {
	t.Helper()
	out := make(chan wsMessage, 16)
	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				close(out)
				return
			}
			var m wsMessage
			if json.Unmarshal(data, &m) == nil {
				out <- m
			}
		}
	}()
	return out
}

// TestInProcessWS_SubscribeErrorFrame covers writeWSError: when svc.Subscribe
// fails after the connection is upgraded, the handler writes an error frame.
func TestInProcessWS_SubscribeErrorFrame(t *testing.T) {
	srv, wsURL := wsServer(t, &wsStubService{subErr: errors.New("boom")}, time.Hour)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	frames := readFrames(t, conn, ctx)
	select {
	case m, ok := <-frames:
		if !ok {
			t.Fatal("connection closed before error frame")
		}
		if m.Type != "error" {
			t.Fatalf("frame type = %q, want error", m.Type)
		}
		if !strings.Contains(m.Error, "subscribe error") {
			t.Fatalf("error = %q, want it to contain 'subscribe error'", m.Error)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for error frame")
	}
}

// TestInProcessWS_Heartbeat covers the runWSHeartbeat ticker branch: with a
// blocking subscribe and a tiny interval, heartbeat frames are emitted.
func TestInProcessWS_Heartbeat(t *testing.T) {
	srv, wsURL := wsServer(t, &wsStubService{block: true}, 5*time.Millisecond)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	frames := readFrames(t, conn, ctx)
	for {
		select {
		case m, ok := <-frames:
			if !ok {
				t.Fatal("connection closed before heartbeat")
			}
			if m.Type == "heartbeat" {
				return
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for heartbeat frame")
		}
	}
}
