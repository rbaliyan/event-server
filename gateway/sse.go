package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// sseWriter serializes writes to an http.ResponseWriter to prevent
// concurrent writes from the heartbeat goroutine and event goroutine.
type sseWriter struct {
	mu      sync.Mutex
	w       http.ResponseWriter
	flusher http.Flusher
}

// writeEvent writes an SSE event frame.
func (sw *sseWriter) writeEvent(eventType string, data []byte) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	eventType = sanitizeSSEField(eventType)
	if _, err := fmt.Fprintf(sw.w, "event: %s\n", eventType); err != nil {
		return err
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if _, err := fmt.Fprintf(sw.w, "data: %s\n", line); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprint(sw.w, "\n"); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

func (sw *sseWriter) writeComment(comment string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	comment = sanitizeSSEField(comment)
	if _, err := fmt.Fprintf(sw.w, ": %s\n\n", comment); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

func (sw *sseWriter) writeRaw(s string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if _, err := fmt.Fprint(sw.w, s); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

// sseSubscribeStream implements the gRPC server-streaming interface
// by writing SSE events to an http.ResponseWriter.
type sseSubscribeStream struct {
	grpc.ServerStream
	ctx context.Context
	sw  *sseWriter
}

func (s *sseSubscribeStream) Context() context.Context { return s.ctx }
func (s *sseSubscribeStream) Send(msg *eventpb.Message) error {
	return sendSSEMessage(s.sw, msg)
}
func (s *sseSubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (s *sseSubscribeStream) SendHeader(metadata.MD) error { return nil }
func (s *sseSubscribeStream) SetTrailer(metadata.MD)       {}
func (s *sseSubscribeStream) SendMsg(any) error            { return nil }
func (s *sseSubscribeStream) RecvMsg(any) error            { return nil }

// sendSSEMessage converts a proto Message to an SSE event and writes it.
func sendSSEMessage(sw *sseWriter, msg *eventpb.Message) error {
	wsMsg := protoToWSMessage(msg)
	data, err := json.Marshal(wsMsg)
	if err != nil {
		return fmt.Errorf("marshal SSE event: %w", err)
	}
	return sw.writeEvent("message", data)
}

// writeSSEHeaders sets the standard SSE response headers.
func writeSSEHeaders(w http.ResponseWriter, r *http.Request, flusher http.Flusher) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	if r.ProtoMajor == 1 {
		w.Header().Set("Connection", "keep-alive")
	}
	flusher.Flush()
}

// writeStreamPreamble sends the initial SSE stream preamble.
func writeStreamPreamble(sw *sseWriter) error {
	return sw.writeRaw("retry: 5000\n\n: connected\n\n")
}

// newInProcessSSEHandler creates an HTTP handler that calls svc.Subscribe
// directly and streams results as SSE events.
func newInProcessSSEHandler(svc eventpb.EventServiceServer, heartbeat time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		eventName := r.PathValue("name")
		if eventName == "" {
			http.Error(w, "missing event name", http.StatusBadRequest)
			return
		}

		req := parseSubscribeQuery(eventName, r)

		writeSSEHeaders(w, r, flusher)

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		ctx = httpHeadersToMetadata(ctx, r)

		sw := &sseWriter{w: w, flusher: flusher}
		if err := writeStreamPreamble(sw); err != nil {
			return
		}

		stream := &sseSubscribeStream{ctx: ctx, sw: sw}

		hbDone := make(chan struct{})
		go func() {
			defer close(hbDone)
			runHeartbeat(ctx, sw, heartbeat)
		}()

		err := svc.Subscribe(req, stream)
		if err != nil && ctx.Err() == nil {
			writeSSEError(sw, err)
		}

		cancel()
		<-hbDone
	})
}

// newRemoteSSEHandler creates an HTTP handler that subscribes via a gRPC
// client and relays responses as SSE events.
func newRemoteSSEHandler(client eventpb.EventServiceClient, heartbeat time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		eventName := r.PathValue("name")
		if eventName == "" {
			http.Error(w, "missing event name", http.StatusBadRequest)
			return
		}

		req := parseSubscribeQuery(eventName, r)

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		ctx = httpHeadersToMetadata(ctx, r)

		stream, err := client.Subscribe(ctx, req)
		if err != nil {
			writeHTTPError(w, err)
			return
		}

		writeSSEHeaders(w, r, flusher)

		sw := &sseWriter{w: w, flusher: flusher}
		if err := writeStreamPreamble(sw); err != nil {
			return
		}

		hbDone := make(chan struct{})
		go func() {
			defer close(hbDone)
			runHeartbeat(ctx, sw, heartbeat)
		}()

		for {
			msg, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					writeSSEError(sw, err)
				}
				break
			}

			if err := sendSSEMessage(sw, msg); err != nil {
				break
			}
		}

		cancel()
		<-hbDone
	})
}

// httpHeadersToMetadata converts selected HTTP request headers into gRPC
// incoming metadata on the context.
func httpHeadersToMetadata(ctx context.Context, r *http.Request) context.Context {
	md := make(metadata.MD)
	for key, values := range r.Header {
		lower := strings.ToLower(key)
		if isForwardableHeader(lower) {
			md[lower] = values
		}
	}
	if len(md) == 0 {
		return ctx
	}
	return metadata.NewIncomingContext(ctx, md)
}

func isForwardableHeader(lower string) bool {
	switch lower {
	case "authorization":
		return true
	}
	if strings.HasPrefix(lower, "x-") {
		switch lower {
		case "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto",
			"x-forwarded-port", "x-real-ip":
			return false
		}
		return true
	}
	return false
}

// runHeartbeat sends SSE comment lines at the given interval.
func runHeartbeat(ctx context.Context, sw *sseWriter, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sw.writeComment("heartbeat"); err != nil {
				return
			}
		}
	}
}

// writeSSEError writes an SSE error event.
func writeSSEError(sw *sseWriter, err error) {
	msg := "internal error"
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.PermissionDenied, codes.Unauthenticated,
			codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
			codes.FailedPrecondition, codes.Unimplemented:
			msg = st.Message()
		}
	}
	_ = sw.writeEvent("error", mustJSON(map[string]string{"error": msg}))
}

// writeHTTPError maps a gRPC error to an HTTP status code.
func writeHTTPError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	var httpCode int
	clientActionable := true
	switch st.Code() {
	case codes.PermissionDenied:
		httpCode = http.StatusForbidden
	case codes.Unauthenticated:
		httpCode = http.StatusUnauthorized
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.NotFound:
		httpCode = http.StatusNotFound
	case codes.AlreadyExists:
		httpCode = http.StatusConflict
	case codes.Unimplemented:
		httpCode = http.StatusNotImplemented
	default:
		httpCode = http.StatusInternalServerError
		clientActionable = false
	}

	msg := "internal error"
	if clientActionable {
		msg = st.Message()
	}
	http.Error(w, msg, httpCode)
}

// mustJSON marshals v to JSON, returning a fallback error object on failure.
func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"error":"internal error"}`)
	}
	return data
}

// sanitizeSSEField strips carriage returns and newlines from s.
func sanitizeSSEField(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	r := strings.NewReplacer("\r\n", "", "\r", "", "\n", "")
	return r.Replace(s)
}
