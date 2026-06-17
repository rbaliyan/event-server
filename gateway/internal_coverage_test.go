package gateway

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newRecorderSSEWriter() (*sseWriter, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	return &sseWriter{w: rec, flusher: rec}, rec
}

func TestSSEWriter_CommentAndError(t *testing.T) {
	t.Parallel()
	sw, rec := newRecorderSSEWriter()

	if err := sw.writeComment("connected"); err != nil {
		t.Fatalf("writeComment: %v", err)
	}
	// Client-actionable code: message is surfaced.
	writeSSEError(sw, status.Error(codes.PermissionDenied, "nope"))
	// Non-actionable code: message is masked as "internal error".
	writeSSEError(sw, status.Error(codes.Internal, "secret detail"))
	// Non-status error: also masked.
	writeSSEError(sw, context.Canceled)

	body := rec.Body.String()
	if !strings.Contains(body, ": connected") {
		t.Errorf("missing comment frame, got:\n%s", body)
	}
	if !strings.Contains(body, "nope") {
		t.Errorf("expected surfaced PermissionDenied message, got:\n%s", body)
	}
	if strings.Contains(body, "secret detail") {
		t.Errorf("internal error detail leaked, got:\n%s", body)
	}
	if !strings.Contains(body, "internal error") {
		t.Errorf("expected masked internal error, got:\n%s", body)
	}
}

func TestSSESubscribeStream_Shim(t *testing.T) {
	t.Parallel()
	sw, rec := newRecorderSSEWriter()
	s := &sseSubscribeStream{ctx: context.Background(), sw: sw}

	if s.Context() != context.Background() {
		t.Error("Context() should return the configured context")
	}
	// No-op shim methods must not error.
	if err := s.SetHeader(nil); err != nil {
		t.Errorf("SetHeader: %v", err)
	}
	if err := s.SendHeader(nil); err != nil {
		t.Errorf("SendHeader: %v", err)
	}
	s.SetTrailer(nil)
	if err := s.SendMsg(nil); err != nil {
		t.Errorf("SendMsg: %v", err)
	}
	if err := s.RecvMsg(nil); err != nil {
		t.Errorf("RecvMsg: %v", err)
	}

	if err := s.Send(&eventpb.Message{Id: "m1", Payload: []byte("hi")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !strings.Contains(rec.Body.String(), "m1") {
		t.Errorf("Send did not write the message frame, got:\n%s", rec.Body.String())
	}
}

func TestWSSubscribeStream_Shim(t *testing.T) {
	t.Parallel()
	ch := make(chan *eventpb.Message, 1)
	s := &wsSubscribeStream{ctx: context.Background(), msgCh: ch}

	if err := s.SetHeader(nil); err != nil {
		t.Errorf("SetHeader: %v", err)
	}
	if err := s.SendHeader(nil); err != nil {
		t.Errorf("SendHeader: %v", err)
	}
	s.SetTrailer(nil) // no-op shim
	if err := s.SendMsg(nil); err != nil {
		t.Errorf("SendMsg: %v", err)
	}
	if err := s.RecvMsg(nil); err != nil {
		t.Errorf("RecvMsg: %v", err)
	}

	if err := s.Send(&eventpb.Message{Id: "m1"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := <-ch; got.Id != "m1" {
		t.Errorf("Send delivered %q, want m1", got.Id)
	}

	// Send on a cancelled context returns the context error.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	full := make(chan *eventpb.Message) // unbuffered: send would block
	s2 := &wsSubscribeStream{ctx: cctx, msgCh: full}
	if err := s2.Send(&eventpb.Message{Id: "x"}); err == nil {
		t.Error("Send on cancelled context should return an error")
	}
}

func TestProtoToWSMessage_Timestamp(t *testing.T) {
	t.Parallel()
	ts := timestamppb.Now()
	m := protoToWSMessage(&eventpb.Message{
		Id: "m1", Source: "s", Payload: []byte("p"), AckId: "a1",
		RetryCount: 2, Timestamp: ts,
	})
	if m.Type != "message" || m.ID != "m1" || m.Source != "s" || m.AckID != "a1" || m.RetryCount != 2 {
		t.Fatalf("unexpected mapping: %+v", m)
	}
	if m.Timestamp == nil || !m.Timestamp.Equal(ts.AsTime()) {
		t.Errorf("timestamp not mapped: %v", m.Timestamp)
	}

	// Nil timestamp -> nil pointer.
	if got := protoToWSMessage(&eventpb.Message{Id: "m2"}); got.Timestamp != nil {
		t.Errorf("expected nil timestamp, got %v", got.Timestamp)
	}
}
