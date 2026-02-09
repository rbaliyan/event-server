package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	eventpb "github.com/rbaliyan/event-server/proto/event/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"github.com/coder/websocket"
)

// wsMessage is the JSON protocol for WebSocket communication.
type wsMessage struct {
	Type       string            `json:"type"`
	ID         string            `json:"id,omitempty"`
	Source     string            `json:"source,omitempty"`
	Payload    []byte            `json:"payload,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Timestamp  *time.Time        `json:"timestamp,omitempty"`
	RetryCount int32             `json:"retry_count,omitempty"`
	AckID      string            `json:"ack_id,omitempty"`
	Error      string            `json:"error,omitempty"`
}

// newRemoteWSHandler creates a WebSocket subscribe handler that connects
// to the event service via a gRPC client.
func newRemoteWSHandler(client eventpb.EventServiceClient, heartbeat time.Duration, originPatterns []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		eventName := r.PathValue("name")
		if eventName == "" {
			http.Error(w, "missing event name", http.StatusBadRequest)
			return
		}

		conn, err := websocket.Accept(w, r, wsAcceptOptions(originPatterns))
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "closing")

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Forward HTTP headers as gRPC metadata
		ctx = httpHeadersToMetadata(ctx, r)

		req := parseSubscribeQuery(eventName, r)
		stream, err := client.Subscribe(ctx, req)
		if err != nil {
			writeWSError(ctx, conn, fmt.Sprintf("subscribe failed: %v", err))
			return
		}

		// Read gRPC stream in a dedicated goroutine
		msgCh := make(chan *eventpb.Message, 100)
		errCh := make(chan error, 1)
		go func() {
			defer close(msgCh)
			for {
				msg, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}
				select {
				case msgCh <- msg:
				case <-ctx.Done():
					return
				}
			}
		}()

		// Start ack reader goroutine
		ackCh := make(chan wsMessage, 16)
		go readWSAcks(ctx, conn, ackCh)

		// Start heartbeat
		hbDone := make(chan struct{})
		go func() {
			defer close(hbDone)
			runWSHeartbeat(ctx, conn, heartbeat)
		}()

		// Main loop: relay messages from gRPC stream to WebSocket,
		// process acks from client
		for {
			select {
			case <-ctx.Done():
				cancel()
				<-hbDone
				return

			case err := <-errCh:
				if err != io.EOF {
					writeWSError(ctx, conn, fmt.Sprintf("stream error: %v", err))
				}
				cancel()
				<-hbDone
				return

			case msg, ok := <-msgCh:
				if !ok {
					cancel()
					<-hbDone
					return
				}
				wsMsg := protoToWSMessage(msg)
				data, err := json.Marshal(wsMsg)
				if err != nil {
					writeWSError(ctx, conn, "failed to encode message")
					continue
				}
				if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
					cancel()
					<-hbDone
					return
				}

			case ack, ok := <-ackCh:
				if !ok {
					cancel()
					<-hbDone
					return
				}
				if ack.Type == "ack" {
					_, _ = client.Ack(ctx, &eventpb.AckRequest{
						Entries: []*eventpb.AckEntry{
							{AckId: ack.AckID, Error: ack.Error},
						},
					})
				}
			}
		}
	})
}

// newInProcessWSHandler creates a WebSocket subscribe handler that calls
// the service directly.
func newInProcessWSHandler(svc eventpb.EventServiceServer, heartbeat time.Duration, originPatterns []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		eventName := r.PathValue("name")
		if eventName == "" {
			http.Error(w, "missing event name", http.StatusBadRequest)
			return
		}

		conn, err := websocket.Accept(w, r, wsAcceptOptions(originPatterns))
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "closing")

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		ctx = httpHeadersToMetadata(ctx, r)
		req := parseSubscribeQuery(eventName, r)

		// Use an adapter to collect messages from svc.Subscribe
		msgCh := make(chan *eventpb.Message, 100)
		errCh := make(chan error, 1)
		adapter := &wsSubscribeStream{
			ctx:   ctx,
			msgCh: msgCh,
		}

		go func() {
			errCh <- svc.Subscribe(req, adapter)
			close(msgCh)
		}()

		// Start ack reader
		ackCh := make(chan wsMessage, 16)
		go readWSAcks(ctx, conn, ackCh)

		// Start heartbeat
		hbDone := make(chan struct{})
		go func() {
			defer close(hbDone)
			runWSHeartbeat(ctx, conn, heartbeat)
		}()

		// Relay messages to WebSocket
		for {
			select {
			case <-ctx.Done():
				cancel()
				<-hbDone
				return

			case err := <-errCh:
				if err != nil && ctx.Err() == nil {
					writeWSError(ctx, conn, fmt.Sprintf("subscribe error: %v", err))
				}
				cancel()
				<-hbDone
				return

			case msg, ok := <-msgCh:
				if !ok {
					cancel()
					<-hbDone
					return
				}
				wsMsg := protoToWSMessage(msg)
				data, err := json.Marshal(wsMsg)
				if err != nil {
					writeWSError(ctx, conn, "failed to encode message")
					continue
				}
				if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
					cancel()
					<-hbDone
					return
				}

			case ack, ok := <-ackCh:
				if !ok {
					cancel()
					<-hbDone
					return
				}
				if ack.Type == "ack" {
					_, _ = svc.Ack(ctx, &eventpb.AckRequest{
						Entries: []*eventpb.AckEntry{
							{AckId: ack.AckID, Error: ack.Error},
						},
					})
				}
			}
		}
	})
}

// wsSubscribeStream adapts the gRPC server-streaming interface to a channel.
type wsSubscribeStream struct {
	grpc.ServerStream
	ctx   context.Context
	msgCh chan<- *eventpb.Message
}

func (s *wsSubscribeStream) Context() context.Context { return s.ctx }
func (s *wsSubscribeStream) Send(msg *eventpb.Message) error {
	select {
	case s.msgCh <- msg:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *wsSubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (s *wsSubscribeStream) SendHeader(metadata.MD) error { return nil }
func (s *wsSubscribeStream) SetTrailer(metadata.MD)       {}
func (s *wsSubscribeStream) SendMsg(any) error            { return nil }
func (s *wsSubscribeStream) RecvMsg(any) error            { return nil }

// readWSAcks reads ack messages from the WebSocket connection.
func readWSAcks(ctx context.Context, conn *websocket.Conn, ch chan<- wsMessage) {
	defer close(ch)
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var msg wsMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		select {
		case ch <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// runWSHeartbeat sends heartbeat messages at the given interval.
func runWSHeartbeat(ctx context.Context, conn *websocket.Conn, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msg := wsMessage{Type: "heartbeat"}
			data, _ := json.Marshal(msg)
			if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
				return
			}
		}
	}
}

// writeWSError sends an error message over WebSocket.
func writeWSError(ctx context.Context, conn *websocket.Conn, errMsg string) {
	msg := wsMessage{Type: "error", Error: errMsg}
	data, _ := json.Marshal(msg)
	_ = conn.Write(ctx, websocket.MessageText, data)
}

// protoToWSMessage converts a proto Message to a WebSocket message.
func protoToWSMessage(msg *eventpb.Message) wsMessage {
	m := wsMessage{
		Type:       "message",
		ID:         msg.Id,
		Source:     msg.Source,
		Payload:    msg.Payload,
		Metadata:   msg.Metadata,
		RetryCount: msg.RetryCount,
		AckID:      msg.AckId,
	}
	if msg.Timestamp != nil {
		t := msg.Timestamp.AsTime()
		m.Timestamp = &t
	}
	return m
}

// parseSubscribeQuery builds a SubscribeRequest from query parameters.
func parseSubscribeQuery(eventName string, r *http.Request) *eventpb.SubscribeRequest {
	q := r.URL.Query()
	req := &eventpb.SubscribeRequest{
		Event: eventName,
	}

	switch q.Get("delivery_mode") {
	case "worker_pool":
		req.DeliveryMode = eventpb.DeliveryMode_DELIVERY_MODE_WORKER_POOL
		req.WorkerGroup = q.Get("worker_group")
	case "broadcast":
		req.DeliveryMode = eventpb.DeliveryMode_DELIVERY_MODE_BROADCAST
	}

	switch q.Get("start_from") {
	case "beginning":
		req.StartFrom = eventpb.StartPosition_START_POSITION_BEGINNING
	case "latest":
		req.StartFrom = eventpb.StartPosition_START_POSITION_LATEST
	}

	if q.Get("latest_only") == "true" {
		req.LatestOnly = true
	}

	if cid := q.Get("consumer_id"); cid != "" {
		req.ConsumerId = cid
	}

	return req
}

// wsAcceptOptions builds WebSocket accept options with configurable origin checking.
func wsAcceptOptions(originPatterns []string) *websocket.AcceptOptions {
	if len(originPatterns) > 0 {
		return &websocket.AcceptOptions{OriginPatterns: originPatterns}
	}
	return &websocket.AcceptOptions{InsecureSkipVerify: true}
}
