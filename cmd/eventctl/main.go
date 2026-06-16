// Command eventctl is a CLI for interacting with an event-server,
// event-scheduler, and schema registry over their HTTP/REST gateways.
//
// Usage:
//
//	eventctl [--server http://host:port] [--scheduler http://host:port] [--schema http://host:port] <command>
//
// Commands:
//
//	events list                     List registered events
//	events pub <event> [payload]    Publish a message (use "-" to read from stdin)
//	events sub <event>              Subscribe and stream messages to stdout
//	events health                   Check event-server health
//
//	scheduler list                  List scheduled messages
//	  [-event <name>] [-limit <n>] [-before <RFC3339>] [-after <RFC3339>]
//	scheduler get <id>              Get a scheduled message by ID
//	scheduler health                Check scheduler health
//
//	schema list                     List all event schemas
//	schema get <event>              Get a schema by event name
//	schema set <event> [flags]      Create or update a schema
//	schema delete <event>           Delete a schema
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const helpText = `Usage: eventctl [flags] <command> [args]

Global flags:
  -server string      event-server HTTP address (default "http://localhost:8080")
  -scheduler string   scheduler HTTP address (defaults to -server value)
  -schema string      schema registry HTTP address (defaults to -server value)

Commands:
  events list                     List registered events
  events pub <event> [payload]    Publish a message ("-" reads payload from stdin)
  events sub <event>              Subscribe and stream messages to stdout (SSE)
    [-start latest|beginning]
  events health                   Check event-server health

  scheduler list                  List scheduled messages
    [-event <name>]
    [-limit <n>]
    [-before <RFC3339>]
    [-after <RFC3339>]
  scheduler get <id>              Get a scheduled message by ID
  scheduler health                Check scheduler health

  schema list                     List all event schemas
  schema get <event>              Get a schema by event name
  schema set <event>              Create or update a schema
    [-description string]
    [-timeout duration]   (e.g. 30s)
    [-retries int]
    [-backoff duration]   (e.g. 1s)
    [-monitor]            enable monitor middleware
    [-idempotency]        enable idempotency middleware
    [-poison]             enable poison detection middleware
  schema delete <event>           Delete a schema
`

func main() {
	fs := flag.NewFlagSet("eventctl", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, helpText) }

	serverAddr := fs.String("server", "http://localhost:8080", "event-server HTTP address")
	schedulerAddr := fs.String("scheduler", "", "scheduler HTTP address (defaults to -server)")
	schemaAddr := fs.String("schema", "", "schema registry HTTP address (defaults to -server)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	args := fs.Args()
	if len(args) < 1 {
		fs.Usage()
		os.Exit(1)
	}

	c := newCLI(*serverAddr, *schedulerAddr, *schemaAddr)

	if err := c.run(args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type cli struct {
	server    string
	scheduler string
	schema    string
}

// newCLI builds a cli from the raw address flags, defaulting the scheduler and
// schema addresses to the server address when empty and trimming trailing
// slashes.
func newCLI(serverAddr, schedulerAddr, schemaAddr string) *cli {
	sched := schedulerAddr
	if sched == "" {
		sched = serverAddr
	}
	schm := schemaAddr
	if schm == "" {
		schm = serverAddr
	}
	return &cli{
		server:    strings.TrimRight(serverAddr, "/"),
		scheduler: strings.TrimRight(sched, "/"),
		schema:    strings.TrimRight(schm, "/"),
	}
}

func (c *cli) run(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("%s requires a subcommand (list, pub, sub, health for events; list, get, health for scheduler)", args[0])
	}
	switch args[0] {
	case "events":
		return c.eventsCmd(args[1:])
	case "scheduler":
		return c.schedulerCmd(args[1:])
	case "schema":
		return c.schemaCmd(args[1:])
	default:
		return fmt.Errorf("unknown command %q; use 'events', 'scheduler', or 'schema'", args[0])
	}
}

// ── events ────────────────────────────────────────────────────────────────────

func (c *cli) eventsCmd(args []string) error {
	switch args[0] {
	case "list":
		return c.eventsList()
	case "pub":
		if len(args) < 2 {
			return fmt.Errorf("usage: events pub <event> [payload]")
		}
		payload := ""
		if len(args) >= 3 {
			payload = args[2]
		}
		return c.eventsPub(args[1], payload, nil)
	case "sub":
		if len(args) < 2 {
			return fmt.Errorf("usage: events sub <event>")
		}
		fs := flag.NewFlagSet("events sub", flag.ContinueOnError)
		start := fs.String("start", "latest", "start position: latest or beginning")
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}
		return c.eventsSub(args[1], *start)
	case "health":
		return c.eventsHealth()
	default:
		return fmt.Errorf("unknown events subcommand %q", args[0])
	}
}

func (c *cli) eventsList() error {
	resp, err := httpGet(c.server + "/v1/events")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp); err != nil {
		return err
	}

	var result struct {
		Events []string `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if len(result.Events) == 0 {
		fmt.Fprintln(os.Stderr, "(no events registered)")
		return nil
	}
	for _, ev := range result.Events {
		fmt.Println(ev)
	}
	return nil
}

func (c *cli) eventsPub(event, payload string, metadata map[string]string) error {
	if payload == "-" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		payload = string(data)
	}

	body := map[string]any{
		"payload": []byte(payload),
	}
	if len(metadata) > 0 {
		body["metadata"] = metadata
	}

	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := http.Post(
		c.server+"/v1/events/"+url.PathEscape(event)+"/messages",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp); err != nil {
		return err
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	fmt.Println("published:", result.ID)
	return nil
}

func (c *cli) eventsSub(event, start string) error {
	u := c.server + "/v1/events/" + url.PathEscape(event) + "/stream"
	if start != "" {
		u += "?start_from=" + url.QueryEscape(start)
	}

	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "subscribed to %s (ctrl+c to stop)\n", event)

	return readSSE(resp.Body, func(data string) {
		printMessage(data)
	})
}

func (c *cli) eventsHealth() error {
	resp, err := httpGet(c.server + "/v1/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp); err != nil {
		return err
	}
	return printJSON(resp.Body)
}

// ── scheduler ─────────────────────────────────────────────────────────────────

func (c *cli) schedulerCmd(args []string) error {
	switch args[0] {
	case "list":
		return c.schedulerList(args[1:])
	case "get":
		if len(args) < 2 {
			return fmt.Errorf("usage: scheduler get <id>")
		}
		return c.schedulerGet(args[1])
	case "health":
		return c.schedulerHealth()
	default:
		return fmt.Errorf("unknown scheduler subcommand %q", args[0])
	}
}

func (c *cli) schedulerList(args []string) error {
	fs := flag.NewFlagSet("scheduler list", flag.ContinueOnError)
	eventName := fs.String("event", "", "filter by event name")
	limit := fs.Int("limit", 0, "max results (0 = server default)")
	before := fs.String("before", "", "filter messages before this time (RFC3339)")
	after := fs.String("after", "", "filter messages after this time (RFC3339)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	params := url.Values{}
	if *eventName != "" {
		params.Set("event_name", *eventName)
	}
	if *limit > 0 {
		params.Set("limit", fmt.Sprint(*limit))
	}
	if *before != "" {
		params.Set("before", *before)
	}
	if *after != "" {
		params.Set("after", *after)
	}

	u := c.scheduler + "/v1/messages"
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	resp, err := httpGet(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp); err != nil {
		return err
	}
	return printJSON(resp.Body)
}

func (c *cli) schedulerGet(id string) error {
	resp, err := httpGet(c.scheduler + "/v1/messages/" + url.PathEscape(id))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp); err != nil {
		return err
	}
	return printJSON(resp.Body)
}

func (c *cli) schedulerHealth() error {
	resp, err := httpGet(c.scheduler + "/v1/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp); err != nil {
		return err
	}
	return printJSON(resp.Body)
}

// ── schema ────────────────────────────────────────────────────────────────────

func (c *cli) schemaCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("schema requires a subcommand: list, get, set, delete")
	}
	switch args[0] {
	case "list":
		return c.schemaList()
	case "get":
		if len(args) < 2 {
			return fmt.Errorf("usage: schema get <event>")
		}
		return c.schemaGet(args[1])
	case "set":
		if len(args) < 2 {
			return fmt.Errorf("usage: schema set <event> [flags]")
		}
		return c.schemaSet(args[1], args[2:])
	case "delete":
		if len(args) < 2 {
			return fmt.Errorf("usage: schema delete <event>")
		}
		return c.schemaDelete(args[1])
	default:
		return fmt.Errorf("unknown schema subcommand %q", args[0])
	}
}

func (c *cli) schemaList() error {
	resp, err := httpGet(c.schema + "/v1/schemas")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}
	return printJSON(resp.Body)
}

func (c *cli) schemaGet(name string) error {
	resp, err := httpGet(c.schema + "/v1/schemas/" + url.PathEscape(name))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}
	return printJSON(resp.Body)
}

func (c *cli) schemaSet(name string, args []string) error {
	fs := flag.NewFlagSet("schema set", flag.ContinueOnError)
	description := fs.String("description", "", "human-readable description")
	timeout := fs.Duration("timeout", 0, "subscriber timeout (e.g. 30s)")
	retries := fs.Int("retries", 0, "max delivery retries (0 = unlimited)")
	backoff := fs.Duration("backoff", 0, "retry backoff duration (e.g. 1s)")
	monitor := fs.Bool("monitor", false, "enable monitor middleware")
	idempotency := fs.Bool("idempotency", false, "enable idempotency middleware")
	poison := fs.Bool("poison", false, "enable poison detection middleware")
	if err := fs.Parse(args); err != nil {
		return err
	}

	body := map[string]any{
		"description":        *description,
		"sub_timeout":        int64(*timeout),
		"max_retries":        *retries,
		"retry_backoff":      int64(*backoff),
		"enable_monitor":     *monitor,
		"enable_idempotency": *idempotency,
		"enable_poison":      *poison,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut,
		c.schema+"/v1/schemas/"+url.PathEscape(name),
		bytes.NewReader(data),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}
	return printJSON(resp.Body)
}

func (c *cli) schemaDelete(name string) error {
	req, err := http.NewRequest(http.MethodDelete,
		c.schema+"/v1/schemas/"+url.PathEscape(name), nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp); err != nil {
		return err
	}
	fmt.Println("deleted:", name)
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func httpGet(u string) (*http.Response, error) {
	return http.Get(u) // #nosec G107 -- CLI tool; URL is caller-supplied via --server flag
}

// checkStatus returns an error for non-2xx responses, including the body.
func checkStatus(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	// Try to extract gRPC-Gateway error message
	var grpcErr struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &grpcErr) == nil && grpcErr.Message != "" {
		return fmt.Errorf("server error %d: %s", resp.StatusCode, grpcErr.Message)
	}
	return fmt.Errorf("server error %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// readSSE reads an SSE stream and calls fn for each data payload.
// Heartbeat comments and empty events are silently skipped.
func readSSE(r io.Reader, fn func(data string)) error {
	scanner := bufio.NewScanner(r)
	var dataLines []string
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "data: "):
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
		case line == "" && len(dataLines) > 0:
			fn(strings.Join(dataLines, "\n"))
			dataLines = dataLines[:0]
		}
	}
	return scanner.Err()
}

// sseMsg mirrors the gateway wsMessage struct for JSON decoding.
// Payload is []byte so encoding/json decodes it from base64 automatically.
type sseMsg struct {
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

func printMessage(data string) {
	var msg sseMsg
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		fmt.Println(data)
		return
	}
	switch msg.Type {
	case "heartbeat", "":
		return
	case "error":
		fmt.Fprintln(os.Stderr, "stream error:", msg.Error)
		return
	}

	out := map[string]any{
		"id":          msg.ID,
		"source":      msg.Source,
		"payload":     string(msg.Payload),
		"retry_count": msg.RetryCount,
	}
	if msg.Timestamp != nil {
		out["timestamp"] = msg.Timestamp.Format(time.RFC3339)
	}
	if len(msg.Metadata) > 0 {
		out["metadata"] = msg.Metadata
	}

	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}

func printJSON(r io.Reader) error {
	var v any
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
	return nil
}
