// Package component is the Matrix external-component SDK for Go
// (ML1, stdlib only).
//
// It speaks matrix.component/0.1 with the local host: handshake,
// registration, activation, and call serving with cooperative
// cancellation. It mirrors the reference SDKs (Rust matrix-component,
// Python matrix_component): same observable behavior.
//
// Concurrency (epic table: context.Context, bounded goroutines):
// the read loop never runs handler code — each call gets a goroutine
// bound to a cancellable context, events/streams go to a dedicated
// dispatcher goroutine over a bounded queue (64, drop-oldest,
// counted). Deadlines include transport wait; timeouts never retry.
package component

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Protocol constants.
const (
	ProtocolID      = "matrix.component"
	ProtocolVersion = "0.1"
	DefaultMaxFrame = 1024 * 1024
	EventCap        = 64
	// DependencyCallsFeature negotiates child calls (M6.1).
	DependencyCallsFeature = "dependency-calls/1"
)

var msgSeq atomic.Uint64

func fresh(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, msgSeq.Add(1))
}

// SdkError is a structured error: stable code, phase, safe message.
type SdkError struct {
	Code    string
	Phase   string
	Message string
}

func (e *SdkError) Error() string { return e.Code + " [" + e.Phase + "]: " + e.Message }

// DepError is a dependency-call error (wire code, no reinterpretation).
type DepError = SdkError

// ResError is an activation resource error.
type ResError = SdkError

func depErr(code, msg string) *SdkError  { return &SdkError{Code: code, Phase: "dependency", Message: msg} }
func resErr(code, msg string) *SdkError  { return &SdkError{Code: code, Phase: "resource", Message: msg} }

// GenEqual compares decimal-string generations without precision loss.
func GenEqual(a, b string) bool {
	x, err1 := strconv.ParseUint(strings.TrimSpace(a), 10, 64)
	y, err2 := strconv.ParseUint(strings.TrimSpace(b), 10, 64)
	return err1 == nil && err2 == nil && x == y
}

// Envelope is the wire envelope (unknown fields ignored on read).
type Envelope struct {
	Protocol   string          `json:"protocol"`
	Version    string          `json:"version"`
	Type       string          `json:"type"`
	MessageID  string          `json:"message_id"`
	SessionID  string          `json:"session_id,omitempty"`
	InstanceID string          `json:"instance_id,omitempty"`
	Generation string          `json:"generation,omitempty"`
	RequestID  string          `json:"request_id,omitempty"`
	Body       json.RawMessage `json:"body"`
}

func bodyMap(raw json.RawMessage) map[string]any {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]any{}
	}
	return m
}

func strField(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

// DepBinding is an opaque activation binding handle (M6.1).
type DepBinding struct {
	ID         string
	Capability string
}

// frame I/O ---------------------------------------------------------------

func writeFrame(w *bufio.Writer, mu *sync.Mutex, v any, maxFrame int) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(raw) > maxFrame {
		return fmt.Errorf("frame above max")
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(raw)))
	mu.Lock()
	defer mu.Unlock()
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(raw); err != nil {
		return err
	}
	return w.Flush()
}

func readFrame(r *bufio.Reader, maxFrame int) (*Envelope, bool, error) {
	var hdr [4]byte
	if _, err := readFull(r, hdr[:]); err != nil {
		return nil, false, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || int(n) > maxFrame {
		return nil, false, fmt.Errorf("bad frame length %d", n)
	}
	payload := make([]byte, n)
	if _, err := readFull(r, payload); err != nil {
		return nil, false, err
	}
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, true, nil // malformed: drop silently, keep session
	}
	return &env, false, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// CallCtx -----------------------------------------------------------------

// CallCtx is the per-call context: bound session, streams, dependencies.
type CallCtx struct {
	comp     *Component
	Ticket   string
	ctx      context.Context
	Bindings []DepBinding
}

func (c *CallCtx) SessionID() string { return c.comp.sessionID }

// EventDroppedCount reports edge-queue drops (slow observers).
func (c *CallCtx) EventDroppedCount() uint64 { return c.comp.evDropped.Load() }

// PendingStreamCount reports queued stream chunks (credit signal).
func (c *CallCtx) PendingStreamCount() int { return c.comp.pendingStreams() }

// Dependencies returns opaque handles of this activation.
func (c *CallCtx) Dependencies() []DepBinding {
	out := make([]DepBinding, len(c.Bindings))
	copy(out, c.Bindings)
	return out
}

// SendStream sends one text chunk. Binary callers must encode first:
// []byte is refused explicitly, never lossy-converted.
func (c *CallCtx) SendStream(streamID string, seq uint64, payload string) error {
	if streamID == "" {
		return &SdkError{Code: "invalid-message", Phase: "stream", Message: "empty stream id"}
	}
	k := c.comp
	return writeFrame(k.writer, &k.writeMu, map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "stream.data",
		"message_id":  fresh("m"),
		"session_id":  k.sessionID,
		"instance_id": k.instanceID,
		"generation":  k.generation,
		"body": map[string]any{
			"stream_id": streamID, "seq": strconv.FormatUint(seq, 10), "payload": payload,
		},
	}, k.maxFrame)
}

// SendStreamBytes refuses binary payloads explicitly (no silent UTF-8 loss).
func (c *CallCtx) SendStreamBytes(streamID string, seq uint64, _ []byte) error {
	return &SdkError{Code: "invalid-message", Phase: "stream",
		Message: "stream payloads are text; binary must be refused, never lossy-converted"}
}

// InvokeDependency invokes a dependency by opaque handle. It blocks
// until terminal, inheriting context cancellation. Without local
// negotiation it refuses with unsupported-feature, wire untouched.
func (c *CallCtx) InvokeDependency(binding string, input any, timeout time.Duration) (any, *SdkError) {
	k := c.comp
	if !k.hasFeature(DependencyCallsFeature) {
		return nil, depErr("unsupported-feature", "dependency calls not negotiated")
	}
	if timeout <= 0 {
		return nil, depErr("invalid-message", "timeout must be positive")
	}
	if input == nil {
		input = map[string]any{}
	}
	rid := fresh("r-dep")
	ch := make(chan depResult, 1)
	k.depMu.Lock()
	k.depWaiters[rid] = ch
	k.depMu.Unlock()
	defer func() {
		k.depMu.Lock()
		delete(k.depWaiters, rid)
		k.depMu.Unlock()
	}()
	err := writeFrame(k.writer, &k.writeMu, map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "dependency.open",
		"message_id": fresh("m-dep"), "session_id": k.sessionID,
		"instance_id": k.instanceID, "generation": k.generation,
		"request_id": rid,
		"body": map[string]any{
			"parent_ticket": c.Ticket, "binding_id": binding,
			"timeout_ms": timeout.Milliseconds(), "input": input,
		},
	}, k.maxFrame)
	if err != nil {
		return nil, depErr("internal", "send: "+err.Error())
	}
	// Local deadline = request + transport slack; expiry cancels on wire.
	timer := time.NewTimer(timeout + 10*time.Second)
	defer timer.Stop()
	select {
	case <-c.ctx.Done():
		k.sendDepCancel(rid)
		// Drain a racing terminal without blocking (waiter is gone).
		select {
		case <-ch:
		default:
		}
		return nil, depErr("cancelled", "parent cancelled")
	case <-timer.C:
		k.sendDepCancel(rid)
		select {
		case <-ch:
		default:
		}
		return nil, depErr("outcome-unknown", "sdk wait timeout")
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return r.output, nil
	}
}

type depResult struct {
	output any
	err    *SdkError
}

func (k *Component) sendDepCancel(target string) {
	_ = writeFrame(k.writer, &k.writeMu, map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "dependency.cancel",
		"message_id": fresh("m-dep-cancel"), "session_id": k.sessionID,
		"instance_id": k.instanceID, "generation": k.generation,
		"request_id": fresh("r-dep-cancel"),
		"body":       map[string]any{"target_request_id": target},
	}, k.maxFrame)
}

type resResult struct {
	extra map[string]any
	err   *SdkError
}

func (c *CallCtx) resourceRoundtrip(operation string, fields map[string]any) (map[string]any, *SdkError) {
	k := c.comp
	rid := fresh("r-res")
	ch := make(chan resResult, 1)
	k.resMu.Lock()
	k.resWaiters[rid] = ch
	k.resMu.Unlock()
	defer func() {
		k.resMu.Lock()
		delete(k.resWaiters, rid)
		k.resMu.Unlock()
	}()
	body := map[string]any{"operation_id": fresh("op-res")}
	for f, v := range fields {
		body[f] = v
	}
	if err := writeFrame(k.writer, &k.writeMu, map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "resource." + operation,
		"message_id": fresh("m-res"), "session_id": k.sessionID,
		"instance_id": k.instanceID, "generation": k.generation,
		"request_id": rid, "body": body,
	}, k.maxFrame); err != nil {
		return nil, resErr("internal", "send: "+err.Error())
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-c.ctx.Done():
		select {
		case <-ch:
		default:
		}
		return nil, resErr("cancelled", "parent cancelled")
	case <-timer.C:
		select {
		case <-ch:
		default:
		}
		return nil, resErr("outcome-unknown", "resource wait timeout")
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return r.extra, nil
	}
}

// AcquireResource acquires an activation resource (cap/sub/timer/task).
func (c *CallCtx) AcquireResource(kind, label string, intervalMs *uint64) (uint64, *SdkError) {
	fields := map[string]any{"kind": kind, "label": label}
	if intervalMs != nil {
		fields["interval_ms"] = *intervalMs
	}
	extra, rerr := c.resourceRoundtrip("acquire", fields)
	if rerr != nil {
		return 0, rerr
	}
	h, _ := extra["handle"].(string)
	n, err := strconv.ParseUint(h, 10, 64)
	if err != nil {
		return 0, resErr("internal", "missing handle")
	}
	return n, nil
}

// ReleaseResource releases a handle from AcquireResource.
func (c *CallCtx) ReleaseResource(handle uint64) *SdkError {
	_, rerr := c.resourceRoundtrip("release", map[string]any{"handle": strconv.FormatUint(handle, 10)})
	return rerr
}

// Handler -----------------------------------------------------------------

// Handler is component logic. OnCall runs on its own goroutine with a
// cancellable context; OnCancel observes cancellation; OnEvent and
// OnStream run on the dispatcher goroutine: observe fast, never block.
type Handler interface {
	OnCall(ctx *CallCtx, ticket, cap string, input any, callCtx context.Context) (any, *SdkError)
	OnCancel(ticket string)
	OnEvent(topic string, payload any)
	OnStream(streamID string, seq uint64, payload string)
}

// BaseHandler provides no-op defaults; embed it and override OnCall.
type BaseHandler struct{}

func (BaseHandler) OnCancel(string)                                     {}
func (BaseHandler) OnEvent(string, any)                                 {}
func (BaseHandler) OnStream(string, uint64, string)                     {}
func (BaseHandler) OnCall(*CallCtx, string, string, any, context.Context) (any, *SdkError) {
	return nil, &SdkError{Code: "internal", Phase: "handler", Message: "OnCall not implemented"}
}

type evItem struct {
	kind     string // "event" | "stream"
	topic    string
	payload  any
	streamID string
	seq      uint64
	text     string
}

// Component ---------------------------------------------------------------

// Component is a connected component: negotiated, activated session.
type Component struct {
	conn       net.Conn
	writer     *bufio.Writer
	writeMu    sync.Mutex
	sessionID  string
	instanceID string
	generation string
	maxFrame   int
	features   []string
	bindings   []DepBinding

	depMu      sync.Mutex
	depWaiters map[string]chan depResult
	resMu      sync.Mutex
	resWaiters map[string]chan resResult

	callsMu sync.Mutex
	calls   map[string]context.CancelFunc

	evMu      sync.Mutex
	evQueue   []evItem
	evDropped atomic.Uint64
	evWake    chan struct{}
}

func (k *Component) hasFeature(f string) bool {
	for _, g := range k.features {
		if g == f {
			return true
		}
	}
	return false
}

func (k *Component) pendingStreams() int {
	k.evMu.Lock()
	defer k.evMu.Unlock()
	n := 0
	for _, it := range k.evQueue {
		if it.kind == "stream" {
			n++
		}
	}
	return n
}

func (k *Component) enqueue(it evItem) {
	k.evMu.Lock()
	if len(k.evQueue) >= EventCap {
		k.evQueue = append(k.evQueue[:0], k.evQueue[1:]...)
		k.evDropped.Add(1)
	}
	k.evQueue = append(k.evQueue, it)
	k.evMu.Unlock()
	select {
	case k.evWake <- struct{}{}:
	default:
	}
}

// Connect negotiates, registers logical, and confirms activation.
func Connect(sockPath, logical string) (*Component, error) {
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	k := &Component{
		conn:       conn,
		writer:     bufio.NewWriter(conn),
		maxFrame:   DefaultMaxFrame,
		depWaiters: map[string]chan depResult{},
		resWaiters: map[string]chan resResult{},
		calls:      map[string]context.CancelFunc{},
		evWake:     make(chan struct{}, 1),
	}
	send := func(v any) error { return writeFrame(k.writer, &k.writeMu, v, k.maxFrame) }
	if err := send(map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "hello",
		"message_id": "h1",
		"body": map[string]any{
			"launch_token": os.Getenv("MATRIX_LAUNCH_TOKEN"),
			"versions":     []string{"0.1"},
			"max_frame":    DefaultMaxFrame,
			"client":       "matrix-component-go",
			"features":     []string{DependencyCallsFeature},
		},
	}); err != nil {
		conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	env, malformed, err := readFrame(reader, k.maxFrame)
	if err != nil || malformed || env == nil || env.Type != "welcome" {
		conn.Close()
		return nil, fmt.Errorf("expected welcome, got %+v err=%v", env, err)
	}
	k.sessionID = env.SessionID
	if mf, ok := bodyMap(env.Body)["max_frame"].(float64); ok && mf > 0 {
		k.maxFrame = int(mf)
	}
	welcomeBody := bodyMap(env.Body)
	if arr, ok := welcomeBody["features"].([]any); ok {
		for _, f := range arr {
			if s, ok := f.(string); ok {
				k.features = append(k.features, s)
			}
		}
	}
	if err := send(map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "component.register",
		"message_id": "reg1", "session_id": k.sessionID,
		"body": map[string]any{"manifest": map[string]any{"id": logical}},
	}); err != nil {
		conn.Close()
		return nil, err
	}
	env, malformed, err = readFrame(reader, k.maxFrame)
	if err != nil || malformed || env == nil || env.Type != "registered" {
		conn.Close()
		return nil, fmt.Errorf("register rejected: %+v err=%v", env, err)
	}
	k.instanceID = env.InstanceID
	k.generation = env.Generation
	env, malformed, err = readFrame(reader, k.maxFrame)
	if err != nil || malformed || env == nil || env.Type != "lifecycle.activate" {
		conn.Close()
		return nil, fmt.Errorf("expected activate, got %+v err=%v", env, err)
	}
	body := bodyMap(env.Body)
	if arr, ok := body["dependency_bindings"].([]any); ok {
		for _, b := range arr {
			m, _ := b.(map[string]any)
			id, _ := m["binding_id"].(string)
			cap, _ := m["capability"].(string)
			if id != "" && cap != "" {
				k.bindings = append(k.bindings, DepBinding{ID: id, Capability: cap})
			}
		}
	}
	op, _ := body["operation_id"]
	if op == nil {
		op = "op?"
	}
	if err := send(map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "lifecycle.result",
		"message_id": "lc1", "session_id": k.sessionID,
		"instance_id": k.instanceID, "generation": k.generation,
		"request_id": env.RequestID,
		"body":       map[string]any{"operation_id": op, "status": "ok", "pending": []any{}},
	}); err != nil {
		conn.Close()
		return nil, err
	}
	return k, nil
}

func (k *Component) boundOK(env *Envelope) bool {
	if env.SessionID != k.sessionID {
		return false
	}
	if env.InstanceID != "" && env.InstanceID != k.instanceID {
		return false
	}
	if env.Generation != "" && !GenEqual(env.Generation, k.generation) {
		return false
	}
	return true
}

func (k *Component) replyLifecycle(op any, requestID string) {
	if op == nil {
		op = "op?"
	}
	_ = writeFrame(k.writer, &k.writeMu, map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "lifecycle.result",
		"message_id": fresh("m-lc"), "session_id": k.sessionID,
		"instance_id": k.instanceID, "generation": k.generation,
		"request_id": requestID,
		"body":       map[string]any{"operation_id": op, "status": "ok", "pending": []any{}},
	}, k.maxFrame)
}

// Serve serves until EOF/error, quiesce, or dispose. It returns the
// exit reason ("dispose" on clean withdrawal).
func (k *Component) Serve(h Handler) string {
	stopEv := make(chan struct{})
	var stopOnce sync.Once
	defer stopOnce.Do(func() { close(stopEv) })
	go k.dispatcher(h, stopEv)
	reader := bufio.NewReader(k.conn)
	for {
		env, malformed, err := readFrame(reader, k.maxFrame)
		if err != nil {
			return "eof"
		}
		if malformed || env == nil {
			continue
		}
		if !k.boundOK(env) {
			continue
		}
		body := bodyMap(env.Body)
		switch env.Type {
		case "lifecycle.prepare", "lifecycle.activate", "lifecycle.quiesce":
			k.replyLifecycle(body["operation_id"], env.RequestID)
		case "lifecycle.dispose":
			k.replyLifecycle(body["operation_id"], env.RequestID)
			return "dispose"
		case "call.open":
			ticket := strField(body, "ticket")
			cap := strField(body, "capability")
			var input any = map[string]any{}
			if raw, ok := body["input"]; ok {
				input = raw
			}
			callCtx, cancel := context.WithCancel(context.Background())
			k.callsMu.Lock()
			k.calls[ticket] = cancel
			k.callsMu.Unlock()
			ctx := &CallCtx{comp: k, Ticket: ticket, ctx: callCtx, Bindings: k.bindings}
			openRID := env.RequestID
			go func() {
				out, callErr := h.OnCall(ctx, ticket, cap, input, callCtx)
				k.callsMu.Lock()
				delete(k.calls, ticket)
				k.callsMu.Unlock()
				if callCtx.Err() != nil {
					return // late after cancel: stay silent
				}
				var rbody map[string]any
				if callErr != nil {
					rbody = map[string]any{"ticket": ticket, "status": "error",
						"error": map[string]any{"code": callErr.Code, "message": callErr.Message}}
				} else {
					if out == nil {
						out = map[string]any{}
					}
					rbody = map[string]any{"ticket": ticket, "status": "ok", "output": out}
				}
				_ = writeFrame(k.writer, &k.writeMu, map[string]any{
					"protocol": ProtocolID, "version": ProtocolVersion, "type": "call.result",
					"message_id": fresh("m-call"), "session_id": k.sessionID,
					"instance_id": k.instanceID, "generation": k.generation,
					"request_id": openRID, "body": rbody,
				}, k.maxFrame)
			}()
		case "call.cancel":
			ticket := strField(body, "ticket")
			k.callsMu.Lock()
			cancel, ok := k.calls[ticket]
			k.callsMu.Unlock()
			if ok {
				cancel()
			}
			h.OnCancel(ticket)
		case "dependency.result":
			if env.RequestID != "" {
				k.depMu.Lock()
				ch, ok := k.depWaiters[env.RequestID]
				if ok {
					delete(k.depWaiters, env.RequestID)
				}
				k.depMu.Unlock()
				if ok {
					if strField(body, "status") == "ok" {
						out := body["output"]
						if out == nil {
							out = map[string]any{}
						}
						ch <- depResult{output: out}
					} else {
						em, _ := body["error"].(map[string]any)
						code, _ := em["code"].(string)
						msg, _ := em["message"].(string)
						if code == "" {
							code = "internal"
						}
						if msg == "" {
							msg = "remote error"
						}
						ch <- depResult{err: depErr(code, msg)}
					}
				}
			}
		case "resource.result":
			if env.RequestID != "" {
				k.resMu.Lock()
				ch, ok := k.resWaiters[env.RequestID]
				if ok {
					delete(k.resWaiters, env.RequestID)
				}
				k.resMu.Unlock()
				if ok {
					if strField(body, "status") == "ok" {
						extra := map[string]any{}
						for f, v := range body {
							if f != "operation_id" && f != "status" {
								extra[f] = v
							}
						}
						ch <- resResult{extra: extra}
					} else {
						code, _ := body["code"].(string)
						msg, _ := body["message"].(string)
						if code == "" {
							code = "internal"
						}
						if msg == "" {
							msg = "remote error"
						}
						ch <- resResult{err: resErr(code, msg)}
					}
				}
			}
		case "event.deliver":
			if topic := strField(body, "topic"); topic != "" {
				k.enqueue(evItem{kind: "event", topic: topic, payload: body["payload"]})
			}
		case "stream.data":
			sid := strField(body, "stream_id")
			seqStr := strField(body, "seq")
			seq, serr := strconv.ParseUint(seqStr, 10, 64)
			payload, _ := body["payload"].(string)
			if sid != "" && serr == nil {
				k.enqueue(evItem{kind: "stream", streamID: sid, seq: seq, text: payload})
			}
		}
	}
}

func (k *Component) dispatcher(h Handler, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-k.evWake:
		case <-time.After(100 * time.Millisecond):
		}
		for {
			k.evMu.Lock()
			if len(k.evQueue) == 0 {
				k.evMu.Unlock()
				break
			}
			it := k.evQueue[0]
			k.evQueue = k.evQueue[1:]
			k.evMu.Unlock()
			// Handler bugs never kill the session.
			func() {
				defer func() { _ = recover() }()
				if it.kind == "stream" {
					h.OnStream(it.streamID, it.seq, it.text)
				} else {
					h.OnEvent(it.topic, it.payload)
				}
			}()
		}
		select {
		case <-stop:
			return
		default:
		}
	}
}
