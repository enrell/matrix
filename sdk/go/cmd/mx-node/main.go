// Command mx-node is the generic Matrix test node for Go
// (ML1 contract: docs/ML1-NODE.md).
//
// Usage: mx-node --matrix-sock <sock> --id <logical>
//        [--event-log <path>] [--stream-log <path>] [--stream-slow-ms <n>]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	mx "github.com/enrell/matrix/sdk/go"
)

type node struct {
	id           string
	eventLog     string
	streamLog    string
	streamSlowMs int
}

func appendLine(path, line string) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	_, _ = f.WriteString(line + "\n")
	_ = f.Close()
}

func (n *node) OnCancel(ticket string) {}

func (n *node) OnEvent(topic string, payload any) {
	if n.eventLog != "" {
		raw, _ := json.Marshal(payload)
		appendLine(n.eventLog, topic+"\t"+string(raw))
	}
}

func (n *node) OnStream(streamID string, seq uint64, payload string) {
	if n.streamSlowMs > 0 {
		time.Sleep(time.Duration(n.streamSlowMs) * time.Millisecond)
	}
	if n.streamLog != "" {
		appendLine(n.streamLog, fmt.Sprintf("%s\t%d\t%d", streamID, seq, len(payload)))
	}
}

func num(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case int:
		return float64(t), true
	}
	return 0, false
}

func abortableSleep(ms int, ctx context.Context) bool {
	slept := 0
	for slept < ms {
		select {
		case <-ctx.Done():
			return true
		case <-time.After(5 * time.Millisecond):
			slept += 5
		}
	}
	return false
}

func (n *node) OnCall(ctx *mx.CallCtx, ticket, cap string, input any, callCtx context.Context) (any, *mx.SdkError) {
	in, _ := input.(map[string]any)
	if in == nil {
		in = map[string]any{}
	}
	bad := func(code, msg string) (any, *mx.SdkError) {
		return nil, &mx.SdkError{Code: code, Phase: "node", Message: msg}
	}
	if v, ok := num(in["sleep_ms"]); ok && v > 0 {
		if abortableSleep(int(v), callCtx) {
			return bad("cancelled", "aborted")
		}
	}
	if fail, _ := in["fail"].(string); fail != "" {
		return bad(fail, "remote "+fail)
	}
	if v, ok := num(in["amplify"]); ok && in["amplify"] != nil {
		size := int(v)
		if size < 0 {
			size = 0
		}
		if size > 1<<20 {
			size = 1 << 20
		}
		return map[string]any{"blob": strings.Repeat("x", size), "via": n.id}, nil
	}
	if on, _ := in["chain"].(bool); on {
		bindings := ctx.Dependencies()
		if len(bindings) == 0 {
			return bad("dependency-unavailable", "no binding")
		}
		inner, _ := in["input"].(map[string]any)
		if inner == nil {
			inner = map[string]any{}
		}
		timeoutMs := 5000.0
		if v, ok := num(in["timeout_ms"]); ok {
			timeoutMs = v
		}
		if timeoutMs < 1 {
			timeoutMs = 1
		}
		out, derr := ctx.InvokeDependency(bindings[0].ID, inner, time.Duration(timeoutMs)*time.Millisecond)
		if derr != nil {
			return nil, derr
		}
		return map[string]any{"chained": out, "via": n.id}, nil
	}
	if acq, ok := in["acquire"].(map[string]any); ok {
		kind, _ := acq["kind"].(string)
		label, _ := acq["label"].(string)
		var ms *uint64
		if v, ok := num(acq["interval_ms"]); ok && acq["interval_ms"] != nil {
			u := uint64(v)
			ms = &u
		}
		h, rerr := ctx.AcquireResource(kind, label, ms)
		if rerr != nil {
			return nil, rerr
		}
		return map[string]any{"acquired": map[string]any{"handle": fmt.Sprint(h)}, "via": n.id}, nil
	}
	if rel, ok := in["release"]; ok && rel != nil {
		var h uint64
		var isNum bool
		switch v := rel.(type) {
		case float64:
			if v == math.Trunc(v) && v >= 0 && v < 18446744073709551616.0 {
				h = uint64(v)
				isNum = true
			}
		case json.Number:
			if u, err := strconv.ParseUint(string(v), 10, 64); err == nil {
				h = u
				isNum = true
			}
		case int:
			if v >= 0 {
				h = uint64(v)
				isNum = true
			}
		}
		if isNum {
			if rerr := ctx.ReleaseResource(h); rerr != nil {
				return nil, rerr
			}
			return map[string]any{"released": fmt.Sprint(h), "via": n.id}, nil
		}
	}
	if spec, ok := in["stream_send"].(map[string]any); ok {
		streamID, _ := spec["stream_id"].(string)
		if streamID == "" {
			streamID = "s-test"
		}
		chunks, _ := num(spec["chunks"])
		nbytes, _ := num(spec["chunk_bytes"])
		slp, _ := num(spec["sleep_ms"])
		if chunks < 0 {
			chunks = 0
		}
		if chunks > 256 {
			chunks = 256
		}
		if nbytes < 0 {
			nbytes = 0
		}
		if nbytes > 4096 {
			nbytes = 4096
		}
		payload := strings.Repeat("x", int(nbytes))
		var sent uint64
		for seq := uint64(0); seq < uint64(chunks); seq++ {
			select {
			case <-callCtx.Done():
				return bad("cancelled", "aborted")
			default:
			}
			if err := ctx.SendStream(streamID, seq, payload); err != nil {
				if se, ok := any(err).(*mx.SdkError); ok {
					return nil, &mx.SdkError{Code: "stream-refused", Phase: "node", Message: se.Message}
				}
				return bad("stream-refused", err.Error())
			}
			sent++
			if slp > 0 {
				s := slp
				if s > 50 {
					s = 50
				}
				if abortableSleep(int(s), callCtx) {
					return bad("cancelled", "aborted")
				}
			}
		}
		return map[string]any{"stream_sent": sent, "via": n.id}, nil
	}
	if spec, ok := in["chain_with_streams"].(map[string]any); ok {
		// Concurrent chain + streams (M7 bidi legs): streams while
		// the child leg is in flight on this same session.
		return n.chainWithStreams(ctx, callCtx, spec)
	}
	return map[string]any{"echo": in, "via": n.id}, nil
}

func (n *node) chainWithStreams(ctx *mx.CallCtx, callCtx context.Context, spec map[string]any) (any, *mx.SdkError) {
	bad := func(code, msg string) (any, *mx.SdkError) {
		return nil, &mx.SdkError{Code: code, Phase: "node", Message: msg}
	}
	streamID, _ := spec["stream_id"].(string)
	if streamID == "" {
		streamID = "s-bidi"
	}
	chunks, _ := num(spec["chunks"])
	nbytes, _ := num(spec["chunk_bytes"])
	interval, intervalOK := num(spec["interval_ms"])
	if !intervalOK {
		interval = 20
	}
	prime, primeOK := num(spec["prime_ms"])
	if !primeOK {
		prime = 50
	}
	if chunks < 0 {
		chunks = 0
	}
	if chunks > 32 {
		chunks = 32
	}
	if nbytes < 0 {
		nbytes = 0
	}
	if nbytes > 1024 {
		nbytes = 1024
	}
	if interval < 0 {
		interval = 0
	}
	if interval > 50 {
		interval = 50
	}
	if prime < 0 {
		prime = 0
	}
	if prime > 1000 {
		prime = 1000
	}
	payload := strings.Repeat("x", int(nbytes))
	var sent uint64
	done := make(chan uint64, 1)
	go func() {
		var s uint64
		if prime > 0 {
			time.Sleep(time.Duration(prime) * time.Millisecond)
		}
		for seq := uint64(0); seq < uint64(chunks); seq++ {
			if err := ctx.SendStream(streamID, seq, payload); err != nil {
				break
			}
			s++
			if interval > 0 {
				time.Sleep(time.Duration(interval) * time.Millisecond)
			}
		}
		done <- s
	}()
	bindings := ctx.Dependencies()
	if len(bindings) == 0 {
		sent = <-done
		_ = sent
		return bad("dependency-unavailable", "no binding")
	}
	inner, _ := spec["input"].(map[string]any)
	if inner == nil {
		inner = map[string]any{}
	}
	timeoutMs, _ := num(spec["timeout_ms"])
	if timeoutMs < 1 {
		timeoutMs = 8000
	}
	out, derr := ctx.InvokeDependency(bindings[0].ID, inner, time.Duration(timeoutMs)*time.Millisecond)
	sent = <-done
	if derr != nil {
		return nil, derr
	}
	return map[string]any{"chained": out, "via": n.id, "stream_sent": sent}, nil
}

func main() {
	sock := flag.String("matrix-sock", "", "host Unix socket")
	id := flag.String("id", "dep-node", "logical component id")
	eventLog := flag.String("event-log", "", "event log path")
	streamLog := flag.String("stream-log", "", "stream log path")
	streamSlowMs := flag.Int("stream-slow-ms", 0, "ms to sleep per received chunk")
	flag.Parse()
	if *sock == "" {
		fmt.Fprintln(os.Stderr, "usage: mx-node --matrix-sock <sock> [--id <logical>] ...")
		os.Exit(2)
	}
	comp, err := mx.Connect(*sock, *id)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect: "+err.Error())
		os.Exit(2)
	}
	switch comp.Serve(&node{id: *id, eventLog: *eventLog, streamLog: *streamLog, streamSlowMs: *streamSlowMs}) {
	case "dispose", "eof":
		os.Exit(0)
	default:
		os.Exit(1)
	}
}
