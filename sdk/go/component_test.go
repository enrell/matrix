package component

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGenEqual(t *testing.T) {
	if !GenEqual("18446744073709551615", "18446744073709551615") {
		t.Fatal("u64 max must compare equal")
	}
	if GenEqual("18446744073709551615", "18446744073709551614") {
		t.Fatal("off-by-one must differ")
	}
	if GenEqual("nope", "1") {
		t.Fatal("non-numeric must not match")
	}
}

type echoHandler struct{ BaseHandler }

func (echoHandler) OnCall(ctx *CallCtx, ticket, cap string, input any, _ context.Context) (any, *SdkError) {
	return map[string]any{"echo": input}, nil
}

func TestMalformedFrameSurvives(t *testing.T) {
	path, done := testLoopback(t, []string{}, []any{}, func(conn net.Conn) {
		garbage := []byte("{oops")
		var hdr [4]byte
		hdr[0], hdr[1], hdr[2], hdr[3] = 0, 0, 0, byte(len(garbage))
		if _, err := conn.Write(hdr[:]); err != nil {
			t.Errorf("write hdr: %v", err)
			return
		}
		if _, err := conn.Write(garbage); err != nil {
			t.Errorf("write garbage: %v", err)
			return
		}
		testSend(t, conn, testCallOpen("tkt-9", map[string]any{"ping": 1}))
		ans := testRead(t, conn, 10*time.Second)
		body, _ := ans["body"].(map[string]any)
		out, _ := body["output"].(map[string]any)
		echo, _ := out["echo"].(map[string]any)
		if ans["type"] != "call.result" || echo["ping"] != float64(1) {
			t.Fatalf("bad answer: %v", ans)
		}
		testSend(t, conn, testDispose())
		_ = testRead(t, conn, 10*time.Second)
	})
	comp, err := Connect(path, "t")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if rc := comp.Serve(echoHandler{}); rc != "dispose" {
		t.Fatalf("reason=%s", rc)
	}
	<-done
}

func TestStaleGenerationIgnored(t *testing.T) {
	path, done := testLoopback(t, []string{}, []any{}, func(conn net.Conn) {
		stale := testCallOpen("tkt-stale", map[string]any{})
		stale["generation"] = "999"
		testSend(t, conn, stale)
		testSend(t, conn, testCallOpen("tkt-9", map[string]any{}))
		ans := testRead(t, conn, 10*time.Second)
		body, _ := ans["body"].(map[string]any)
		if body["ticket"] != "tkt-9" {
			t.Fatalf("stale frame acted on: %v", ans)
		}
		testSend(t, conn, testDispose())
		_ = testRead(t, conn, 10*time.Second)
	})
	comp, err := Connect(path, "t")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if rc := comp.Serve(echoHandler{}); rc != "dispose" {
		t.Fatalf("reason=%s", rc)
	}
	<-done
}

type gateHandler struct{ BaseHandler }

func (gateHandler) OnCall(ctx *CallCtx, ticket, cap string, input any, _ context.Context) (any, *SdkError) {
	if _, derr := ctx.InvokeDependency("bind-x", map[string]any{}, 2*time.Second); derr != nil {
		return map[string]any{"refused": derr.Code}, nil
	}
	return map[string]any{"unexpected": "wire-touched"}, nil
}

func TestFeatureGateRefusesLocally(t *testing.T) {
	path, done := testLoopback(t, []string{}, []any{}, func(conn net.Conn) {
		testSend(t, conn, testCallOpen("tkt-9", map[string]any{}))
		ans := testRead(t, conn, 10*time.Second)
		body, _ := ans["body"].(map[string]any)
		out, _ := body["output"].(map[string]any)
		if out["refused"] != "unsupported-feature" {
			t.Fatalf("bad gate: %v", ans)
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		var hdr [4]byte
		if _, err := readFullBuf(conn, hdr[:]); err == nil {
			t.Fatal("SDK touched the wire after local refusal")
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		testSend(t, conn, testDispose())
		_ = testRead(t, conn, 10*time.Second)
	})
	comp, err := Connect(path, "t")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if comp.hasFeature(DependencyCallsFeature) {
		t.Fatal("empty features negotiated?!")
	}
	if rc := comp.Serve(gateHandler{}); rc != "dispose" {
		t.Fatalf("reason=%s", rc)
	}
	<-done
}

type chainHandler struct{ BaseHandler }

func (chainHandler) OnCall(ctx *CallCtx, ticket, cap string, input any, _ context.Context) (any, *SdkError) {
	out, derr := ctx.InvokeDependency(ctx.Dependencies()[0].ID, map[string]any{"v": 1}, 5*time.Second)
	if derr != nil {
		return nil, derr
	}
	return map[string]any{"got": out}, nil
}

func TestDependencyRoundtrip(t *testing.T) {
	bindings := []any{map[string]any{"binding_id": "bind-1", "capability": "c@1"}}
	path, done := testLoopback(t, []string{DependencyCallsFeature}, bindings, func(conn net.Conn) {
		testSend(t, conn, testCallOpen("tkt-9", map[string]any{"chain_it": true}))
		opened := testRead(t, conn, 10*time.Second)
		if opened["type"] != "dependency.open" {
			t.Fatalf("expected open, got %v", opened["type"])
		}
		body, _ := opened["body"].(map[string]any)
		if body["binding_id"] != "bind-1" || body["parent_ticket"] != "tkt-9" {
			t.Fatalf("bad open: %v", body)
		}
		rid, _ := opened["request_id"].(string)
		res := testEnv("dependency.result",
			map[string]any{"status": "ok", "output": map[string]any{"deep": 1}})
		res["request_id"] = rid // correlate to the open we answered
		testSend(t, conn, res)
		ans := testRead(t, conn, 10*time.Second)
		abody, _ := ans["body"].(map[string]any)
		aout, _ := abody["output"].(map[string]any)
		got, _ := aout["got"].(map[string]any)
		if got["deep"] != float64(1) {
			t.Fatalf("bad chain answer: %v", ans)
		}
		testSend(t, conn, testDispose())
		_ = testRead(t, conn, 10*time.Second)
	})
	comp, err := Connect(path, "t")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if rc := comp.Serve(chainHandler{}); rc != "dispose" {
		t.Fatalf("reason=%s", rc)
	}
	<-done
}

type slowHandler struct {
	BaseHandler
	mu     sync.Mutex
	events int
}

func (h *slowHandler) OnEvent(topic string, payload any) {
	time.Sleep(30 * time.Millisecond) // slow observer yields; reader flows
	h.mu.Lock()
	h.events++
	h.mu.Unlock()
}

func (h *slowHandler) OnCall(ctx *CallCtx, ticket, cap string, input any, _ context.Context) (any, *SdkError) {
	in, _ := input.(map[string]any)
	if in != nil {
		if _, ok := in["report_drops"]; ok {
			return map[string]any{"dropped": ctx.EventDroppedCount()}, nil
		}
	}
	return map[string]any{"ok": true}, nil
}

func TestFloodDropsCountedReaderIndependent(t *testing.T) {
	path, done := testLoopback(t, []string{}, []any{}, func(conn net.Conn) {
		for i := 0; i < 120; i++ {
			testSend(t, conn, testEnv("event.deliver",
				map[string]any{"topic": "t", "payload": map[string]any{"n": i}}))
		}
		start := time.Now()
		testSend(t, conn, testCallOpen("tkt-9", map[string]any{}))
		ans := testRead(t, conn, 10*time.Second)
		if dt := time.Since(start); dt >= 5*time.Second {
			t.Fatalf("call stalled under flood: %s", dt)
		}
		if ans["type"] != "call.result" {
			t.Fatalf("bad answer: %v", ans["type"])
		}
		testSend(t, conn, testCallOpen("tkt-10", map[string]any{"report_drops": true}))
		rep := testRead(t, conn, 10*time.Second)
		rbody, _ := rep["body"].(map[string]any)
		rout, _ := rbody["output"].(map[string]any)
		dropped, _ := rout["dropped"].(float64)
		if dropped < 1 {
			t.Fatalf("overflow not counted: %v", rep)
		}
		testSend(t, conn, testDispose())
		_ = testRead(t, conn, 10*time.Second)
	})
	comp, err := Connect(path, "t")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if rc := comp.Serve(&slowHandler{}); rc != "dispose" {
		t.Fatalf("reason=%s", rc)
	}
	<-done
}

type binHandler struct{ BaseHandler }

func (binHandler) OnCall(ctx *CallCtx, ticket, cap string, input any, _ context.Context) (any, *SdkError) {
	// Binary payloads refuse explicitly, never lossy-convert.
	if err := ctx.SendStreamBytes("s1", 0, []byte{0xff, 0xfe}); err != nil {
		if se, ok := any(err).(*SdkError); ok {
			return nil, &SdkError{Code: "invalid-message", Phase: "node", Message: se.Message}
		}
		return nil, &SdkError{Code: "invalid-message", Phase: "node", Message: err.Error()}
	}
	return map[string]any{}, nil
}

func TestBinaryRefused(t *testing.T) {
	path, done := testLoopback(t, []string{}, []any{}, func(conn net.Conn) {
		testSend(t, conn, testCallOpen("tkt-9", map[string]any{"send_bytes": true}))
		ans := testRead(t, conn, 10*time.Second)
		body, _ := ans["body"].(map[string]any)
		if body["status"] != "error" {
			t.Fatalf("binary accepted: %v", ans)
		}
		em, _ := body["error"].(map[string]any)
		if em["code"] != "invalid-message" {
			t.Fatalf("wrong code: %v", em)
		}
		if strings.Contains(string(mustJSON(ans)), "�") {
			t.Fatal("lossy replacement char on the wire")
		}
		testSend(t, conn, testDispose())
		_ = testRead(t, conn, 10*time.Second)
	})
	comp, err := Connect(path, "t")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if rc := comp.Serve(binHandler{}); rc != "dispose" {
		t.Fatalf("reason=%s", rc)
	}
	<-done
}

func mustJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}
