package component

// Loopback fake host for the Go SDK tests (stdlib only).

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

const testMaxFrame = 1024 * 1024

var testSeq atomic.Uint64

func testFresh(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, testSeq.Add(1))
}

func testSend(t *testing.T, conn net.Conn, msg any) {
	t.Helper()
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(raw)))
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatalf("write hdr: %v", err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatalf("write body: %v", err)
	}
}

func testRead(t *testing.T, conn net.Conn, timeout time.Duration) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var hdr [4]byte
	if _, err := readFullBuf(conn, hdr[:]); err != nil {
		t.Fatalf("read hdr: %v", err)
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > testMaxFrame {
		t.Fatalf("bad length %d", n)
	}
	buf := make([]byte, n)
	if _, err := readFullBuf(conn, buf); err != nil {
		t.Fatalf("read body: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(buf, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return m
}

func readFullBuf(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func testEnv(ty string, body map[string]any) map[string]any {
	return map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": ty,
		"message_id": testFresh("m"), "session_id": "s1", "instance_id": "1",
		"generation": "1", "request_id": testFresh("r"), "body": body,
	}
}

func testCallOpen(ticket string, input any) map[string]any {
	return map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "call.open",
		"message_id": "m-" + ticket, "session_id": "s1", "instance_id": "1",
		"generation": "1", "request_id": "r-" + ticket,
		"body": map[string]any{"ticket": ticket, "capability": "c@1", "input": input},
	}
}

func testDispose() map[string]any {
	return map[string]any{
		"protocol": ProtocolID, "version": ProtocolVersion, "type": "lifecycle.dispose",
		"message_id": "d1", "session_id": "s1", "instance_id": "1",
		"generation": "1", "request_id": "rd1",
		"body": map[string]any{"operation_id": "op", "deadline_ms": 100},
	}
}

// testLoopback serves one connection through handshake, then runs script.
func testLoopback(t *testing.T, features []string, bindings []any, script func(net.Conn)) (string, chan struct{}) {
	t.Helper()
	dir, err := os.MkdirTemp("", "gounits-")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "t.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		defer os.RemoveAll(dir)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		hello := testRead(t, conn, 10*time.Second)
		if hello["type"] != "hello" {
			t.Errorf("expected hello, got %v", hello["type"])
			return
		}
		testSend(t, conn, map[string]any{
			"protocol": ProtocolID, "version": ProtocolVersion, "type": "welcome",
			"message_id": "h1", "session_id": "s1",
			"body": map[string]any{"version": "0.1", "max_frame": testMaxFrame,
				"limits": map[string]any{}, "features": features},
		})
		reg := testRead(t, conn, 10*time.Second)
		if reg["type"] != "component.register" {
			t.Errorf("expected register, got %v", reg["type"])
			return
		}
		testSend(t, conn, map[string]any{
			"protocol": ProtocolID, "version": ProtocolVersion, "type": "registered",
			"message_id": "r", "session_id": "s1", "instance_id": "1",
			"generation": "1", "body": map[string]any{"logical": "t"},
		})
		act := testEnv("lifecycle.activate", map[string]any{
			"operation_id": "op", "manifest": map[string]any{},
			"bindings": []any{}, "dependency_bindings": bindings,
		})
		act["request_id"] = "q"
		testSend(t, conn, act)
		lc := testRead(t, conn, 10*time.Second)
		if lc["type"] != "lifecycle.result" {
			t.Errorf("expected lifecycle.result, got %v", lc["type"])
			return
		}
		script(conn)
	}()
	return path, done
}
