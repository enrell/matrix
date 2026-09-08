package component

// Operator-surface tests: mapping over a fake binary, bootstrap
// phases, doctor shape; live parts need MX_MATRIX_MANAGED + openssl.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// sdk/go -> repo root.
	return filepath.Dir(filepath.Dir(dir))
}

func managedBinary() string {
	if v := os.Getenv("MX_MATRIX_MANAGED"); v != "" {
		return v
	}
	return ""
}

func writeFakeBinary(t *testing.T) (dir, fake string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "opfake-")
	if err != nil {
		t.Fatal(err)
	}
	fake = filepath.Join(dir, "matrix-managed")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"request\" ]; then\n" +
		"  case \"$7\" in\n" +
		"    *sleep*) exec sleep 30;;\n" +
		"    *badjson*) echo 'not json';;\n" +
		"    *denied*) echo 'permission-denied: nope' >&2; exit 1;;\n" +
		"    *) echo '{\"ok\":true}';;\n" +
		"  esac\n" +
		"else echo 'usage: matrix-managed serve <config>' >&2; exit 1\n" +
		"fi\n"
	if err := os.WriteFile(fake, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"ca", "cert", "key"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte{}, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir, fake
}

func TestOperatorMapping(t *testing.T) {
	dir, fake := writeFakeBinary(t)
	defer os.RemoveAll(dir)
	pki := OperatorPKI{CA: filepath.Join(dir, "ca"), Cert: filepath.Join(dir, "cert"), Key: filepath.Join(dir, "key")}
	c, err := Attach(fake, "127.0.0.1:9", pki, "")
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	got, err := c.Request(map[string]any{"action": "ping"}, 0)
	if err != nil || got["ok"] != true {
		t.Fatalf("ping: %v %v", got, err)
	}
	if _, err := c.Request(map[string]any{"action": "denied-op"}, 0); err == nil {
		t.Fatal("denied-op succeeded")
	} else if oe, ok := err.(*OperatorError); !ok || oe.Code != "permission-denied" {
		t.Fatalf("wrong denial: %v", err)
	}
	if _, err := c.Request(map[string]any{"action": "badjson"}, 0); err == nil {
		t.Fatal("badjson succeeded")
	} else if oe, ok := err.(*OperatorError); !ok || oe.Code != "internal" {
		t.Fatalf("wrong code: %v", err)
	}
	if _, err := c.Request(map[string]any{"action": "sleep"}, time.Second); err == nil {
		t.Fatal("sleep succeeded")
	} else if oe, ok := err.(*OperatorError); !ok || oe.Code != "outcome-unknown" {
		t.Fatalf("wrong timeout code: %v", err)
	}
	if _, err := c.Request(map[string]any{"no-action": true}, 0); err == nil {
		t.Fatal("actionless succeeded")
	}
	c.Close()
	if _, err := c.Request(map[string]any{"action": "ping"}, 0); err == nil {
		t.Fatal("closed client served")
	}
}

func TestBootstrapPhases(t *testing.T) {
	if _, err := Start("/nonexistent/matrix-managed", map[string]any{"home": "/tmp/x"}, OperatorPKI{}, ""); err == nil {
		t.Fatal("missing binary started")
	} else if be, ok := err.(*BootstrapError); !ok || be.Phase != "spawn" {
		t.Fatalf("wrong phase: %v", err)
	}
	if _, err := Start("/bin/true", map[string]any{"components": []any{}}, OperatorPKI{}, ""); err == nil {
		t.Fatal("homeless config started")
	} else if be, ok := err.(*BootstrapError); !ok || be.Phase != "config" {
		t.Fatalf("wrong phase: %v", err)
	}
	if _, err := Attach("/nonexistent/x", "127.0.0.1:1", OperatorPKI{}, ""); err == nil {
		t.Fatal("bad attach succeeded")
	}
}

func TestDoctorShape(t *testing.T) {
	rep := Doctor("/nonexistent/binary")
	if rep.Go == "" || rep.Binary == "" {
		t.Fatalf("thin report: %+v", rep)
	}
	blob, _ := json.Marshal(rep)
	if strings.Contains(strings.Replace(string(blob), "socket_dir_writable", "", 1), "lease") {
		t.Fatal("secret-adjacent word in report")
	}
	if bin := managedBinary(); bin != "" {
		rep2 := Doctor(bin)
		if !rep2.BinaryFound || !rep2.CLIShapeOK {
			t.Fatalf("binary not recognized: %+v", rep2)
		}
	}
}

func liveAvailable(t *testing.T) (string, string) {
	t.Helper()
	bin := managedBinary()
	if bin == "" {
		t.Skip("needs MX_MATRIX_MANAGED (a staged matrix-managed binary)")
	}
	if !haveBinary("openssl") {
		t.Skip("needs openssl")
	}
	devpki := filepath.Join(repoRoot(t), "scripts", "dev-pki.py")
	if _, err := os.Stat(devpki); err != nil {
		t.Skip("needs scripts/dev-pki.py")
	}
	t.Setenv("MX_DEV_PKI", devpki)
	return bin, devpki
}

func TestStartAttachLifecycle(t *testing.T) {
	bin, devpki := liveAvailable(t)
	tmp, err := os.MkdirTemp("", "oplive-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	pkiDir := filepath.Join(tmp, "pki")
	cmd := exec.Command("python3", devpki, pkiDir, "--server-name", "localhost")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dev-pki: %v %s", err, out)
	}
	der, _ := os.ReadFile(filepath.Join(pkiDir, "client.der"))
	sum := sha256.Sum256(der)
	fp := hex.EncodeToString(sum[:])
	cfg := map[string]any{
		"home": filepath.Join(tmp, "home"),
		"components": []any{map[string]any{
			"manifest": map[string]any{"id": "echo", "capabilities": []any{"echo.msg@1"}, "reducer": "echo"},
			"trusted":  true,
		}},
		"grants": map[string]any{fp: map[string]any{
			"components": []any{"echo"}, "capabilities": []any{"echo.msg@1"}}},
		"tls": map[string]any{"listen": "127.0.0.1:0",
			"ca": filepath.Join(pkiDir, "ca.der"), "cert": filepath.Join(pkiDir, "server.der"),
			"key": filepath.Join(pkiDir, "server-key.der")},
	}
	opki := OperatorPKI{CA: filepath.Join(pkiDir, "ca.der"),
		Cert: filepath.Join(pkiDir, "client.der"), Key: filepath.Join(pkiDir, "client-key.der")}
	kernel, err := Start(bin, cfg, opki, "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer kernel.Close()
	if !strings.HasPrefix(kernel.API, "0.1.") {
		t.Fatalf("api=%s", kernel.API)
	}
	act, err := kernel.ClientOf().Activate("echo", 20000, 0)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	lease, _ := act["lease"].(string)
	fence, _ := act["fence"].(string)
	v, err := kernel.ClientOf().Invoke(lease, fence, "op-live-1", "echo.msg@1", map[string]any{"ping": 1}, 0)
	if err != nil || v["ok"] != true {
		t.Fatalf("invoke: %v %v", v, err)
	}
	attached, err := Attach(bin, kernel.Listen(), opki, "")
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if v2, err := attached.Invoke(lease, fence, "op-live-2", "echo.msg@1", map[string]any{}, 0); err != nil || v2["ok"] != true {
		t.Fatalf("attached invoke: %v %v", v2, err)
	}
	attached.Close() // attachment owns nothing: daemon keeps serving
	if v3, err := kernel.ClientOf().Invoke(lease, fence, "op-live-3", "echo.msg@1", map[string]any{}, 0); err != nil || v3["ok"] != true {
		t.Fatalf("post-attach invoke: %v %v", v3, err)
	}
	if _, err := kernel.ClientOf().Invoke("dead", "1", "op-x", "echo.msg@1", map[string]any{}, 0); err == nil {
		t.Fatal("dead lease served")
	}
	if _, err := kernel.ClientOf().Release(lease, fence, 0); err != nil {
		t.Fatalf("release: %v", err)
	}
	kernel.Close()
	kernel.Close() // idempotent
	if _, err := kernel.ClientOf().Invoke(lease, fence, "op-x", "echo.msg@1", map[string]any{}, 0); err == nil {
		t.Fatal("closed kernel served")
	}
}
