// Operator/application surface for Go (ML1, stdlib only).
//
// Same contract as Python matrix_operator and the JS operator module:
// calls travel through the staged matrix-managed binary (serve to own
// a kernel, request for authenticated admin actions over mutual TLS).
// Start owns its process; Connect only attaches. Server identity never
// implies caller authority: OperatorPKI is always explicit.
package component

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

// ExpectedAPIPrefix gates binary compatibility before operating.
const ExpectedAPIPrefix = "0.1."

const (
	readyTimeout = 30 * time.Second
	stopTimeout  = 5 * time.Second
	requestSlack = 10 * time.Second
)

// BootstrapError is a start/connect failure. Nothing owned is left behind.
type BootstrapError struct {
	Code    string
	Phase   string
	Message string
}

func (e *BootstrapError) Error() string { return e.Code + " [" + e.Phase + "]: " + e.Message }

// OperatorError is an admin refusal/failure. Wire codes pass through;
// timeouts report outcome-unknown and never retry implicitly.
type OperatorError struct {
	Code    string
	Message string
}

func (e *OperatorError) Error() string { return e.Code + " [request]: " + e.Message }

var knownCodes = []string{
	"permission-denied", "stale-generation", "outcome-unknown",
	"unauthenticated", "invalid-message", "unsupported-version",
	"dependency-unavailable", "ambiguous-provider",
	"context-not-active", "resource-exhausted", "deadline-exceeded",
	"cancelled", "cleanup-pending", "internal",
}

func guessCode(stderr string) string {
	low := strings.ToLower(stderr)
	for _, c := range knownCodes {
		if strings.Contains(low, c) {
			return c
		}
	}
	if strings.TrimSpace(stderr) != "" {
		return "transport"
	}
	return "internal"
}

// OperatorPKI is the caller's identity (never the server's).
type OperatorPKI struct {
	CA   string
	Cert string
	Key  string
}

// Client is an attached operator client. Close marks the handle
// closed and never stops any daemon.
type Client struct {
	binary     string
	listen     string
	pki        OperatorPKI
	serverName string
	owned      bool
	closed     bool
}

// Request performs one authenticated admin action.
func (c *Client) Request(action map[string]any, timeout time.Duration) (map[string]any, error) {
	if c.closed {
		return nil, &SdkError{Code: "internal", Phase: "client", Message: "client is closed"}
	}
	if action == nil || action["action"] == nil {
		return nil, &OperatorError{Code: "invalid-message", Message: "action map with 'action' required"}
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	raw, _ := json.Marshal(action)
	ctx, cancel := context.WithTimeout(context.Background(), timeout+requestSlack)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.binary, "request",
		c.pki.CA, c.pki.Cert, c.pki.Key, c.listen, c.serverName, string(raw))
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if ctx.Err() == context.DeadlineExceeded {
		return nil, &OperatorError{Code: "outcome-unknown",
			Message: fmt.Sprintf("admin action timed out after %s (not retried)", timeout)}
	}
	if err != nil {
		first := text
		if i := strings.IndexByte(first, '\n'); i >= 0 {
			first = first[:i]
		}
		if first == "" {
			first = "request refused"
		}
		return nil, &OperatorError{Code: guessCode(text), Message: first}
	}
	var decoded map[string]any
	if jerr := json.Unmarshal([]byte(text), &decoded); jerr != nil {
		return nil, &OperatorError{Code: "internal", Message: "undecodable response: " + jerr.Error()}
	}
	return decoded, nil
}

// Activate provisions a lease for component.
func (c *Client) Activate(component string, ttlMs int64, timeout time.Duration) (map[string]any, error) {
	if ttlMs <= 0 {
		ttlMs = 20000
	}
	return c.Request(map[string]any{"action": "activate", "component": component, "ttl_ms": ttlMs}, timeout)
}

// Status queries a lease.
func (c *Client) Status(lease, fence string, timeout time.Duration) (map[string]any, error) {
	return c.Request(map[string]any{"action": "status", "lease": lease, "fence": fence}, timeout)
}

// Invoke calls a capability under a lease.
func (c *Client) Invoke(lease, fence, operation, cap string, input any, timeout time.Duration) (map[string]any, error) {
	if input == nil {
		input = map[string]any{}
	}
	return c.Request(map[string]any{"action": "invoke", "lease": lease, "fence": fence,
		"operation": operation, "cap": cap, "input": input}, timeout)
}

// Release retires a lease.
func (c *Client) Release(lease, fence string, timeout time.Duration) (map[string]any, error) {
	return c.Request(map[string]any{"action": "release", "lease": lease, "fence": fence}, timeout)
}

// Renew rotates a lease.
func (c *Client) Renew(lease, fence string, ttlMs int64, timeout time.Duration) (map[string]any, error) {
	if ttlMs <= 0 {
		ttlMs = 20000
	}
	return c.Request(map[string]any{"action": "renew", "lease": lease, "fence": fence, "ttl_ms": ttlMs}, timeout)
}

// WaitReady polls status until the session reports ready.
func (c *Client) WaitReady(lease, fence string, timeout time.Duration) (map[string]any, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	end := time.Now().Add(timeout)
	var last map[string]any
	for time.Now().Before(end) {
		st, err := c.Status(lease, fence, 5*time.Second)
		if err != nil {
			return nil, err
		}
		last = st
		if ready, _ := st["ready"].(bool); ready {
			return st, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	red, _ := json.Marshal(redact(last))
	return nil, &OperatorError{Code: "outcome-unknown",
		Message: fmt.Sprintf("session not ready in budget (last=%s)", red)}
}

// Close marks this handle closed. It never stops any daemon.
func (c *Client) Close() { c.closed = true }

func redact(v map[string]any) map[string]any {
	out := map[string]any{}
	for k, val := range v {
		if k == "lease" || k == "launch_token" || k == "token" {
			out[k] = "<redacted>"
			continue
		}
		if m, ok := val.(map[string]any); ok {
			out[k] = redact(m)
			continue
		}
		out[k] = val
	}
	return out
}

// OwnedKernel is a kernel this application started and owns.
type OwnedKernel struct {
	cmd     *exec.Cmd
	workdir string
	client  *Client
	// Epoch, API and Profile come from the ready line.
	Epoch   any
	API     string
	Profile string
	closed  bool
}

// ClientOf returns the owned operator client.
func (k *OwnedKernel) ClientOf() *Client { return k.client }

// Listen returns the management address.
func (k *OwnedKernel) Listen() string { return k.client.listen }

// Close is idempotent: SIGTERM, bounded wait, SIGKILL, remove the
// private directory. It reaps exactly the spawned daemon.
func (k *OwnedKernel) Close() {
	if k.closed {
		return
	}
	k.closed = true
	k.client.Close()
	if k.cmd.Process != nil {
		_ = k.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = k.cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(stopTimeout):
			_ = k.cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(stopTimeout):
			}
		}
	}
	_ = os.RemoveAll(k.workdir)
}

// Attach connects to an existing kernel. The client owns no process.
func Attach(binary, listen string, pki OperatorPKI, serverName string) (*Client, error) {
	for _, item := range []struct{ label, path string }{
		{"binary", binary}, {"ca", pki.CA}, {"cert", pki.Cert}, {"key", pki.Key},
	} {
		if item.path == "" {
			return nil, &BootstrapError{Code: "transport", Phase: "connect", Message: item.label + " path required"}
		}
		if _, err := os.Stat(item.path); err != nil {
			return nil, &BootstrapError{Code: "transport", Phase: "connect",
				Message: fmt.Sprintf("%s not found: %s", item.label, item.path)}
		}
	}
	if listen == "" {
		return nil, &BootstrapError{Code: "invalid-message", Phase: "connect", Message: "listen address required"}
	}
	if serverName == "" {
		serverName = "localhost"
	}
	return &Client{binary: binary, listen: listen, pki: pki, serverName: serverName}, nil
}

// Start spawns an owned kernel from a config map. operatorPKI is the
// caller's identity and is required; failures reap everything created.
func Start(binary string, config map[string]any, operatorPKI OperatorPKI, serverName string) (*OwnedKernel, error) {
	if binary == "" {
		return nil, &BootstrapError{Code: "transport", Phase: "spawn", Message: "binary path required"}
	}
	if st, err := os.Stat(binary); err != nil || st.IsDir() || st.Mode().Perm()&0111 == 0 {
		return nil, &BootstrapError{Code: "transport", Phase: "spawn", Message: "binary not executable: " + binary}
	}
	home, _ := config["home"].(string)
	if config == nil || home == "" {
		return nil, &BootstrapError{Code: "invalid-message", Phase: "config", Message: "config map with 'home' required"}
	}
	if operatorPKI.CA == "" || operatorPKI.Cert == "" || operatorPKI.Key == "" {
		return nil, &BootstrapError{Code: "invalid-message", Phase: "config",
			Message: "operator PKI (ca/cert/key) is required: server identity never implies caller authority"}
	}
	for _, item := range []struct{ label, path string }{
		{"ca", operatorPKI.CA}, {"cert", operatorPKI.Cert}, {"key", operatorPKI.Key},
	} {
		if _, err := os.Stat(item.path); err != nil {
			return nil, &BootstrapError{Code: "invalid-message", Phase: "config",
				Message: fmt.Sprintf("operator %s not found: %s", item.label, item.path)}
		}
	}
	workdir, err := os.MkdirTemp("", "mx-go-")
	if err != nil {
		return nil, &BootstrapError{Code: "internal", Phase: "spawn", Message: err.Error()}
	}
	reap := func(cmd *exec.Cmd) {
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = os.RemoveAll(workdir)
	}
	cfgPath := filepath.Join(workdir, "config.json")
	raw, _ := json.Marshal(config)
	if err := os.WriteFile(cfgPath, raw, 0600); err != nil {
		_ = os.RemoveAll(workdir)
		return nil, &BootstrapError{Code: "internal", Phase: "config", Message: err.Error()}
	}
	cmd := exec.Command(binary, "serve", cfgPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = os.RemoveAll(workdir)
		return nil, &BootstrapError{Code: "internal", Phase: "spawn", Message: err.Error()}
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(workdir)
		return nil, &BootstrapError{Code: "transport", Phase: "spawn", Message: err.Error()}
	}
	ready, berr := readReady(cmd, stdout, &stderr)
	if berr != nil {
		reap(cmd)
		return nil, berr
	}
	listen, _ := ready["listen"].(string)
	if listen == "" {
		if tls, ok := config["tls"].(map[string]any); ok {
			listen, _ = tls["listen"].(string)
		}
	}
	if serverName == "" {
		serverName = "localhost"
	}
	client := &Client{binary: binary, listen: listen, pki: operatorPKI, serverName: serverName, owned: true}
	return &OwnedKernel{cmd: cmd, workdir: workdir, client: client,
		Epoch: ready["epoch"], API: fmt.Sprint(ready["api"]), Profile: fmt.Sprint(ready["profile"])}, nil
}

func readReady(cmd *exec.Cmd, stdout io.Reader, stderr *strings.Builder) (map[string]any, error) {
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		br := bufio.NewReader(stdout)
		line, err := br.ReadString('\n')
		ch <- result{line: line, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			_ = cmd.Wait()
			first := firstLine(strings.TrimSpace(stderr.String()))
			if first == "" {
				first = "daemon exited"
			}
			return nil, &BootstrapError{Code: "internal", Phase: "config", Message: "daemon refused config: " + first}
		}
		var ready map[string]any
		if err := json.Unmarshal([]byte(r.line), &ready); err != nil {
			return nil, &BootstrapError{Code: "transport", Phase: "ready", Message: "undecodable ready line"}
		}
		if ok, _ := ready["ready"].(bool); !ok {
			return nil, &BootstrapError{Code: "transport", Phase: "ready", Message: "daemon not ready"}
		}
		if api, _ := ready["api"].(string); api != "" && !strings.HasPrefix(api, ExpectedAPIPrefix) {
			return nil, &BootstrapError{Code: "unsupported-version", Phase: "version",
				Message: "binary api outside " + ExpectedAPIPrefix + "x: " + api}
		}
		return ready, nil
	case <-time.After(readyTimeout):
		return nil, &BootstrapError{Code: "transport", Phase: "ready", Message: "no ready line in budget"}
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func goVersion() string { return runtime.Version() }

func haveBinary(name string) bool {
	for _, dir := range strings.Split(os.Getenv("PATH"), string(os.PathListSeparator)) {
		if dir == "" {
			continue
		}
		if st, err := os.Stat(filepath.Join(dir, name)); err == nil && !st.IsDir() && st.Mode().Perm()&0111 != 0 {
			return true
		}
	}
	return false
}

// DoctorReport is environment diagnosis (no secrets).
type DoctorReport struct {
	Go                 string   `json:"go"`
	Binary             string   `json:"binary"`
	BinaryFound        bool     `json:"binary_found"`
	BinaryExecutable   bool     `json:"binary_executable"`
	CLIShapeOK         bool     `json:"cli_shape_ok"`
	Openssl            bool     `json:"openssl"`
	Bwrap              bool     `json:"bwrap"`
	SocketDirWritable  bool     `json:"socket_dir_writable"`
	Errors             []string `json:"errors"`
}

// Doctor diagnoses the operator environment.
func Doctor(binary string) DoctorReport {
	rep := DoctorReport{Binary: binary, Errors: []string{}}
	rep.Go = goVersion()
	rep.Openssl = haveBinary("openssl")
	rep.Bwrap = haveBinary("bwrap")
	if binary != "" {
		if st, err := os.Stat(binary); err == nil && !st.IsDir() {
			rep.BinaryFound = true
			if st.Mode().Perm()&0111 != 0 {
				rep.BinaryExecutable = true
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				out, _ := exec.CommandContext(ctx, binary).CombinedOutput()
				cancel()
				if strings.Contains(string(out), "matrix-managed serve") {
					rep.CLIShapeOK = true
				} else {
					rep.Errors = append(rep.Errors, "binary does not speak the managed CLI shape")
				}
			} else {
				rep.Errors = append(rep.Errors, "binary not executable")
			}
		} else {
			rep.Errors = append(rep.Errors, "binary not found: set it explicitly or via PATH (no silent download)")
		}
	} else {
		rep.Errors = append(rep.Errors, "binary not found: set it explicitly or via PATH (no silent download)")
	}
	if dir, err := os.MkdirTemp("", "mx-doc-"); err == nil {
		sock := filepath.Join(dir, "t.sock")
		if ln, err := net.Listen("unix", sock); err == nil {
			rep.SocketDirWritable = true
			_ = ln.Close()
		} else {
			rep.Errors = append(rep.Errors, "unix socket probe failed: "+err.Error())
		}
		_ = os.RemoveAll(dir)
	} else {
		rep.Errors = append(rep.Errors, "temp dir probe failed: "+err.Error())
	}
	return rep
}
