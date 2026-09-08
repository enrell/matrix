"""Operator/application surface for the Matrix kernel (ML1, stdlib only).

The component side stays in ``matrix_component`` (wire protocol
``matrix.component/0.1``, unchanged). This module is the *other*
surface: an application starts its own managed kernel or attaches to a
shared one, then operates it through the public admin path.

Transport decision (ML1, documented in ``docs/ML1-MATRIX.md``): operator
calls go through the staged ``matrix-managed`` binary (``serve`` to own
a kernel, ``request`` for authenticated admin actions over mutual TLS).
The SDK never reimplements mTLS per language and never touches kernel
internals: authority, leases, fences and generations keep working
exactly as the CLI contract defines. ``start`` owns its process;
``connect`` only attaches — closing an attached client never stops a
shared kernel, and failed bootstrap reaps only what it created.

Secrets: lease tokens travel to the CLI via local process argv
(same-uid visibility, like direct CLI use). The SDK never logs tokens,
requests or responses; ``doctor()`` redacts them.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

EXPECTED_API_PREFIX = "0.1."
READY_TIMEOUT_S = 30.0
STOP_TIMEOUT_S = 5.0
REQUEST_SLACK_S = 10.0


class MatrixError(Exception):
    """Structured error: stable ``code``, ``phase`` and safe message."""

    def __init__(self, code: str, phase: str, message: str):
        super().__init__(f"{code} [{phase}]: {message}")
        self.code = code
        self.phase = phase
        self.message = message


class BootstrapError(MatrixError):
    """``start``/``connect`` failure. Nothing owned is left behind."""


class OperatorError(MatrixError):
    """Admin action refused or failed. Wire codes pass through
    untouched (``permission-denied``, ``stale-generation``,
    ``outcome-unknown``, ...); transport/decoding failures use
    ``transport``/``internal``. A timeout reports ``outcome-unknown``
    and never retries implicitly."""


_KNOWN_CODES = (
    "permission-denied", "stale-generation", "outcome-unknown",
    "unauthenticated", "invalid-message", "unsupported-version",
    "dependency-unavailable", "ambiguous-provider",
    "context-not-active", "resource-exhausted", "deadline-exceeded",
    "cancelled", "cleanup-pending", "internal",
)


def _guess_code(stderr: str) -> str:
    low = stderr.lower()
    for code in _KNOWN_CODES:
        if code in low:
            return code
    return "transport" if stderr.strip() else "internal"


def _redact(obj):
    if isinstance(obj, dict):
        return {k: ("<redacted>" if k in ("lease", "launch_token", "token") else _redact(v))
                for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


class Client:
    """Attached operator client (shared or owned transport).

    ``owned=False`` (from ``connect``): ``close()`` only marks the
    handle closed, never stops the daemon. ``owned=True`` handles are
    held by ``OwnedKernel``; close the kernel, not the client.
    """

    def __init__(self, binary, listen, ca, cert, key, server_name="localhost",
                 owned=False):
        self._binary = binary
        self._listen = listen
        self._pki = (ca, cert, key)
        self._server_name = server_name
        self._owned = owned
        self._closed = False

    def _check_open(self):
        if self._closed:
            raise MatrixError("internal", "client", "client is closed")

    def request(self, action: dict, timeout_s: float = 30.0):
        """One authenticated admin action. Returns the decoded JSON
        response; refusals raise ``OperatorError`` with the wire code."""
        self._check_open()
        if not isinstance(action, dict) or not action.get("action"):
            raise OperatorError("invalid-message", "request", "action dict with 'action' required")
        ca, cert, key = self._pki
        cmd = [self._binary, "request", ca, cert, key,
               self._listen, self._server_name, json.dumps(action)]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=timeout_s + REQUEST_SLACK_S)
        except FileNotFoundError:
            raise OperatorError("transport", "request", f"binary not found: {self._binary}")
        except subprocess.TimeoutExpired:
            raise OperatorError("outcome-unknown", "request",
                                f"admin action timed out after {timeout_s}s (not retried)")
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            raise OperatorError(_guess_code(err), "request",
                                err.splitlines()[0] if err else "request refused")
        try:
            return json.loads(proc.stdout)
        except ValueError as exc:
            raise OperatorError("internal", "request", f"undecodable response: {exc}")

    def activate(self, component: str, ttl_ms: int = 20000, timeout_s: float = 30.0):
        return self.request({"action": "activate", "component": component,
                             "ttl_ms": int(ttl_ms)}, timeout_s)

    def status(self, lease: str, fence, timeout_s: float = 30.0):
        return self.request({"action": "status", "lease": lease,
                             "fence": str(fence)}, timeout_s)

    def invoke(self, lease: str, fence, operation: str, cap: str, input,
               timeout_s: float = 30.0):
        return self.request({"action": "invoke", "lease": lease,
                             "fence": str(fence), "operation": operation,
                             "cap": cap, "input": input}, timeout_s)

    def release(self, lease: str, fence, timeout_s: float = 30.0):
        return self.request({"action": "release", "lease": lease,
                             "fence": str(fence)}, timeout_s)

    def renew(self, lease: str, fence, ttl_ms: int = 20000, timeout_s: float = 30.0):
        return self.request({"action": "renew", "lease": lease,
                             "fence": str(fence), "ttl_ms": int(ttl_ms)}, timeout_s)

    def wait_ready(self, lease: str, fence, timeout_s: float = 20.0):
        """Polls ``status`` until the session reports ready (or the
        budget runs out). Refusals propagate; exhaustion is explicit."""
        end = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < end:
            last = self.status(lease, fence, timeout_s=5.0)
            if isinstance(last, dict) and last.get("ready") is True:
                return last
            time.sleep(0.1)
        raise OperatorError("outcome-unknown", "wait_ready",
                            f"session not ready within {timeout_s}s (last={json.dumps(_redact(last))})")

    def close(self):
        """Marks this handle closed. Never stops any daemon: an
        attached client owns no process."""
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class OwnedKernel:
    """A kernel this application started and owns. ``close()`` is
    idempotent and reaps exactly the spawned daemon plus the private
    directory it created (nothing shared)."""

    def __init__(self, binary, proc, workdir, client, epoch, api, profile):
        self._binary = binary
        self._proc = proc
        self._workdir = workdir
        self._client = client
        self._closed = False
        self.epoch = epoch
        self.api = api
        self.profile = profile

    @property
    def client(self):
        return self._client

    @property
    def listen(self):
        return self._client._listen

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._client.close()
        finally:
            if self._proc.poll() is None:
                try:
                    self._proc.send_signal(signal.SIGTERM)
                    self._proc.wait(timeout=STOP_TIMEOUT_S)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass
                    try:
                        self._proc.wait(timeout=STOP_TIMEOUT_S)
                    except Exception:
                        pass
            shutil.rmtree(self._workdir, ignore_errors=True)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def connect(binary, listen, ca, cert, key, server_name="localhost"):
    """Attaches to an existing kernel. The returned client owns no
    process: ``close()`` never shuts the daemon down."""
    for label, path in (("binary", binary), ("ca", ca), ("cert", cert), ("key", key)):
        if not path or not os.path.exists(path):
            raise BootstrapError("transport", "connect", f"{label} not found: {path}")
    if not listen:
        raise BootstrapError("invalid-message", "connect", "listen address required")
    return Client(binary, listen, ca, cert, key, server_name, owned=False)


def start(binary, config: dict, operator_pki: dict | None = None,
          server_name: str = "localhost"):
    """Starts an owned kernel from a config dict. Fails before partial
    operation: bad configs are refused by the daemon pre-mutation and
    surfaced here with phase ``config``; any bootstrap failure reaps
    the spawned process and removes the private directory.

    ``operator_pki`` (``ca``/``cert``/``key``) is the *caller's*
    identity and is required: it is never derived from the server's
    ``tls`` block (server identity is not caller authority)."""
    if not binary or not os.access(binary, os.X_OK):
        raise BootstrapError("transport", "spawn", f"binary not executable: {binary}")
    if not isinstance(config, dict) or not config.get("home"):
        raise BootstrapError("invalid-message", "config",
                             "config dict with 'home' required")
    workdir = tempfile.mkdtemp(prefix="mx-py-")
    proc = None
    try:
        cfg_path = os.path.join(workdir, "config.json")
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump(config, f)
        proc = subprocess.Popen(
            [binary, "serve", cfg_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        line = _read_ready_line(proc)
        ready = _parse_ready(line, proc)
        listen = ready.get("listen") or _listen_from_config(config)
        if not operator_pki or not all(operator_pki.get(k) for k in ("ca", "cert", "key")):
            raise BootstrapError("invalid-message", "config",
                                 "operator PKI (ca/cert/key) is required: server "
                                 "identity never implies caller authority")
        for label in ("ca", "cert", "key"):
            if not os.path.exists(operator_pki[label]):
                raise BootstrapError("invalid-message", "config",
                                     f"operator {label} not found: {operator_pki[label]}")
        client = Client(binary, listen, operator_pki["ca"], operator_pki["cert"],
                        operator_pki["key"], server_name, owned=True)
        return OwnedKernel(binary, proc, workdir, client,
                           epoch=ready.get("epoch"), api=ready.get("api"),
                           profile=ready.get("profile"))
    except Exception:
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=STOP_TIMEOUT_S)
            except Exception:
                pass
        shutil.rmtree(workdir, ignore_errors=True)
        raise


def _read_ready_line(proc) -> str:
    end = time.monotonic() + READY_TIMEOUT_S
    while time.monotonic() < end:
        if proc.poll() is not None:
            err = ""
            try:
                err = (proc.stderr.read() or "").strip()
            except Exception:
                pass
            first = err.splitlines()[0] if err else f"exit {proc.returncode}"
            raise BootstrapError("internal", "config", f"daemon refused config: {first}")
        import select
        try:
            ready_fds, _, _ = select.select([proc.stdout], [], [], 0.2)
        except Exception:
            ready_fds = []
        if ready_fds:
            line = proc.stdout.readline()
            if line:
                return line
    raise BootstrapError("transport", "ready",
                         f"no ready line within {READY_TIMEOUT_S}s")


def _parse_ready(line: str, proc) -> dict:
    try:
        ready = json.loads(line)
    except ValueError:
        raise BootstrapError("transport", "ready", f"undecodable ready line: {line[:120]}")
    if not isinstance(ready, dict) or ready.get("ready") is not True:
        raise BootstrapError("transport", "ready", f"daemon not ready: {line[:160]}")
    api = str(ready.get("api", ""))
    if api and not api.startswith(EXPECTED_API_PREFIX):
        raise BootstrapError("unsupported-version", "version",
                             f"binary api {api!r} outside {EXPECTED_API_PREFIX}x")
    return ready


def _listen_from_config(config: dict):
    tls = config.get("tls") or {}
    return tls.get("listen", "")


def doctor(binary: str | None = None) -> dict:
    """Environment diagnosis: binary found/executable, CLI shape,
    PKI tooling, isolation prerequisites, socket-dir writability.
    Never prints or returns secrets; safe to paste into a report."""
    report: dict = {
        "python": sys.version.split()[0],
        "binary": binary or "",
        "binary_found": False,
        "binary_executable": False,
        "cli_shape_ok": False,
        "openssl": shutil.which("openssl") is not None,
        "bwrap": shutil.which("bwrap") is not None,
        "socket_dir_writable": False,
        "errors": [],
    }
    if binary and os.path.exists(binary):
        report["binary_found"] = True
        if os.access(binary, os.X_OK):
            report["binary_executable"] = True
            try:
                proc = subprocess.run([binary], capture_output=True, text=True, timeout=10)
                out = (proc.stderr or "") + (proc.stdout or "")
                if "matrix-managed serve" in out:
                    report["cli_shape_ok"] = True
                else:
                    report["errors"].append("binary does not speak the managed CLI shape")
            except Exception as exc:
                report["errors"].append(f"binary probe failed: {exc}")
        else:
            report["errors"].append("binary not executable")
    else:
        report["errors"].append("binary not found: set it explicitly or via PATH (no silent download)")
    try:
        probe = tempfile.mkdtemp(prefix="mx-doc-")
        test_sock = os.path.join(probe, "t.sock")
        import socket as _s
        s = _s.socket(_s.AF_UNIX, _s.SOCK_STREAM)
        s.bind(test_sock)
        s.close()
        report["socket_dir_writable"] = True
        shutil.rmtree(probe, ignore_errors=True)
    except OSError as exc:
        report["errors"].append(f"unix socket probe failed: {exc}")
    return report


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="matrix-doctor",
                                 description="Diagnose the Matrix operator environment (no secrets printed).")
    ap.add_argument("--binary", default=None, help="matrix-managed binary path")
    args = ap.parse_args()
    rep = doctor(args.binary)
    print(json.dumps(rep, indent=2))
    raise SystemExit(0 if (rep["cli_shape_ok"]) else 1)
