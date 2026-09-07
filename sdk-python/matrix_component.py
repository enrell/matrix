"""Python SDK for external components (M2.4, stdlib only).

Speaks ``matrix.component`` v0.1 with the local host: handshake, registration,
activation, and call serving with cooperative cancellation.
Mirrors ``matrix-component`` (Rust): same observable behavior,
without requiring latency equality (C24).
"""

from __future__ import annotations

import json
import os
import queue
import socket
import struct
import threading

PROTOCOL_ID = "matrix.component"
PROTOCOL_VERSION = "0.1"
DEFAULT_MAX_FRAME = 1024 * 1024

_next_id = [1]
_next_lock = threading.Lock()


def _fresh(prefix: str) -> str:
    with _next_lock:
        i = _next_id[0]
        _next_id[0] += 1
    return f"{prefix}-{i}"


def _send_frame(wfile, payload: bytes) -> None:
    wfile.write(struct.pack(">I", len(payload)) + payload)
    wfile.flush()


def _send_envelope(ctx, ty, message_id, body, request_id=None):
    msg = {
        "protocol": PROTOCOL_ID,
        "version": PROTOCOL_VERSION,
        "type": ty,
        "message_id": message_id,
        "session_id": ctx.session_id,
        "instance_id": ctx.instance_id,
        "generation": str(ctx.generation),
        "body": body,
    }
    if request_id is not None:
        msg["request_id"] = request_id
    _send_frame(ctx.wfile, json.dumps(msg).encode("utf-8"))


def _read_frame(rfile, max_frame: int):
    hdr = rfile.read(4)
    if not hdr:
        return None
    if len(hdr) < 4:
        raise IOError("truncated length prefix")
    (ln,) = struct.unpack(">I", hdr)
    if ln == 0 or ln > max_frame:
        raise IOError(f"bad frame length {ln}")
    buf = b""
    while len(buf) < ln:
        chunk = rfile.read(ln - len(buf))
        if not chunk:
            raise IOError("truncated frame")
        buf += chunk
    return buf


class DepError(Exception):
    """Dependency-call error (wire code, no reinterpretation)."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class ResError(Exception):
    """Activation resource error (M6.3)."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class CallCtx:
    """Call context: bound session + streams + dependencies."""

    def __init__(self, comp, ticket: str, cancel: threading.Event, bindings):
        self._comp = comp
        self.ticket = ticket
        self._cancel = cancel
        self._bindings = bindings
        self._waiters = comp.dep_waiters
        self._waiters_lock = comp.dep_waiters_lock
        self._res_waiters = comp.res_waiters
        self._res_waiters_lock = comp.res_waiters_lock
        self._reader_ident = comp.reader_ident

    def event_dropped_count(self) -> int:
        """Events/chunks dropped by dispatcher overflow (slow handlers)."""
        with self._comp.ev_lock:
            return self._comp.ev_dropped

    def pending_stream_count(self) -> int:
        """Stream chunks still queued for ``on_stream`` (credit signal)."""
        with self._comp.ev_lock:
            return sum(1 for item in self._comp.ev_queue if item[0] == "stream")

    def _check_not_reader(self, err):
        if threading.get_ident() == self._reader_ident:
            raise err("internal", "blocking call on reader thread")

    @property
    def session_id(self):
        return self._comp.session_id

    def send_stream(self, stream_id: str, seq: int, payload: str) -> None:
        with self._comp.write_lock:
            _send_envelope(
                self._comp,
                "stream.data",
                f"m-{stream_id}-{seq}",
                {"stream_id": stream_id, "seq": str(seq), "payload": payload},
            )

    def dependencies(self):
        """Opaque handles of this activation for dependency calls."""
        return list(self._bindings)

    def invoke_dependency(self, binding: str, input, timeout_s: float):
        """Invokes a dependency by opaque handle (M6.1). Blocks until
        terminal, inheriting context cancellation. Without local negotiation,
        recusa com ``unsupported-feature`` sem tocar o fio."""
        if "dependency-calls/1" not in self._comp.features:
            raise DepError("unsupported-feature", "dependency calls not negotiated")
        timeout_ms = int(timeout_s * 1000)
        if timeout_ms <= 0:
            raise DepError("invalid-message", "timeout must be positive")
        self._check_not_reader(DepError)
        rid = _fresh("r-dep")
        mid = _fresh("m-dep")
        box: dict = {}
        arrived = threading.Event()
        with self._waiters_lock:
            self._waiters[rid] = (box, arrived)
        try:
            with self._comp.write_lock:
                _send_envelope(
                    self._comp,
                    "dependency.open",
                    mid,
                    {"parent_ticket": self.ticket, "binding_id": binding,
                     "timeout_ms": timeout_ms, "input": input},
                    request_id=rid,
                )
        except OSError as exc:
            with self._waiters_lock:
                self._waiters.pop(rid, None)
            raise DepError("internal", f"send: {exc}")
        # Prazo local = pedido + folga de transporte; estouro cancela no fio.
        deadline = timeout_s + 10.0
        end = arrived.wait(timeout=deadline) if deadline > 0 else False
        # Parent-cancellation inheritance wins over the result.
        if self._cancel.is_set():
            self._cancel_dep(rid)
            with self._waiters_lock:
                self._waiters.pop(rid, None)
            raise DepError("cancelled", "parent cancelled")
        if not end:
            self._cancel_dep(rid)
            with self._waiters_lock:
                self._waiters.pop(rid, None)
            raise DepError("outcome-unknown", "sdk wait timeout")
        with self._waiters_lock:
            self._waiters.pop(rid, None)
        if "error" in box:
            raise DepError(box["error"][0], box["error"][1])
        return box.get("output")

    def _cancel_dep(self, target: str) -> None:
        try:
            with self._comp.write_lock:
                _send_envelope(
                    self._comp,
                    "dependency.cancel",
                    _fresh("m-dep-cancel"),
                    {"target_request_id": target},
                    request_id=_fresh("r-dep-cancel"),
                )
        except OSError:
            pass

    def _resource_roundtrip(self, operation: str, fields: dict):
        self._check_not_reader(ResError)
        body = {"operation_id": _fresh("op-res")}
        body.update(fields)
        rid = _fresh("r-res")
        box: dict = {}
        arrived = threading.Event()
        with self._res_waiters_lock:
            self._res_waiters[rid] = (box, arrived)
        try:
            with self._comp.write_lock:
                _send_envelope(
                    self._comp,
                    f"resource.{operation}",
                    _fresh("m-res"),
                    body,
                    request_id=rid,
                )
        except OSError as exc:
            with self._res_waiters_lock:
                self._res_waiters.pop(rid, None)
            raise ResError("internal", f"send: {exc}")
        ok = arrived.wait(timeout=10.0)
        with self._res_waiters_lock:
            self._res_waiters.pop(rid, None)
        if self._cancel.is_set():
            raise ResError("cancelled", "parent cancelled")
        if not ok:
            raise ResError("outcome-unknown", "resource wait timeout")
        if "error" in box:
            raise ResError(box["error"][0], box["error"][1])
        return {k: v for k, v in box.items() if k not in ("operation_id", "status")}

    def acquire_resource(self, kind: str, label: str, interval_ms=None) -> int:
        """Acquires an activation resource (cap/sub/timer/task)."""
        fields = {"kind": kind, "label": label}
        if interval_ms is not None:
            fields["interval_ms"] = int(interval_ms)
        extra = self._resource_roundtrip("acquire", fields)
        try:
            return int(str(extra.get("handle", "")))
        except (TypeError, ValueError):
            raise ResError("internal", "missing handle")

    def release_resource(self, handle: int) -> None:
        """Releases a handle from `acquire_resource`."""
        self._resource_roundtrip("release", {"handle": str(handle)})


class Handler:
    """Component logic. ``on_call`` runs on one thread per call."""

    def on_call(self, ctx, ticket: str, cap: str, input: dict, cancel: threading.Event):
        raise NotImplementedError

    def on_cancel(self, ticket: str):
        pass

    def on_event(self, topic: str, payload) -> None:
        """Receives bus events for manifest-declared subscriptions (M6.3).
        Runs on the reader thread: observe fast, never block it."""

    def on_stream(self, stream_id: str, seq: int, payload: str) -> None:
        """Receives stream chunks for this activation (M7). Runs on the
        dispatcher thread like ``on_event``: observe fast. The host only
        grants more credit as the queue drains, so slowness throttles
        the sender instead of growing memory."""


class Component:
    """Connected component: negotiated, activated session."""

    def __init__(self, sock, session_id, instance_id, generation, max_frame):
        self.sock = sock
        self.rfile = sock.makefile("rb")
        self.wfile = sock.makefile("wb")
        self.write_lock = threading.Lock()
        self.session_id = session_id
        self.instance_id = instance_id
        self.generation = generation
        self.max_frame = max_frame
        self.calls: dict[str, threading.Event] = {}
        self.calls_lock = threading.Lock()
        self.features: list = []
        self.dep_bindings: list = []
        self.dep_waiters: dict = {}
        self.dep_waiters_lock = threading.Lock()
        self.res_waiters: dict = {}
        self.res_waiters_lock = threading.Lock()
        from collections import deque
        self.ev_queue = deque()
        self.ev_lock = threading.Lock()
        self.ev_dropped = 0
        self.ev_wake: queue.Queue = queue.Queue(maxsize=1)
        self.reader_ident = None

    @property
    def wfile_proxy(self):
        return self

    @classmethod
    def connect(cls, sock_path: str, logical: str) -> "Component":
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(30)
        sock.connect(sock_path)
        comp = cls.__new__(cls)
        comp.sock = sock
        comp.rfile = sock.makefile("rb")
        comp.wfile = sock.makefile("wb")
        comp.write_lock = threading.Lock()
        comp.calls = {}
        comp.calls_lock = threading.Lock()
        comp.dep_waiters = {}
        comp.dep_waiters_lock = threading.Lock()
        comp.res_waiters = {}
        comp.res_waiters_lock = threading.Lock()
        from collections import deque
        comp.ev_queue = deque()
        comp.ev_lock = threading.Lock()
        comp.ev_dropped = 0
        comp.ev_wake: queue.Queue = queue.Queue(maxsize=1)
        comp.reader_ident = None

        def send_raw(msg, max_frame=DEFAULT_MAX_FRAME):
            with comp.write_lock:
                _send_frame(comp.wfile, json.dumps(msg).encode("utf-8"))

        send_raw({
            "protocol": PROTOCOL_ID, "version": PROTOCOL_VERSION,
            "type": "hello", "message_id": "h1",
            "body": {"launch_token": os.environ.get("MATRIX_LAUNCH_TOKEN", ""), "versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME,
                     "client": "matrix-component-py", "features": ["dependency-calls/1"]},
        })
        welcome = json.loads(_read_frame(comp.rfile, DEFAULT_MAX_FRAME))
        assert welcome["type"] == "welcome", welcome
        session_id = welcome["session_id"]
        max_frame = int(welcome["body"].get("max_frame", DEFAULT_MAX_FRAME))
        comp.session_id = session_id
        comp.max_frame = max_frame
        # M6.1 step 1: stores negotiated extensions; legacy session = empty.
        features = (welcome.get("body") or {}).get("features") or []
        comp.features = [f for f in features if isinstance(f, str)]
        send_raw({
            "protocol": PROTOCOL_ID, "version": PROTOCOL_VERSION,
            "type": "component.register", "message_id": "reg1",
            "session_id": session_id,
            "body": {"manifest": {"id": logical}},
        }, max_frame)
        reg = json.loads(_read_frame(comp.rfile, max_frame))
        assert reg["type"] == "registered", reg
        comp.instance_id = reg["instance_id"]
        comp.generation = int(reg["generation"])
        # Aguarda o activate do host e confirma.
        act = json.loads(_read_frame(comp.rfile, max_frame))
        assert act["type"] == "lifecycle.activate", act
        op = (act.get("body") or {}).get("operation_id", "op?")
        # M6.1 step 3: opaque activation bindings for children.
        raw_bindings = (act.get("body") or {}).get("dependency_bindings") or []
        comp.dep_bindings = [
            {"id": b.get("binding_id"), "capability": b.get("capability")}
            for b in raw_bindings
            if isinstance(b, dict) and b.get("binding_id") and b.get("capability")
        ]
        send_raw({
            "protocol": PROTOCOL_ID, "version": PROTOCOL_VERSION,
            "type": "lifecycle.result",
            "message_id": "lc1", "session_id": session_id,
            "instance_id": comp.instance_id,
            "generation": str(comp.generation),
            "request_id": act.get("request_id"),
            "body": {"operation_id": op, "status": "ok", "pending": []},
        }, max_frame)
        return comp

    def _bound_ok(self, env: dict) -> bool:
        return (
            env.get("session_id") == self.session_id
            and str(env.get("instance_id", self.instance_id)) == str(self.instance_id)
            and int(env.get("generation", self.generation)) == int(self.generation)
        )

    def _reply_lifecycle(self, operation_id, request_id) -> None:
        with self.write_lock:
            _send_envelope(
                self, "lifecycle.result",
                f"m-lc-{operation_id}",
                {"operation_id": operation_id, "status": "ok", "pending": []},
                request_id=request_id,
            )

    def _ev_dispatcher(self, handler: Handler) -> None:
        while True:
            try:
                self.ev_wake.get(timeout=0.1)
            except queue.Empty:
                if self._ev_stop.is_set():
                    break
                continue
            while True:
                with self.ev_lock:
                    if not self.ev_queue:
                        break
                    item = self.ev_queue.popleft()
                try:
                    if item[0] == "stream":
                        _, stream_id, seq, payload = item
                        handler.on_stream(stream_id, seq, payload)
                    else:
                        _, topic, payload = item
                        handler.on_event(topic, payload)
                except Exception:
                    pass
            if self._ev_stop.is_set():
                break

    def serve(self, handler: Handler) -> str:
        """Serves until EOF/error, quiesce, or dispose. Returns the exit reason."""
        self.reader_ident = threading.get_ident()
        self._ev_stop = threading.Event()
        ev_thread = threading.Thread(target=self._ev_dispatcher, args=(handler,), daemon=True)
        ev_thread.start()
        try:
            while True:
                try:
                    raw = _read_frame(self.rfile, self.max_frame)
                except (IOError, OSError):
                    return "eof"
                if raw is None:
                    return "eof"
                try:
                    env = json.loads(raw)
                except ValueError:
                    continue
                if not self._bound_ok(env):
                    continue
                ty = env.get("type")
                body = env.get("body") or {}
                request_id = env.get("request_id")
                if ty in ("lifecycle.prepare", "lifecycle.activate", "lifecycle.quiesce"):
                    self._reply_lifecycle(body.get("operation_id", "op?"), request_id)
                elif ty == "lifecycle.dispose":
                    self._reply_lifecycle(body.get("operation_id", "op?"), request_id)
                    return "dispose"
                elif ty == "call.open":
                    ticket = (body.get("ticket") or "")
                    cap = (body.get("capability") or "")
                    call_input = body.get("input")
                    if call_input is None:
                        call_input = {}
                    open_rid = env.get("request_id")
                    cancel = threading.Event()
                    with self.calls_lock:
                        self.calls[ticket] = cancel
                    t = threading.Thread(
                        target=self._run_call,
                        args=(handler, ticket, cap, call_input, cancel, open_rid),
                        daemon=True,
                    )
                    t.start()
                elif ty == "call.cancel":
                    ticket = (body.get("ticket") or "")
                    with self.calls_lock:
                        ev = self.calls.get(ticket)
                    if ev is not None:
                        ev.set()
                    try:
                        handler.on_cancel(ticket)
                    except Exception:
                        pass
                elif ty == "dependency.result":
                    # Terminal de filha: roteia ao waiter do open (M6.1) ou
                    # drops it (late answer without a waiter). `accepted` never answers.
                    if request_id is not None:
                        with self.dep_waiters_lock:
                            waiter = self.dep_waiters.pop(request_id, None)
                        if waiter is not None:
                            box, arrived = waiter
                            status = (body.get("status") or "")
                            if status == "ok":
                                box["output"] = body.get("output")
                            else:
                                err = body.get("error") or {}
                                box["error"] = (err.get("code") or "internal",
                                                err.get("message") or "remote error")
                            arrived.set()
                elif ty == "resource.result":
                    if request_id is not None:
                        with self.res_waiters_lock:
                            waiter = self.res_waiters.pop(request_id, None)
                        if waiter is not None:
                            box, arrived = waiter
                            status = (body.get("status") or "")
                            if status == "ok":
                                for k, v in body.items():
                                    if k not in ("operation_id", "status"):
                                        box[k] = v
                            else:
                                box["error"] = (body.get("code") or "internal",
                                                body.get("message") or "remote error")
                            arrived.set()
                elif ty == "event.deliver":
                    topic = body.get("topic") or ""
                    if topic:
                        with self.ev_lock:
                            if len(self.ev_queue) >= 64:
                                self.ev_queue.popleft()
                                self.ev_dropped += 1
                            self.ev_queue.append(("event", topic, body.get("payload")))
                        try:
                            self.ev_wake.put_nowait(None)
                        except queue.Full:
                            pass
                elif ty == "stream.data":
                    stream_id = body.get("stream_id") or ""
                    try:
                        seq = int(str(body.get("seq") or ""))
                    except (TypeError, ValueError):
                        seq = -1
                    if stream_id and seq >= 0:
                        with self.ev_lock:
                            if len(self.ev_queue) >= 64:
                                self.ev_queue.popleft()
                                self.ev_dropped += 1
                            self.ev_queue.append(("stream", stream_id, seq, str(body.get("payload") or "")))
                        try:
                            self.ev_wake.put_nowait(None)
                        except queue.Full:
                            pass
                # other types: ignored without dropping the session.
        finally:
            try:
                self._ev_stop.set()
            except AttributeError:
                pass
            try:
                ev_thread.join(timeout=5)
            except Exception:
                pass
            try:
                self.sock.close()
            except OSError:
                pass

    def _run_call(self, handler, ticket, cap, input, cancel, request_id):
        ctx = CallCtx(self, ticket, cancel, self.dep_bindings)
        try:
            out = handler.on_call(ctx, ticket, cap, input, cancel)
        except Exception as exc:  # handler errors become business errors
            out = ("error", "internal", f"handler: {exc}")
        finally:
            with self.calls_lock:
                self.calls.pop(ticket, None)
        if cancel.is_set():
            return  # late after cancel: stays silent (no false success).
        if isinstance(out, tuple) and out and out[0] == "error":
            _, code, message = out
            body = {"ticket": ticket, "status": "error",
                    "error": {"code": code, "message": message}}
        else:
            body = {"ticket": ticket, "status": "ok", "output": out}
        with self.write_lock:
            try:
                _send_envelope(self, "call.result", f"m-call-{ticket}", body,
                               request_id=request_id)
            except OSError:
                pass
