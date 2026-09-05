"""SDK Python para componentes externos (M2.4, stdlib apenas).

Fala ``matrix.component`` v0.1 com o host local: handshake, registro,
ativação e atendimento de chamadas com cancelamento cooperativo.
Espelha ``matrix-component`` (Rust): mesmo comportamento observável,
sem exigir igualdade de latência (C24).
"""

from __future__ import annotations

import json
import os
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


class CallCtx:
    """Contexto de uma chamada: sessão amarrada + envio de streams."""

    def __init__(self, comp, ticket: str):
        self._comp = comp
        self.ticket = ticket

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


class Handler:
    """Lógica do componente. ``on_call`` roda numa thread por chamada."""

    def on_call(self, ctx, ticket: str, cap: str, input: dict, cancel: threading.Event):
        raise NotImplementedError

    def on_cancel(self, ticket: str):
        pass


class Component:
    """Componente conectado: sessão negociada e ativada."""

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

        def send_raw(msg, max_frame=DEFAULT_MAX_FRAME):
            with comp.write_lock:
                _send_frame(comp.wfile, json.dumps(msg).encode("utf-8"))

        send_raw({
            "protocol": PROTOCOL_ID, "version": PROTOCOL_VERSION,
            "type": "hello", "message_id": "h1",
            "body": {"launch_token": os.environ.get("MATRIX_LAUNCH_TOKEN", ""), "versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME,
                     "client": "matrix-component-py"},
        })
        welcome = json.loads(_read_frame(comp.rfile, DEFAULT_MAX_FRAME))
        assert welcome["type"] == "welcome", welcome
        session_id = welcome["session_id"]
        max_frame = int(welcome["body"].get("max_frame", DEFAULT_MAX_FRAME))
        comp.session_id = session_id
        comp.max_frame = max_frame
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

    def serve(self, handler: Handler) -> str:
        """Atende até EOF/erro, quiesce ou dispose. Retorna motivo de saída."""
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
                # demais tipos: ignorados sem derrubar a sessão.
        finally:
            try:
                self.sock.close()
            except OSError:
                pass

    def _run_call(self, handler, ticket, cap, input, cancel, request_id):
        ctx = CallCtx(self, ticket)
        try:
            out = handler.on_call(ctx, ticket, cap, input, cancel)
        except Exception as exc:  # erro do handler vira erro de negócio
            out = ("error", "internal", f"handler: {exc}")
        finally:
            with self.calls_lock:
                self.calls.pop(ticket, None)
        if cancel.is_set():
            return  # tardio após cancel: cala (sem sucesso falso).
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
