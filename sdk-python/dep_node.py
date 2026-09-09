"""Generic composition node (M6.1 step 3+): echo + chained calls.

Usage: ``dep_node.py --matrix-sock <sock> --id <logical>``
Same semantics as ``crates/matrix-host/examples/dep_node.rs``:
Keys match in this order, first wins: ``sleep_ms``, ``fail``,
``amplify``, ``chain_with_streams``, ``chain``, ``acquire``,
``release``, ``stream_send``, otherwise echo.
- ``input.chain``: invokes the first binding's dependency with
  ``input.input`` and answers ``{"chained": ..., "via": id}``;
  child errors become business errors with the same code;
- ``input.sleep_ms``: abortable sleep;
- ``input.fail``: remote business error;
- otherwise ``{"echo": input, "via": id}``.
- ``input.amplify``: answers ``{"blob": "x"*N}`` (bounded test output).
- ``input.stream_send``: ``{stream_id, chunks, chunk_bytes, sleep_ms?}``
  sends bounded stream chunks, answers ``{"stream_sent": N}``.
- ``input.chain_with_streams``: ``{stream_id, chunks, chunk_bytes,
  interval_ms?, prime_ms?, input, timeout_ms?}`` chains while streaming
  concurrently on the same session (M7 bidi legs); ids under ``remote/``
  associate to the in-flight leg, others stay local.
- stream chunks go to ``--stream-log`` as ``stream_id<TAB>seq<TAB>len``;
  ``--stream-slow-ms`` sleeps per chunk (slow-consumer tests).
"""

import argparse
import json
import sys
import threading
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from matrix_component import Component, DepError, Handler, ResError

_U64MAX = (1 << 64) - 1


def _u64(value, default):
    """int in u64 range, else default (mirrors Rust `as_u64().unwrap_or`)."""
    if isinstance(value, bool):
        return default
    if isinstance(value, int) and 0 <= value <= _U64MAX:
        return value
    return default


class Node(Handler):
    def __init__(self, logical: str, event_log=None, stream_log=None, stream_slow_ms: float = 0.0):
        self.logical = logical
        self.event_log = event_log
        self.stream_log = stream_log
        self.stream_slow_ms = stream_slow_ms

    def on_event(self, topic: str, payload) -> None:
        if self.event_log:
            try:
                with open(self.event_log, "a", encoding="utf-8") as f:
                    f.write(f"{topic}\t{json.dumps(payload)}\n")
            except OSError:
                pass

    def on_stream(self, stream_id: str, seq: int, payload: str) -> None:
        if self.stream_slow_ms > 0:
            time.sleep(self.stream_slow_ms)
        if self.stream_log:
            try:
                with open(self.stream_log, "a", encoding="utf-8") as f:
                    f.write(f"{stream_id}\t{seq}\t{len(payload)}\n")
            except OSError:
                pass

    def on_call(self, ctx, ticket: str, cap: str, input: dict, cancel: threading.Event):
        # Branch order mirrors dep_node.rs (first matching key wins):
        # sleep, fail, amplify, chain_with_streams, chain, acquire,
        # release, stream_send, otherwise echo.
        sleep_ms = _u64(input.get("sleep_ms"), 0)
        slept = 0
        while slept < sleep_ms:
            if cancel.is_set():
                return ("error", "cancelled", "aborted")
            time.sleep(0.005)
            slept += 5
        fail = input.get("fail")
        if isinstance(fail, str):
            return ("error", fail, f"remote {fail}")
        amp = input.get("amplify")
        if isinstance(amp, int) and not isinstance(amp, bool):
            return {"blob": "x" * min(amp, 1 << 20), "via": self.logical}
        if "chain_with_streams" in input:
            # Concurrent chain + streams (M7 bidi): stream on a thread
            # while the child leg is in flight on this same session.
            spec = input["chain_with_streams"]
            if not isinstance(spec, dict):
                spec = {}
            stream_id = spec.get("stream_id")
            if not isinstance(stream_id, str):
                stream_id = "s-bidi"
            chunks = min(_u64(spec.get("chunks"), 0), 32)
            nbytes = min(_u64(spec.get("chunk_bytes"), 0), 1024)
            interval_ms = min(_u64(spec.get("interval_ms"), 20), 50)
            prime_ms = min(_u64(spec.get("prime_ms"), 50), 1000)
            payload = "x" * nbytes
            sent_box = [0]

            def _stream():
                if prime_ms > 0:
                    time.sleep(prime_ms / 1000.0)
                for seq in range(chunks):
                    try:
                        ctx.send_stream(stream_id, seq, payload)
                    except (OSError, ValueError):
                        break
                    sent_box[0] += 1
                    if interval_ms > 0:
                        time.sleep(interval_ms / 1000.0)

            streamer = threading.Thread(target=_stream, daemon=True)
            streamer.start()
            bindings = ctx.dependencies()
            if not bindings:
                streamer.join()
                return ("error", "dependency-unavailable", "no binding")
            inner = spec.get("input", {})
            timeout_s = max(_u64(spec.get("timeout_ms"), 8000), 1) / 1000.0
            try:
                out = ctx.invoke_dependency(bindings[0]["id"], inner, timeout_s)
            except DepError as exc:
                streamer.join()
                return ("error", exc.code, exc.message)
            streamer.join()
            return {"chained": out, "via": self.logical, "stream_sent": sent_box[0]}
        if input.get("chain") is True:
            bindings = ctx.dependencies()
            if not bindings:
                return ("error", "dependency-unavailable", "no binding")
            inner = input.get("input", {})
            timeout_ms = max(_u64(input.get("timeout_ms"), 5000), 1)
            try:
                out = ctx.invoke_dependency(bindings[0]["id"], inner, timeout_ms / 1000.0)
            except DepError as exc:
                return ("error", exc.code, exc.message)
            return {"chained": out, "via": self.logical}
        if "acquire" in input:
            acq = input["acquire"]
            if not isinstance(acq, dict):
                acq = {}
            kind = acq.get("kind")
            kind = kind if isinstance(kind, str) else ""
            label = acq.get("label")
            label = label if isinstance(label, str) else ""
            try:
                handle = ctx.acquire_resource(kind, label, _u64(acq.get("interval_ms"), None))
            except ResError as exc:
                return ("error", exc.code, exc.message)
            return {"acquired": {"handle": str(handle)}, "via": self.logical}
        rel = input.get("release")
        if (isinstance(rel, int) and not isinstance(rel, bool) and 0 <= rel <= _U64MAX):
            try:
                ctx.release_resource(rel)
            except ResError as exc:
                return ("error", exc.code, exc.message)
            return {"released": str(rel), "via": self.logical}
        if "stream_send" in input:
            spec = input["stream_send"]
            if not isinstance(spec, dict):
                spec = {}
            stream_id = spec.get("stream_id")
            if not isinstance(stream_id, str):
                stream_id = "s-test"
            chunks = min(_u64(spec.get("chunks"), 0), 256)
            nbytes = min(_u64(spec.get("chunk_bytes"), 0), 4096)
            sleep_ms = _u64(spec.get("sleep_ms"), 0)
            payload = "x" * nbytes
            sent = 0
            for seq in range(chunks):
                if cancel.is_set():
                    return ("error", "cancelled", "aborted")
                try:
                    ctx.send_stream(stream_id, seq, payload)
                except (OSError, ValueError) as exc:
                    return ("error", "stream-refused", str(exc))
                sent += 1
                if sleep_ms > 0:
                    time.sleep(min(sleep_ms, 50) / 1000.0)
            return {"stream_sent": sent, "via": self.logical}
        return {"echo": input, "via": self.logical}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix-sock", required=True)
    ap.add_argument("--id", default="dep-node")
    ap.add_argument("--event-log", default=None)
    ap.add_argument("--stream-log", default=None)
    ap.add_argument("--stream-slow-ms", type=float, default=0.0)
    args = ap.parse_args()
    try:
        comp = Component.connect(args.matrix_sock, args.id)
    except (AssertionError, OSError, ValueError) as exc:
        print(f"connect: {exc}", file=sys.stderr)
        return 2
    reason = comp.serve(Node(args.id, args.event_log, args.stream_log, args.stream_slow_ms / 1000.0))
    if reason in ("dispose", "eof"):
        return 0
    print(f"serve: {reason}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
