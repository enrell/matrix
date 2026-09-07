"""Generic composition node (M6.1 step 3+): echo + chained calls.

Usage: ``dep_node.py --matrix-sock <sock> --id <logical>``
Same semantics as ``crates/matrix-host/examples/dep_node.rs``:
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
        sleep_ms = input.get("sleep_ms", 0)
        try:
            sleep_ms = int(sleep_ms)
        except (TypeError, ValueError):
            sleep_ms = 0
        slept = 0
        while slept < sleep_ms:
            if cancel.is_set():
                return ("error", "cancelled", "aborted")
            time.sleep(0.005)
            slept += 5
        fail = input.get("fail")
        if isinstance(fail, str) and fail:
            return ("error", fail, f"remote {fail}")
        if isinstance(input.get("amplify"), int):
            n = min(max(int(input["amplify"]), 0), 1 << 20)
            return {"blob": "x" * n, "via": self.logical}
        if input.get("chain"):
            bindings = ctx.dependencies()
            if not bindings:
                return ("error", "dependency-unavailable", "no binding")
            inner = input.get("input") or {}
            timeout_ms = input.get("timeout_ms", 5000)
            try:
                timeout_ms = int(timeout_ms)
            except (TypeError, ValueError):
                timeout_ms = 5000
            try:
                out = ctx.invoke_dependency(bindings[0]["id"], inner, max(timeout_ms, 1) / 1000.0)
            except DepError as exc:
                return ("error", exc.code, exc.message)
            return {"chained": out, "via": self.logical}
        if isinstance(input.get("acquire"), dict):
            acq = input["acquire"]
            try:
                ms = acq.get("interval_ms")
                handle = ctx.acquire_resource(
                    str(acq.get("kind", "")), str(acq.get("label", "")),
                    int(ms) if ms is not None else None)
            except ResError as exc:
                return ("error", exc.code, exc.message)
            except (TypeError, ValueError) as exc:
                return ("error", "invalid-message", f"bad acquire: {exc}")
            return {"acquired": {"handle": str(handle)}, "via": self.logical}
        if "release" in input:
            try:
                ctx.release_resource(int(input["release"]))
            except ResError as exc:
                return ("error", exc.code, exc.message)
            except (TypeError, ValueError) as exc:
                return ("error", "invalid-message", f"bad release: {exc}")
            return {"released": str(input["release"]), "via": self.logical}
        if isinstance(input.get("stream_send"), dict):
            spec = input["stream_send"]
            stream_id = str(spec.get("stream_id", "s-test"))
            try:
                chunks = min(max(int(spec.get("chunks", 0)), 0), 256)
                nbytes = min(max(int(spec.get("chunk_bytes", 0)), 0), 4096)
                sleep_ms = max(int(spec.get("sleep_ms", 0) or 0), 0)
            except (TypeError, ValueError):
                return ("error", "invalid-message", "bad stream_send")
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
        if isinstance(input.get("chain_with_streams"), dict):
            # Concurrent chain + streams (M7 bidi): stream on a thread
            # while the child leg is in flight on this same session.
            spec = input["chain_with_streams"]
            stream_id = str(spec.get("stream_id", "s-bidi"))
            try:
                chunks = min(max(int(spec.get("chunks", 0)), 0), 32)
                nbytes = min(max(int(spec.get("chunk_bytes", 0)), 0), 1024)
                interval_ms = min(max(int(spec.get("interval_ms", 20) or 20), 0), 50)
                prime_ms = min(max(int(spec.get("prime_ms", 50) or 50), 0), 1000)
            except (TypeError, ValueError):
                return ("error", "invalid-message", "bad chain_with_streams")
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
            if not isinstance(inner, dict):
                inner = {}
            timeout_s = spec.get("timeout_ms", 8000)
            try:
                timeout_s = max(float(timeout_s), 1.0) / 1000.0
            except (TypeError, ValueError):
                timeout_s = 8.0
            try:
                out = ctx.invoke_dependency(bindings[0]["id"], inner, timeout_s)
            except DepError as exc:
                streamer.join()
                return ("error", exc.code, exc.message)
            streamer.join()
            return {"chained": out, "via": self.logical, "stream_sent": sent_box[0]}
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
    except (AssertionError, OSError) as exc:
        print(f"connect: {exc}", file=sys.stderr)
        return 2
    comp.serve(Node(args.id, args.event_log, args.stream_log, args.stream_slow_ms / 1000.0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
