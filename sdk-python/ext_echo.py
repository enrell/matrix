#!/usr/bin/env python3
"""Reference echo on the Python SDK (M2.4).

Usage: ``ext_echo.py --matrix-sock <sock> --id <logical>``
Same behavior as the Rust echo: abortable sleep, remote error, opening
count, cancel mark, stream flood, and clean exit.
"""

import argparse
import sys
import threading
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from matrix_component import CallCtx, Component, Handler


def append_line(path: str, line: str) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


class Echo(Handler):
    def __init__(self):
        self.marks: dict[str, str] = {}
        self.lock = threading.Lock()

    def on_call(self, ctx: CallCtx, ticket: str, cap: str, input: dict, cancel: threading.Event):
        if not isinstance(input, dict):
            input = {}
        cf = input.get("count_file")
        if isinstance(cf, str):
            append_line(cf, ticket)
        mc = input.get("mark_cancel")
        if isinstance(mc, str):
            with self.lock:
                self.marks[ticket] = mc
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
        if isinstance(fail, str):
            with self.lock:
                self.marks.pop(ticket, None)
            return ("error", fail, f"remote {fail}")
        fl = input.get("flood_stream")
        if isinstance(fl, dict):
            fid = fl.get("id", "s1")
            try:
                chunk = min(int(fl.get("chunk", 65536)), 1 << 20)
            except (TypeError, ValueError):
                chunk = 65536
            try:
                count = min(int(fl.get("count", 4)), 64)
            except (TypeError, ValueError):
                count = 4
            payload = "x" * chunk
            for seq in range(count):
                if cancel.is_set():
                    return ("error", "cancelled", "aborted")
                try:
                    ctx.send_stream(fid, seq, payload)
                except OSError:
                    break
        with self.lock:
            self.marks.pop(ticket, None)
        return {"echo": input}

    def on_cancel(self, ticket: str):
        with self.lock:
            path = self.marks.pop(ticket, None)
        if path is not None:
            append_line(path, ticket)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix-sock", required=True)
    ap.add_argument("--id", default="ext-echo")
    args = ap.parse_args()
    try:
        comp = Component.connect(args.matrix_sock, args.id)
    except (OSError, AssertionError, ValueError) as exc:
        print(f"connect: {exc}", file=sys.stderr)
        return 2
    reason = comp.serve(Echo())
    return 0 if reason in ("dispose", "eof") else 1


if __name__ == "__main__":
    raise SystemExit(main())
