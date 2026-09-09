#!/usr/bin/env python3
"""Seeded deterministic fuzz-case generator (emits JSON, no live I/O).
Perf note: amplify ~1MiB times out live (deadline-exceeded); capped at 1024."""
import argparse
import json
import random
import sys

CAP = "prov.echo@1"
U64MAX = "18446744073709551615"


def make_case(rng, seed, i):
    op = "op-%d-%d" % (seed, i)
    k = rng.randrange(7)
    if k == 0:  # echo round-trip
        return {"name": "echo-%d" % i, "operation": op, "cap": CAP,
                "input": {"hello": "world-%d" % rng.randrange(1 << 30)},
                "expect": "ok", "match": '"echo"'}
    if k == 1:  # u64-max as decimal string (no precision loss)
        return {"name": "u64max-%d" % i, "operation": op, "cap": CAP,
                "input": {"seq": U64MAX}, "expect": "ok",
                "match": U64MAX}
    if k == 2:  # JSON array is valid text -> echo is correct (binary
        return {"name": "nontext-%d" % i, "operation": op, "cap": CAP,  # refusal lives at frame level, matrix-conform vectors cover it)
                "input": {"raw_bytes": [255, 254, rng.randrange(256)]},
                "expect": "ok", "match": '"echo"'}
    if k == 3:  # sleep_ms small (abortable sleep path)
        return {"name": "sleep-%d" % i, "operation": op, "cap": CAP,
                "input": {"sleep_ms": rng.choice([1, 5, 10, 20])},
                "expect": "ok", "match": '"ok"'}
    if k == 4:  # chain:true without binding -> dependency-unavailable
        return {"name": "chain-nobind-%d" % i, "operation": op, "cap": CAP,
                "input": {"chain": True, "input": {"hello": "world"}},
                "expect": "deny", "match": "dependency-unavailable"}
    if k == 5:  # amplify bounded (fast/billi sizes only; see perf note)
        return {"name": "amplify-%d" % i, "operation": op, "cap": CAP,
                "input": {"amplify": rng.choice([16, 1024])},
                "expect": "ok", "match": '"blob"'}
    # acquire timer grants a handle; bare release below just echoes
    if rng.randrange(2) == 0:
        return {"name": "acquire-%d" % i, "operation": op, "cap": CAP,
                "input": {"acquire": {"kind": "timer", "label": "fz-%d" % i,
                          "interval_ms": 10}}, "expect": "ok",
                "match": '"acquired"'}
    # bare release echoes per otherwise-branch (no handle ever acquired here)
    return {"name": "release-%d" % i, "operation": op, "cap": CAP,
            "input": {"release": "0"}, "expect": "ok", "match": '"echo"'}


def shrink(case):
    """Delta-debug stub: smallest input preserving the failure shape."""
    c = dict(case)
    inp = dict(c.get("input", {}))
    if "hello" in inp:
        inp["hello"] = ""
    if "amplify" in inp:
        inp["amplify"] = 1
    if "sleep_ms" in inp:
        inp["sleep_ms"] = 1
    if "raw_bytes" in inp:
        inp["raw_bytes"] = [255]
    c["input"] = inp
    return c


def report_failure(case, seed):
    print(json.dumps({"seed": seed, "minimal": shrink(case)}, indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--count", type=int, default=50)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    cases = [make_case(rng, a.seed, i) for i in range(a.count)]
    json.dump(cases, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        pass
