#!/usr/bin/env python3
"""Seeded deterministic fuzz-case generator (emits JSON, no live I/O).
Oracle per docs/ML1-NODE.md: release N numeric (deny, never echo);
raw_bytes JSON echoes per otherwise-branch (text-only refusal is frame-level,
matrix-conform vectors cover it: this generator emits echo, never invalid-message);
amplify caps min(N,1MiB): 1048577 input.timeout_ms=15000 NOT honored (manifest
execution.timeout_ms governs); probed stable terminal at 5s+15s budgets is
deny/deadline-exceeded (slow-path, node survives), asserted as such.
Fixed release-string-echoes case pins Rust as_u64 parity (non-numeric release echoes)."""
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
    if k == 2:  # skip text-only refusal (frame-level); emit echo instead
        return {"name": "echo-skip-textonly-%d" % i, "operation": op,
                "cap": CAP, "input": {"hello": "skip-textonly-%d" % i},
                "expect": "ok", "match": '"echo"'}
    if k == 3:  # sleep_ms small (abortable sleep path)
        return {"name": "sleep-%d" % i, "operation": op, "cap": CAP,
                "input": {"sleep_ms": rng.choice([1, 5, 10, 20])},
                "expect": "ok", "match": '"ok"'}
    if k == 4:  # chain:true without binding -> dependency-unavailable
        return {"name": "chain-nobind-%d" % i, "operation": op, "cap": CAP,
                "input": {"chain": True, "input": {"hello": "world"}},
                "expect": "deny", "match": "dependency-unavailable"}
    if k == 5:  # amplify: small ok vs large slow-path deny (see header)
        if rng.randrange(2) == 0:
            return {"name": "amplify-large-%d" % i, "operation": op,
                    "cap": CAP,
                    "input": {"amplify": 1048577, "timeout_ms": 15000},
                    "expect": "deny", "match": "deadline-exceeded"}
        return {"name": "amplify-%d" % i, "operation": op, "cap": CAP,
                "input": {"amplify": rng.choice([16, 1024])},
                "expect": "ok", "match": '"blob"'}
    # acquire timer grants a handle; numeric release denies (never echoes)
    if rng.randrange(2) == 0:
        return {"name": "acquire-%d" % i, "operation": op, "cap": CAP,
                "input": {"acquire": {"kind": "timer", "label": "fz-%d" % i,
                          "interval_ms": 10}}, "expect": "ok",
                "match": '"acquired"'}
    # unknown/foreign numeric handle: permission-denied (or already-released
    # on reuse); string "0" would echo, so only ints here.
    return {"name": "release-%d" % i, "operation": op, "cap": CAP,
            "input": {"release": rng.choice([0, 999999])},
            "expect": "deny", "match": "permission-denied|invalid-message|unknown-handle|already-released|released|foreign|stale"}


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
    cases.append({"name": "release-string-echoes", "operation": "op-%d-fixed-relstr" % a.seed, "cap": CAP, "input": {"release": "0"}, "expect": "ok", "match": '"echo"', "absent": "released"})
    json.dump(cases, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        pass
