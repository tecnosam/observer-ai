#!/usr/bin/env python3
"""Write a large randomized slice corpus for TestBuildStateFuzzGolden.

Each output line is {"trace": {...}, "state": "<python build_state>"}. The
corpus covers every Unicode code point (to catch differences between Python's
and Go's idea of "printable"), random float bit patterns and random nested
args. It is not checked in; `make fuzz-golden` generates it and runs the test.

Usage: python3 testdata/gen_fuzz.py OUT.jsonl [SEED]
"""
import json
import random
import struct
import sys

from gen_golden import build_state


def all_code_points():
    return [chr(c) for c in range(0x110000) if not 0xD800 <= c <= 0xDFFF]


def rand_float(rng):
    while True:
        f = struct.unpack("<d", struct.pack("<Q", rng.getrandbits(64)))[0]
        if f == f and f not in (float("inf"), float("-inf")):
            return f


def rand_text(rng, pool, n):
    return "".join(rng.choice(pool) for _ in range(n))


def rand_value(rng, pool, depth=0):
    kind = rng.randrange(9 if depth < 4 else 6)
    if kind == 0:
        return rand_text(rng, pool, rng.randrange(0, 12))
    if kind == 1:
        return rng.randrange(-10**rng.randrange(1, 25), 10**rng.randrange(1, 25))
    if kind == 2:
        return rand_float(rng)
    if kind == 3:
        return rng.choice([10.0**e for e in range(-8, 20)]) * rng.choice([1, -1, 1.5, 123.456])
    if kind == 4:
        return rng.choice([True, False, None])
    if kind == 5:
        return rng.choice(["it's", 'say "hi"', "'\"", "\\", "", "a\nb", "\t\r\x00\x7f"])
    if kind in (6, 7):
        return {rand_text(rng, pool, rng.randrange(0, 6)): rand_value(rng, pool, depth + 1)
                for _ in range(rng.randrange(0, 4))}
    return [rand_value(rng, pool, depth + 1) for _ in range(rng.randrange(0, 4))]


def main():
    out = sys.argv[1]
    rng = random.Random(int(sys.argv[2]) if len(sys.argv) > 2 else 0)
    points = all_code_points()
    ascii_pool = [chr(c) for c in range(128)]
    mixed_pool = ascii_pool * 40 + rng.sample(points, 4000)
    traces = []

    # Every code point, once as tool args (repr path) and once as evidence.
    for i in range(0, len(points), 1000):
        chunk = "".join(points[i:i + 1000])
        traces.append({
            "question": "q", "final_answer": "a",
            "tool_calls": [{"name": "t", "args": chunk}, {"name": "t", "args": {chunk[:50]: chunk[50:100]}}],
            "evidence": [chunk + chunk],
        })

    for _ in range(3000):
        t = {"question": rand_text(rng, mixed_pool, rng.randrange(1, 40)) or "q",
             "final_answer": rand_text(rng, mixed_pool, rng.randrange(1, 40)) or "a"}
        if rng.random() < 0.9:
            calls = []
            for _ in range(rng.randrange(0, 4)):
                c = {"name": rand_text(rng, mixed_pool, rng.randrange(1, 8)) or "t"}
                if rng.random() < 0.9:
                    c["args"] = rand_value(rng, mixed_pool)
                if rng.random() < 0.7:
                    c["status"] = rng.choice(["ok", "error", "", None, 500, True, rand_text(rng, mixed_pool, 5)])
                calls.append(c)
            t["tool_calls"] = calls
        if rng.random() < 0.9:
            t["evidence"] = [rand_text(rng, mixed_pool, rng.choice([0, 5, 1499, 1500, 1501, 1700]))
                             for _ in range(rng.randrange(0, 6))]
        traces.append(t)

    with open(out, "w", encoding="ascii") as f:
        for t in traces:
            f.write(json.dumps({"trace": t, "state": build_state(t)}) + "\n")
    print(f"wrote {len(traces)} cases to {out}")


if __name__ == "__main__":
    main()
