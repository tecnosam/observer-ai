#!/usr/bin/env python3
"""Send sample traces from the labeled evaluation dataset to a running engine.

Picks an equal number of traces per label (wrong / partial / correct) from one
split of labeled_traces.jsonl and posts each to the engine. Only the fields
the API accepts are sent; labels and gold answers stay behind.

Usage:
  python3 scripts/send_samples.py                 # 30 held-out traces, queued
  python3 scripts/send_samples.py -n 60 --sync    # wait for each decision
"""
import argparse
import collections
import json
import random
import sys
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_DATASET = (
    Path(__file__).resolve().parents[3] / "finetune/notebooks/data/eval/labeled_traces.jsonl"
)


def to_api_trace(row):
    return {
        "trace_id": row["trace_id"],
        "question": row["question"],
        "tool_calls": [
            {k: c[k] for k in ("name", "args", "status") if k in c} for c in row.get("tool_calls", [])
        ],
        "evidence": row.get("evidence", []),
        "final_answer": row["final_answer"],
    }


def pick(rows, n, rng):
    by_label = collections.defaultdict(list)
    for r in rows:
        by_label[r["label"]].append(r)
    labels = sorted(by_label)
    picked = []
    for i, label in enumerate(labels):
        share = n // len(labels) + (1 if i < n % len(labels) else 0)
        picked += rng.sample(by_label[label], min(share, len(by_label[label])))
    rng.shuffle(picked)
    return picked


def post(url, body):
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    ap.add_argument("--url", default="http://localhost:8080", help="engine base URL")
    ap.add_argument("-n", type=int, default=30, help="number of traces to send")
    ap.add_argument("--split", default="heldout", help="dataset split to sample from ('all' for every row)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--sync", action="store_true", help="use /v1/traces:score and print each decision")
    args = ap.parse_args()

    rows = [json.loads(line) for line in args.dataset.open(encoding="utf-8")]
    if args.split != "all":
        rows = [r for r in rows if r.get("split") == args.split]
    # A trace with no final answer is not a valid request.
    rows = [r for r in rows if r.get("question") and r.get("final_answer")]
    if not rows:
        sys.exit(f"no usable traces in split {args.split!r} of {args.dataset}")

    picked = pick(rows, args.n, random.Random(args.seed))
    endpoint = args.url.rstrip("/") + ("/v1/traces:score" if args.sync else "/v1/traces")
    statuses = collections.Counter()
    for row in picked:
        status, body = post(endpoint, to_api_trace(row))
        statuses[status] += 1
        if args.sync and status == 200:
            p = body["p_keep"]
            print(f"{row['trace_id']}  label={row['label']:<8} {body['decision']:<4} "
                  f"p_keep={'n/a' if p is None else format(p, '.3f')}  {body['reason']}")
        elif status not in (200, 202):
            print(f"{row['trace_id']}  HTTP {status}: {body.get('error', body)}")
    print(f"sent {len(picked)} traces to {endpoint}: "
          + ", ".join(f"{n} x HTTP {s}" for s, n in sorted(statuses.items())))


if __name__ == "__main__":
    main()
