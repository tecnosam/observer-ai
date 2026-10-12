#!/usr/bin/env python3
"""Send sample traces from the labeled evaluation dataset to a running engine.

Picks an equal number of traces per label (wrong / partial / correct) from one
split of labeled_traces.jsonl and posts each to the engine. Only the fields
the API accepts are sent; labels and gold answers stay behind.

With --otlp the same traces are sent as OpenTelemetry spans instead (an AGENT
root span and one TOOL span per tool call, OTLP/JSON over HTTP), the way an
instrumented agent would export them.

Usage:
  python3 scripts/send_samples.py                 # 30 held-out traces, queued
  python3 scripts/send_samples.py -n 60 --sync    # wait for each decision
  python3 scripts/send_samples.py --otlp          # as spans to the OTLP receiver
"""
import argparse
import collections
import json
import random
import re
import secrets
import sys
import time
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


def _attr(key, value):
    return {"key": key, "value": {"stringValue": value}}


def to_otlp_request(row):
    """One trace as an OTLP/JSON export request.

    Evidence and tool statuses go in the observer.* override attributes, so
    the engine builds exactly the slice it builds for the JSON form.
    """
    trace_id = row["trace_id"] if re.fullmatch(r"[0-9a-f]{32}", row["trace_id"]) else secrets.token_hex(16)
    root_id = secrets.token_hex(8)
    start = time.time_ns()
    calls = row.get("tool_calls", [])

    root_attrs = [
        _attr("openinference.span.kind", "AGENT"),
        _attr("input.value", row["question"]),
        _attr("output.value", row["final_answer"]),
    ]
    root_attrs += [_attr(f"observer.evidence.{i}", e) for i, e in enumerate(row.get("evidence", []))]
    spans = [{
        "traceId": trace_id, "spanId": root_id, "name": "observer_agent.run", "kind": 1,
        "startTimeUnixNano": str(start), "endTimeUnixNano": str(start + (len(calls) + 1) * 1_000_000),
        "attributes": root_attrs,
    }]
    for i, call in enumerate(calls):
        attrs = [
            _attr("openinference.span.kind", "TOOL"),
            _attr("tool.name", call["name"]),
            _attr("observer.tool.status", str(call.get("status", "ok"))),
        ]
        if "args" in call:
            attrs.append(_attr("input.value", json.dumps(call["args"])))
        spans.append({
            "traceId": trace_id, "spanId": secrets.token_hex(8), "parentSpanId": root_id,
            "name": f"execute_tool {call['name']}", "kind": 1,
            "startTimeUnixNano": str(start + (i + 1) * 1_000_000),
            "endTimeUnixNano": str(start + (i + 1) * 1_000_000 + 500_000),
            "attributes": attrs,
        })
    return {"resourceSpans": [{
        "resource": {"attributes": [
            _attr("service.name", "send_samples"),
            _attr("openinference.project.name", "send_samples"),
        ]},
        "scopeSpans": [{"scope": {"name": "send_samples"}, "spans": spans}],
    }]}


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
    ap.add_argument("--otlp", action="store_true", help="send as spans to the OTLP/HTTP receiver")
    ap.add_argument("--otlp-url", default="http://localhost:4318", help="OTLP/HTTP receiver base URL")
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

    if args.otlp and args.sync:
        sys.exit("--otlp and --sync cannot be combined: the OTLP receiver has no synchronous path")

    picked = pick(rows, args.n, random.Random(args.seed))
    if args.otlp:
        endpoint = args.otlp_url.rstrip("/") + "/v1/traces"
    else:
        endpoint = args.url.rstrip("/") + ("/v1/traces:score" if args.sync else "/v1/traces")
    statuses = collections.Counter()
    for row in picked:
        status, body = post(endpoint, to_otlp_request(row) if args.otlp else to_api_trace(row))
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
