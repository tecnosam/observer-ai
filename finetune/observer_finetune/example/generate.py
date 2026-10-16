"""Generate a small synthetic eval set in the shape observer-finetune reads.

    uv run python example/generate.py            # writes example/traces.jsonl

Every trace is a short agent run over a made-up world (countries, capitals,
rivers, founders), so the labels are right by construction: the answer is
correct when it matches the facts, partial when it names only part of them,
and wrong otherwise. Seeded, so every run writes the same file.

The failure kinds mirror the real dataset: an agent that just gets it wrong
(natural), one whose search found nothing useful (missing_evidence), one that
answered without searching (early_stop), and one misled by a planted
contradicting paragraph (contradiction).
"""

import json
import random
from pathlib import Path

SEED = 13
N_QUESTIONS = 600
OUT = Path(__file__).resolve().parent / "traces.jsonl"

SYLLABLES = ["ka", "lo", "ri", "ven", "dar", "mo", "sel", "tor", "ani", "bel", "quin", "zor", "ula", "pet", "rhu", "va"]


def name(rng, parts=2):
    return "".join(rng.choice(SYLLABLES) for _ in range(parts)).capitalize()


def make_world(rng, n=120):
    world = []
    for _ in range(n):
        world.append({
            "country": name(rng, 3),
            "capital": name(rng, 2),
            "river": f"{name(rng, 2)} River",
            "founder": f"{name(rng, 2)} {name(rng, 3)}",
            "founded": rng.randint(1100, 1950),
        })
    return world


QUESTION_KINDS = [
    ("river", "Which river flows through the capital of {country}?", "{capital} is the capital of {country}.", "{capital} lies on the {river}."),
    ("founded", "In what year was the capital of {country} founded?", "{capital} is the capital of {country}.", "{capital} was founded in {founded}."),
    ("founder", "Who founded the capital of {country}?", "{capital} is the capital of {country}.", "{capital} was founded by {founder}."),
]


def run_trace(rng, i, qid, kind, place, other, failure):
    """One agent run: the tools it called, what it found, what it answered, and the verdict."""
    key, template, hop1, hop2 = kind
    question = template.format(**place)
    truth = str(place[key])
    hop1_text, hop2_text = hop1.format(**place), hop2.format(**place)
    distractor = hop2.format(**other)

    calls = [{"name": "search", "args": {"query": f"capital of {place['country']}"}, "status": "ok"}]
    evidence = [hop1_text, distractor]
    stop_reason, used_search = "answered", True

    if failure == "early_stop":
        calls, evidence, used_search = [], [], False
        answer = str(other[key]) if rng.random() < 0.8 else truth
        if rng.random() < 0.15:
            stop_reason, answer = "no_answer", ""
    elif failure == "missing_evidence":
        calls.append({"name": "search", "args": {"query": f"{place['capital']} {key}"}, "status": "not_found"})
        answer = str(other[key]) if rng.random() < 0.85 else truth
    elif failure == "contradiction":
        planted = hop2_text.replace(truth, str(other[key]))
        calls.append({"name": "lookup", "args": {"title": place["capital"]}, "status": "ok"})
        evidence = [hop1_text, planted, hop2_text]
        answer = str(other[key]) if rng.random() < 0.7 else truth
    else:  # natural: the agent found everything and still sometimes gets it wrong
        calls.append({"name": "lookup", "args": {"title": place["capital"]}, "status": "ok"})
        evidence = [hop1_text, hop2_text, distractor]
        roll = rng.random()
        answer = truth if roll < 0.55 else str(other[key]) if roll < 0.85 else place["capital"]

    if key == "founder" and answer == truth and rng.random() < 0.25:
        answer = truth.split()[0]  # only the first name: partially right
        label = "partial"
    else:
        label = "correct" if answer == truth else "wrong"

    return {
        "trace_id": f"ex-{i:05d}",
        "qid": qid,
        "question": question,
        "tool_calls": calls,
        "evidence": evidence,
        "final_answer": answer,
        "label": label,
        "used_search": used_search,
        "stop_reason": stop_reason,
        "failure_type": failure,
    }


def main():
    rng = random.Random(SEED)
    world = make_world(rng)
    rows = []
    for q in range(N_QUESTIONS):
        place, other = rng.sample(world, 2)
        kind = rng.choice(QUESTION_KINDS)
        # Most questions get one run; some get a second run with a seeded fault, as in the real dataset.
        failures = ["natural"] + ([rng.choice(["missing_evidence", "early_stop", "contradiction"])] if rng.random() < 0.4 else [])
        for failure in failures:
            rows.append(run_trace(rng, len(rows), f"q{q:04d}", kind, place, other, failure))
    rng.shuffle(rows)
    OUT.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
    labels = {k: sum(r["label"] == k for r in rows) for k in ("correct", "partial", "wrong")}
    print(f"wrote {len(rows)} traces over {N_QUESTIONS} questions to {OUT}: {labels}")


if __name__ == "__main__":
    main()
