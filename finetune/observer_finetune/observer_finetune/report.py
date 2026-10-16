"""What a run leaves behind besides the checkpoint: manifest.json, results.csv and report.md."""

import csv
import json
from pathlib import Path


def write_results(path: Path, rows: list[dict]):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_manifest(path: Path, manifest: dict):
    path.write_text(json.dumps(manifest, indent=2, default=float))


def write_report(path: Path, manifest: dict):
    m = manifest
    rates = list(m["metrics"]["finetuned"]["normalized_recall"])
    budget = f"{m['evaluation']['sample_rate']:.0%}"
    lines = [
        f"# Fine-tuning run: {m['model']}",
        "",
        f"Data: {m['source']}. Decision signal: `{m['decision']['question']}`.",
        f"Test portion: {m['split']['test']['traces']} traces, {m['split']['test']['base_rate']:.1%} wrong.",
        "",
    ]
    if m["warnings"]:
        lines += ["## Warnings", ""] + [f"- **{w}**" for w in m["warnings"]] + [""]
    lines += [
        f"## Results at a {budget} review budget",
        "",
        "Normalized recall: 0 is random sampling, 1 catches every wrong trace the budget allows.",
        "",
        "| | AUROC | " + " | ".join(f"norm. recall, {float(r):.0%} wrong" for r in rates) + " |",
        "|---|---|" + "---|" * len(rates),
    ]
    for name in ("zero_shot", "finetuned", "heuristic"):
        row = m["metrics"][name]
        lines.append(f"| {name.replace('_', '-')} | {row['auroc']:.3f} | "
                     + " | ".join(f"{row['normalized_recall'][r]:.3f}" for r in rates) + " |")
    lines += [
        "",
        f"The heuristic row is the best of the no-model rules ({m['metrics']['heuristic']['name']}).",
        "",
        "## Serving it",
        "",
        "```sh",
        f"cd {m['kev_path']}",
        f"uv run --extra serve python -m kev.serve --run {m['checkpoint']} --port 8010",
        "```",
        "",
        f"Point the Observer engine's `scorer.manifest` at `{path.parent / 'manifest.json'}`.",
        "",
    ]
    path.write_text("\n".join(lines))
