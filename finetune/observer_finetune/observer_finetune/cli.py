"""observer-finetune check | train"""

import argparse
import sys

from . import check, finetune
from .schemas import TrainOptions
from .sources.factory import DEFAULT_LABEL_NAME


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="observer-finetune", description="Fine-tune Kev on labeled agent traces.")
    commands = parser.add_subparsers(dest="command", required=True)

    def data_args(p):
        p.add_argument("--data", required=True, help="labeled traces: a .jsonl file, OTLP span lines, or phoenix://host:port/project")
        p.add_argument("--annotations", help="labels for OTLP span files: annotation lines with trace_id or span_id")
        p.add_argument("--label-name", default=DEFAULT_LABEL_NAME, help="the annotation that holds the label (default: correctness)")

    data_args(commands.add_parser("check", help="validate the data and show the split, without training"))
    train = commands.add_parser("train", help="fine-tune, evaluate and write the run directory")
    data_args(train)
    train.add_argument("--out", required=True, help="the run directory to create")
    train.add_argument("--kev-url", help="a server running the released Kev, for the zero-shot baseline")
    train.add_argument("--kev-path", help="a Kev checkout to use instead of the managed one")
    train.add_argument("--epochs", type=int, default=TrainOptions.epochs)
    train.add_argument("--device", help="mps, cuda or cpu (default: mps on Apple Silicon, else cuda)")
    train.add_argument("--max-steps", type=int, default=0, help="stop after this many optimizer steps, for a quick trial")
    args = parser.parse_args(argv)
    sys.stdout.reconfigure(line_buffering=True)   # progress shows up as it happens, even when piped to a file

    try:
        if args.command == "check":
            report = check(args.data, annotations=args.annotations, label_name=args.label_name)
            print(f"{report.traces} traces from {report.source}: {report.labels}")
            print(f"split: {report.split_method}")
            for name, counts in report.splits.items():
                print(f"  {name:<12}{counts['traces']:>6} traces {counts['questions']:>6} questions {counts['wrong']:>6} wrong")
            for note in report.notes:
                print(f"note: {note}")
            for problem in report.problems:
                print(f"problem: {problem}")
            return 0 if report.ok else 1
        result = finetune(args.data, args.out, kev_url=args.kev_url, kev_path=args.kev_path,
                          annotations=args.annotations, label_name=args.label_name,
                          train_options=TrainOptions(epochs=args.epochs, device=args.device, max_steps=args.max_steps))
        print(f"checkpoint: {result.checkpoint}")
        return 0
    except ValueError as err:
        print(f"observer-finetune: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
