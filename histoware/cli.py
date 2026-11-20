"""Command line interface: load / summary / batches."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys

from .common import HistowareError
from . import attribution as attribution_mod
from . import summary as summary_mod
from . import warehouse as warehouse_mod


def _build_parser():
    parser = argparse.ArgumentParser(
        prog="histoware",
        description="Latency-histogram checkpoint warehouse and shift"
                    " summary tool.")
    sub = parser.add_subparsers(dest="command", required=True)

    load = sub.add_parser(
        "load", help="load one JSONL checkpoint batch into the warehouse")
    load.add_argument("--db", required=True, help="warehouse SQLite path")
    load.add_argument("--batch", required=True, help="batch id")
    load.add_argument("--file", required=True,
                      help="JSONL checkpoint file, or '-' for stdin")

    summ = sub.add_parser(
        "summary", help="build a result database for a set of shifts")
    summ.add_argument("--db", required=True, help="warehouse SQLite path")
    summ.add_argument("--version", type=int, default=None,
                      help="warehouse version to summarise"
                           " (default: current)")
    summ.add_argument("--roster", required=True, help="duty roster JSONL")
    summ.add_argument("--request", required=True, help="shifts JSONL")
    summ.add_argument("--out", required=True, help="result SQLite path")

    batches = sub.add_parser(
        "batches", help="list committed batches and versions")
    batches.add_argument("--db", required=True, help="warehouse SQLite path")

    attr = sub.add_parser(
        "attribute",
        help="attribute a shift regression between two result databases")
    attr.add_argument("--baseline", required=True,
                      help="baseline shift summary result SQLite path")
    attr.add_argument("--candidate", required=True,
                      help="candidate shift summary result SQLite path")
    attr.add_argument("--pairs", required=True,
                      help="explicit pairing manifest JSONL")
    attr.add_argument("--out", required=True,
                      help="comparison SQLite path")
    return parser


def main(argv=None):
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "load":
            if args.file == "-":
                result = warehouse_mod.load_batch(
                    args.db, args.batch, sys.stdin.buffer)
            else:
                with open(args.file, "rb") as fh:
                    result = warehouse_mod.load_batch(args.db, args.batch, fh)
            print(json.dumps(result, indent=2, sort_keys=True))
        elif args.command == "summary":
            stats = summary_mod.run_summary(
                args.db, args.version, args.roster, args.request, args.out)
            stats["out"] = args.out
            print(json.dumps(stats, indent=2, sort_keys=True))
        elif args.command == "batches":
            print(json.dumps(warehouse_mod.list_batches(args.db), indent=2))
        elif args.command == "attribute":
            stats = attribution_mod.run_attribution(
                args.baseline, args.candidate, args.pairs, args.out)
            stats["out"] = args.out
            print(json.dumps(stats, indent=2, sort_keys=True))
    except HistowareError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except sqlite3.Error as exc:
        print(f"error: sqlite: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
