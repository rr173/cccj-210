"""Command line interface: load / summary / batches / retire."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys

from .common import HistowareError, RetiredVersionError
from . import attribution as attribution_mod
from . import retention as retention_mod
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

    retire = sub.add_parser(
        "retire", help="manage warehouse version retention")
    rsub = retire.add_subparsers(dest="retire_command", required=True)

    rinit = rsub.add_parser(
        "init", help="bring a warehouse under retention management")
    rinit.add_argument("--db", required=True, help="warehouse SQLite path")

    rstatus = rsub.add_parser(
        "status", help="show waterline, exceptions and policy generation")
    rstatus.add_argument("--db", required=True, help="warehouse SQLite path")

    rplan = rsub.add_parser(
        "plan", help="rehearse a retention policy and save the plan")
    rplan.add_argument("--db", required=True, help="warehouse SQLite path")
    rplan.add_argument("--target", required=True,
                       help="complete target policy JSON file")
    rplan.add_argument("--out", required=True,
                       help="write the rehearsal plan JSON here")

    rapprove = rsub.add_parser(
        "approve", help="re-verify a plan and retire versions atomically")
    rapprove.add_argument("--db", required=True, help="warehouse SQLite path")
    rapprove.add_argument("--plan", required=True,
                          help="plan JSON file produced by 'retire plan'")

    rreceipt = rsub.add_parser(
        "receipt", help="show a retirement receipt by plan id")
    rreceipt.add_argument("--db", required=True, help="warehouse SQLite path")
    rreceipt.add_argument("--plan-id", required=True,
                          help="plan id whose receipt to show")
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
        elif args.command == "retire":
            if args.retire_command == "init":
                result = retention_mod.init_retention(args.db)
            elif args.retire_command == "status":
                result = retention_mod.get_status(args.db)
            elif args.retire_command == "plan":
                result = retention_mod.rehearse(args.db, args.target)
                with open(args.out, "w", encoding="utf-8") as fh:
                    json.dump(result, fh, indent=2, sort_keys=True)
                    fh.write("\n")
                result["plan_file"] = args.out
            elif args.retire_command == "approve":
                result = retention_mod.approve(args.db, args.plan)
            elif args.retire_command == "receipt":
                result = retention_mod.get_receipt(args.db, args.plan_id)
            else:       # pragma: no cover - argparse enforces the choice
                raise HistowareError("unknown retire command")
            print(json.dumps(result, indent=2, sort_keys=True))
    except RetiredVersionError as exc:
        print(f"error: retired version: {exc}", file=sys.stderr)
        return 2
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
