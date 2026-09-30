"""Command line entry point for consensus-protocol-lab."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .simulator import SimulationError, parse_scenario, run_simulation


def _run_simulate(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        print(f"error: cannot read scenario file: {exc.strerror or exc}", file=sys.stderr)
        return 2

    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        print(f"error: invalid JSON: {exc.msg}", file=sys.stderr)
        return 2

    try:
        scenario = parse_scenario(raw)
        result = run_simulation(scenario)
    except SimulationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    json.dump(result, sys.stdout, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="consensus-protocol-lab", description="Deterministic simulation lab for Raft-style consensus protocols")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the current version")

    simulate = sub.add_parser("simulate", help="run a deterministic Raft leader-election scenario")
    simulate.add_argument("scenario", help="path to a UTF-8 JSON scenario file")

    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0

    if args.command == "simulate":
        return _run_simulate(args.scenario)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
