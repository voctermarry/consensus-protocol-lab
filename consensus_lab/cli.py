"""Command line entry point for consensus-protocol-lab."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .explore import PlanError, run_exploration
from .simulate import ScenarioError, run_simulation


def _cmd_simulate(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        print(f"error: cannot read scenario file: {exc}", file=sys.stderr)
        return 2
    except UnicodeDecodeError:
        print(f"error: scenario file is not valid UTF-8: {path}", file=sys.stderr)
        return 2
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        print(f"error: invalid JSON: {exc}", file=sys.stderr)
        return 2
    try:
        result = run_simulation(raw)
    except ScenarioError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _cmd_explore(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        print(f"error: cannot read PLAN file: {exc}", file=sys.stderr)
        return 2
    except UnicodeDecodeError:
        print(f"error: PLAN file is not valid UTF-8: {path}", file=sys.stderr)
        return 2
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        print(f"error: invalid JSON: {exc}", file=sys.stderr)
        return 2
    try:
        result = run_exploration(raw)
    except (PlanError, ScenarioError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="consensus-protocol-lab", description="Deterministic simulation lab for Raft-style consensus protocols")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the current version")
    simulate = sub.add_parser("simulate", help="run a deterministic Raft leader-election simulation")
    simulate.add_argument("scenario", help="path to a UTF-8 JSON scenario file")
    explore = sub.add_parser("explore", help="enumerate message-fault combinations for a deterministic Raft simulation")
    explore.add_argument("plan", help="path to a UTF-8 JSON exploration PLAN file")
    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0
    if args.command == "simulate":
        return _cmd_simulate(args.scenario)
    if args.command == "explore":
        return _cmd_explore(args.plan)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
