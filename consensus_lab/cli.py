"""Command line entry point for consensus-protocol-lab."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .explore import run_explore
from .replay import run_replay
from .simulate import ScenarioError, run_simulation


def _read_json_file(path: str, kind: str) -> tuple[object | None, int | None]:
    """Read and decode a UTF-8 JSON file.

    Returns ``(value, None)`` on success or ``(None, 2)`` after writing a
    single ``error:`` line on any read, encoding or syntax failure.
    """
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        print(f"error: cannot read {kind} file: {exc}", file=sys.stderr)
        return None, 2
    except UnicodeDecodeError:
        print(f"error: {kind} file is not valid UTF-8: {path}", file=sys.stderr)
        return None, 2
    try:
        return json.loads(text), None
    except json.JSONDecodeError as exc:
        print(f"error: invalid JSON: {exc}", file=sys.stderr)
        return None, 2


def _cmd_simulate(path: str) -> int:
    raw, code = _read_json_file(path, "scenario")
    if code is not None:
        return code
    try:
        result = run_simulation(raw)
    except ScenarioError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _cmd_explore(path: str) -> int:
    raw, code = _read_json_file(path, "plan")
    if code is not None:
        return code
    try:
        result = run_explore(raw)
    except ScenarioError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _cmd_replay(scenario_path: str, result_path: str) -> int:
    scenario_raw, code = _read_json_file(scenario_path, "scenario")
    if code is not None:
        return code
    result_raw, code = _read_json_file(result_path, "result")
    if code is not None:
        return code
    try:
        verdict = run_replay(scenario_raw, result_raw)
    except ScenarioError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(verdict, ensure_ascii=False, separators=(",", ":")))
    return 0 if verdict["status"] == "matched" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="consensus-protocol-lab", description="Deterministic simulation lab for Raft-style consensus protocols")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the current version")
    simulate = sub.add_parser("simulate", help="run a deterministic Raft leader-election simulation")
    simulate.add_argument("scenario", help="path to a UTF-8 JSON scenario file")
    explore = sub.add_parser("explore", help="enumerate bounded message-fault combinations over a base scenario")
    explore.add_argument("plan", help="path to a UTF-8 JSON plan file")
    replay = sub.add_parser("replay", help="verify a saved simulate result is reproduced by its scenario")
    replay.add_argument("scenario", help="path to the UTF-8 JSON scenario file")
    replay.add_argument("result", help="path to a UTF-8 JSON file holding one simulate result")
    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0
    if args.command == "simulate":
        return _cmd_simulate(args.scenario)
    if args.command == "explore":
        return _cmd_explore(args.plan)
    if args.command == "replay":
        return _cmd_replay(args.scenario, args.result)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
