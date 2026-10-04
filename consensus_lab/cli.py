"""Command line entry point for consensus-protocol-lab."""

from __future__ import annotations

import argparse
import json
import math
import sys

from . import __version__
from .explore import run_explore
from .replay import run_replay
from .simulate import ScenarioError, run_simulation


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite number is not valid JSON: {value}")


def _parse_finite_float(value: str) -> float:
    # Reject JSON numbers whose magnitude overflows float (e.g. 1e999);
    # they would otherwise decode silently to inf.
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"non-finite number is not valid JSON: {value}")
    return number


def _loads_strict_json(text: str) -> object:
    """Decode JSON, rejecting NaN/Infinity/-Infinity literals and any
    number that does not decode to a finite value. String values and
    object keys are unaffected (parse_constant only fires on bare
    literals)."""
    return json.loads(
        text,
        parse_constant=_reject_json_constant,
        parse_float=_parse_finite_float,
    )


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
        raw = _loads_strict_json(text)
    except (json.JSONDecodeError, ValueError) as exc:
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
        print(f"error: cannot read plan file: {exc}", file=sys.stderr)
        return 2
    except UnicodeDecodeError:
        print(f"error: plan file is not valid UTF-8: {path}", file=sys.stderr)
        return 2
    try:
        raw = _loads_strict_json(text)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"error: invalid JSON: {exc}", file=sys.stderr)
        return 2
    try:
        result = run_explore(raw)
    except ScenarioError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _read_utf8_json(path: str, kind: str) -> tuple[bool, object]:
    """Read and decode a UTF-8 JSON file for replay. Returns (ok, value);
    on failure prints one ``error:`` line to stderr and returns (False,
    None). ``kind`` labels the file in the messages."""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        print(f"error: cannot read {kind} file: {exc}", file=sys.stderr)
        return False, None
    except UnicodeDecodeError:
        print(f"error: {kind} file is not valid UTF-8: {path}", file=sys.stderr)
        return False, None
    try:
        # Strict JSON: reject the non-standard NaN/Infinity tokens that
        # Python's decoder otherwise accepts, and any number that decodes
        # to a non-finite float (e.g. 1e999 overflowing to inf).
        return True, _loads_strict_json(text)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"error: invalid JSON in {kind} file: {exc}", file=sys.stderr)
        return False, None


def _cmd_replay(scenario_path: str, result_path: str) -> int:
    ok, scenario_raw = _read_utf8_json(scenario_path, "scenario")
    if not ok:
        return 2
    ok, result_raw = _read_utf8_json(result_path, "result")
    if not ok:
        return 2
    # A non-object RESULT cannot carry the comparison contract (status is
    # reported as mismatched only when the top level is an object).
    if not isinstance(result_raw, dict):
        print("error: result file must contain a JSON object", file=sys.stderr)
        return 2
    try:
        report = run_replay(scenario_raw, result_raw)
    except ScenarioError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    # Compact, member order fixed by construction: byte-identical for
    # identical input.
    print(json.dumps(report, ensure_ascii=False, separators=(",", ":")))
    return 0 if report["status"] == "matched" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="consensus-protocol-lab", description="Deterministic simulation lab for Raft-style consensus protocols")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the current version")
    simulate = sub.add_parser("simulate", help="run a deterministic Raft leader-election simulation")
    simulate.add_argument("scenario", help="path to a UTF-8 JSON scenario file")
    explore = sub.add_parser("explore", help="enumerate bounded message-fault combinations over a base scenario")
    explore.add_argument("plan", help="path to a UTF-8 JSON plan file")
    replay = sub.add_parser("replay", help="recompute a simulation and field-compare it against a saved result")
    replay.add_argument("scenario", help="path to the UTF-8 JSON scenario file")
    replay.add_argument("result", help="path to a UTF-8 JSON file holding a simulate result")
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
