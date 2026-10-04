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
    raise ValueError(f"non-finite JSON number token: {value}")


def _reject_non_finite(value: object) -> None:
    """Reject every non-finite number anywhere in a decoded JSON tree.

    ``parse_constant`` refuses the bare NaN/Infinity/-Infinity literals, but a
    number whose exponent overflows float range (e.g. 1e1000) decodes to a
    non-finite float without ever being seen as a constant token; such values
    can hide at any depth (for example inside a client command), so the whole
    tree is checked after decoding. Object keys and the strings "NaN" /
    "Infinity" / "-Infinity" are ordinary strings and never rejected.
    """
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, float):
            if not math.isfinite(current):
                raise ValueError(f"non-finite JSON number: {current!r}")
        elif isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)


def _load_strict_json(text: str) -> object:
    """Decode strict JSON: no NaN/Infinity/-Infinity tokens and no non-finite
    numbers anywhere in the tree."""
    raw = json.loads(text, parse_constant=_reject_json_constant)
    _reject_non_finite(raw)
    return raw


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
        raw = _load_strict_json(text)
    except ValueError as exc:
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
        raw = _load_strict_json(text)
    except ValueError as exc:
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
        # Strict JSON boundary: refuse the non-standard NaN/Infinity tokens
        # and any non-finite number an exponent overflow could decode to,
        # anywhere in the tree.
        return True, _load_strict_json(text)
    except ValueError as exc:
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
