"""Bounded enumeration of message-fault combinations for the Raft simulation.

An exploration PLAN is a UTF-8 JSON object with ``scenario``, ``candidates``,
``maxFaults`` and ``maxCases`` (and no other fields).  The base scenario uses
the existing scenario format but must not contain ``messageFaults``; each
candidate is an existing message-fault rule.  Combinations are enumerated in a
fixed order — the empty combination first, then by ascending rule count and by
the numeric lexicographic order of candidate-index tuples — and each
combination is simulated independently.  Like the simulator itself, exploration
never reads the wall clock, never uses randomness and never creates files, so
the same PLAN produces byte-identical output.
"""

from __future__ import annotations

import copy
import itertools

from .simulate import ScenarioError, _Simulator, parse_scenario

_PLAN_FIELDS = {"scenario", "candidates", "maxFaults", "maxCases"}
_PLAN_REQUIRED = _PLAN_FIELDS


class PlanError(Exception):
    """The exploration PLAN input is invalid."""


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_plan(raw: object) -> dict:
    """Validate the decoded JSON exploration PLAN and return a normalized config.

    The base scenario and every candidate rule are validated through the
    existing scenario parser; candidate rule ``index`` fields are dropped and
    reassigned in candidate order.
    """
    if not isinstance(raw, dict):
        raise PlanError("PLAN must be a JSON object")
    unknown = sorted(set(raw) - _PLAN_FIELDS)
    if unknown:
        raise PlanError(f"unknown field(s): {', '.join(unknown)}")
    missing = sorted(_PLAN_REQUIRED - set(raw))
    if missing:
        raise PlanError(f"missing field(s): {', '.join(missing)}")

    scenario_raw = raw["scenario"]
    if not isinstance(scenario_raw, dict):
        raise PlanError("scenario must be a JSON object")
    if "messageFaults" in scenario_raw:
        raise PlanError("scenario must not contain messageFaults")

    candidates_raw = raw["candidates"]
    if not isinstance(candidates_raw, list) or not candidates_raw:
        raise PlanError("candidates must be a non-empty list")

    max_faults = raw["maxFaults"]
    if not _is_int(max_faults):
        raise PlanError("maxFaults must be an integer")
    if max_faults < 0 or max_faults > len(candidates_raw):
        raise PlanError("maxFaults must be between 0 and the number of candidates")

    max_cases = raw["maxCases"]
    if not _is_int(max_cases) or max_cases < 1:
        raise PlanError("maxCases must be a positive integer")

    # Validate the base scenario and every candidate through one parse pass.
    # parse_scenario rejects non-object candidates, unknown/missing fields,
    # invalid nodes/message kinds/occurrences/actions/delays and duplicate
    # complete selectors.
    combined = dict(scenario_raw)
    combined["messageFaults"] = list(candidates_raw)
    try:
        config = parse_scenario(combined)
    except ScenarioError as exc:
        raise PlanError(str(exc)) from exc
    normalized_rules = [
        {key: value for key, value in rule.items() if key != "index"}
        for rule in config["messageFaults"]
    ]

    base_config = dict(config)
    base_config["messageFaults"] = []

    return {
        "scenario": base_config,
        "candidates": normalized_rules,
        "maxFaults": max_faults,
        "maxCases": max_cases,
    }


def _case_status(result: dict) -> str:
    """A case fails when any enabled report lists a non-empty violations list."""
    for report_name in (
        "electionSafety",
        "logMatching",
        "stateMachineSafety",
        "linearizability",
        "liveness",
    ):
        report = result.get(report_name)
        if report is not None and report.get("violations"):
            return "failed"
    return "passed"


def run_exploration(raw: object) -> dict:
    """Validate a PLAN and enumerate and simulate every fault combination."""
    plan = parse_plan(raw)
    candidates = plan["candidates"]
    max_faults = plan["maxFaults"]
    max_cases = plan["maxCases"]
    base_config = plan["scenario"]

    combinations: list[tuple[int, ...]] = [()]
    for size in range(1, max_faults + 1):
        combinations.extend(itertools.combinations(range(len(candidates)), size))

    if len(combinations) > max_cases:
        raise PlanError(
            f"number of combinations ({len(combinations)}) exceeds maxCases ({max_cases})"
        )

    cases = []
    for case_id, selected in enumerate(combinations):
        case_config = copy.deepcopy(base_config)
        # Rules are injected in their original candidate order; the rule index
        # recorded in the timeline matches the candidate index.
        case_config["messageFaults"] = [
            {**candidates[index], "index": index} for index in selected
        ]
        result = _Simulator(case_config).run()
        cases.append(
            {
                "caseId": case_id,
                "selected": list(selected),
                "status": _case_status(result),
                "result": result,
            }
        )

    return {
        "totalCases": len(cases),
        "passedCases": sum(1 for case in cases if case["status"] == "passed"),
        "failedCases": sum(1 for case in cases if case["status"] == "failed"),
        "cases": cases,
    }
