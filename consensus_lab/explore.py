"""Bounded enumeration of message-fault combinations over a base scenario.

``explore`` reuses the deterministic simulator for every combination: it
never reads the wall clock, never uses randomness and never creates files,
so identical PLAN input produces byte-identical output.
"""

from __future__ import annotations

import math
from itertools import combinations

from .simulate import ScenarioError, _Simulator, _require_int, parse_scenario

_PLAN_REQUIRED_FIELDS = {"scenario", "candidates", "maxFaults", "maxCases"}
_PLAN_OPTIONAL_FIELDS = {"minimizeFailures"}
_PLAN_FIELDS = _PLAN_REQUIRED_FIELDS | _PLAN_OPTIONAL_FIELDS

# Reports whose violation lists decide a case's status. A report the scenario
# does not enable is absent from the result and plays no part in the verdict.
_CHECKED_REPORTS = (
    "electionSafety",
    "logMatching",
    "stateMachineSafety",
    "linearizability",
    "liveness",
)


def run_explore(raw: object) -> dict:
    """Validate a decoded JSON PLAN and run every fault combination."""
    if not isinstance(raw, dict):
        raise ScenarioError("plan must be a JSON object")
    unknown = sorted(set(raw) - _PLAN_FIELDS)
    if unknown:
        raise ScenarioError(f"unknown field(s): {', '.join(unknown)}")
    missing = sorted(_PLAN_REQUIRED_FIELDS - set(raw))
    if missing:
        raise ScenarioError(f"missing field(s): {', '.join(missing)}")

    scenario_raw = raw["scenario"]
    if not isinstance(scenario_raw, dict):
        raise ScenarioError("scenario must be a JSON object")
    if "messageFaults" in scenario_raw:
        raise ScenarioError("scenario must not contain messageFaults")

    candidates = raw["candidates"]
    if not isinstance(candidates, list):
        raise ScenarioError("candidates must be a list")
    if not candidates:
        raise ScenarioError("candidates must not be empty")

    max_faults = _require_int(raw["maxFaults"], "maxFaults", 0)
    if max_faults > len(candidates):
        raise ScenarioError("maxFaults must not exceed the number of candidates")
    max_cases = _require_int(raw["maxCases"], "maxCases", 1)

    minimize = raw.get("minimizeFailures", False)
    if not isinstance(minimize, bool):
        raise ScenarioError("minimizeFailures must be a boolean")

    # Validate the base scenario and every candidate rule with the original
    # scenario validation by presenting the candidates as its messageFaults;
    # this also rejects duplicated full selectors. Each normalized rule keeps
    # its candidate index in its "index" field.
    combined = dict(scenario_raw)
    combined["messageFaults"] = candidates
    config = parse_scenario(combined)
    rules = config["messageFaults"]

    total = sum(math.comb(len(candidates), size) for size in range(max_faults + 1))
    if total > max_cases:
        raise ScenarioError(
            f"combination count {total} exceeds maxCases {max_cases}"
        )

    # The empty combination comes first, then combinations of ascending size;
    # itertools.combinations yields index tuples in numeric lexicographic
    # order, and each tuple is ascending, so selected rules are always
    # injected in original candidate-index order.
    cases = []
    passed = 0
    case_id = 0
    for size in range(max_faults + 1):
        for selected in combinations(range(len(candidates)), size):
            case_config = dict(config)
            case_config["messageFaults"] = [rules[i] for i in selected]
            result = _Simulator(case_config).run()
            failed = any(
                key in result and result[key]["violations"]
                for key in _CHECKED_REPORTS
            )
            if failed:
                status = "failed"
            else:
                status = "passed"
                passed += 1
            cases.append(
                {
                    "caseId": case_id,
                    "selected": list(selected),
                    "status": status,
                    "result": result,
                }
            )
            case_id += 1
    if minimize:
        _attach_minimization(cases)
    return {
        "totalCases": len(cases),
        "passedCases": passed,
        "failedCases": len(cases) - passed,
        "cases": cases,
    }


def _failure_reports(result: dict) -> list:
    """Names of enabled reports with non-empty violations, in check order."""
    return [
        key
        for key in _CHECKED_REPORTS
        if key in result and result[key]["violations"]
    ]


def _attach_minimization(cases: list) -> None:
    """Add failureReports/minimalSelected/minimalCaseId to every failed case.

    Minimization only deletes rules from the failed case's own selection, so
    every candidate sub-combination is itself an enumerated case whose stored
    result decides whether the same failure signature is preserved; no extra
    simulations are run. The fewest-rule combination wins, ties broken by
    numeric lexicographic order of the index list; combinations() yields
    subsets of an ascending tuple in exactly that order.
    """
    signatures = [_failure_reports(case["result"]) for case in cases]
    by_selection = {tuple(case["selected"]): case for case in cases}
    for case, signature in zip(cases, signatures):
        if case["status"] != "failed":
            continue
        case["failureReports"] = list(signature)
        selected = case["selected"]
        minimal = None
        for size in range(len(selected) + 1):
            for subset in combinations(selected, size):
                if signatures[by_selection[subset]["caseId"]] == signature:
                    minimal = subset
                    break
            if minimal is not None:
                break
        case["minimalSelected"] = list(minimal)
        case["minimalCaseId"] = by_selection[minimal]["caseId"]
