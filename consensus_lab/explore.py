"""Bounded enumeration of message-fault combinations over a base scenario.

``explore`` reuses the deterministic simulator for every combination: it
never reads the wall clock, never uses randomness and never creates files,
so identical PLAN input produces byte-identical output.
"""

from __future__ import annotations

import math
from itertools import combinations

from .simulate import ScenarioError, _Simulator, _require_int, parse_scenario

_PLAN_FIELDS = {"scenario", "candidates", "maxFaults", "maxCases", "minimizeFailures"}
_REQUIRED_PLAN_FIELDS = {"scenario", "candidates", "maxFaults", "maxCases"}

# Reports whose violation lists decide a case's status. A report the scenario
# does not enable is absent from the result and plays no part in the verdict.
_CHECKED_REPORTS = (
    "electionSafety",
    "logMatching",
    "stateMachineSafety",
    "linearizability",
    "liveness",
)


def _failure_signature(result: dict) -> tuple[str, ...]:
    """Names of the enabled reports whose violation list is non-empty, in
    fixed check-priority order. Two cases share a failure signature exactly
    when they violate the same set of invariants."""
    return tuple(
        key
        for key in _CHECKED_REPORTS
        if key in result and result[key]["violations"]
    )


def run_explore(raw: object) -> dict:
    """Validate a decoded JSON PLAN and run every fault combination."""
    if not isinstance(raw, dict):
        raise ScenarioError("plan must be a JSON object")
    unknown = sorted(set(raw) - _PLAN_FIELDS)
    if unknown:
        raise ScenarioError(f"unknown field(s): {', '.join(unknown)}")
    missing = sorted(_REQUIRED_PLAN_FIELDS - set(raw))
    if missing:
        raise ScenarioError(f"missing field(s): {', '.join(missing)}")

    minimize_failures = raw.get("minimizeFailures", False)
    if not isinstance(minimize_failures, bool):
        raise ScenarioError("minimizeFailures must be a boolean")

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
    # Only populated while minimizing: signature -> list of
    # (selected index tuple, caseId). Minimization never runs extra
    # simulations; it only references cases enumerated here, so those
    # references do not count against maxCases.
    by_signature: dict[tuple[str, ...], list[tuple[tuple[int, ...], int]]] = {}
    passed = 0
    case_id = 0
    for size in range(max_faults + 1):
        for selected in combinations(range(len(candidates)), size):
            case_config = dict(config)
            case_config["messageFaults"] = [rules[i] for i in selected]
            result = _Simulator(case_config).run()
            signature = _failure_signature(result)
            if signature:
                status = "failed"
            else:
                status = "passed"
                passed += 1
            case = {
                "caseId": case_id,
                "selected": list(selected),
                "status": status,
                "result": result,
            }
            if minimize_failures and signature:
                by_signature.setdefault(signature, []).append((selected, case_id))
            cases.append(case)
            case_id += 1

    if minimize_failures:
        for case in cases:
            if case["status"] != "failed":
                continue
            signature = _failure_signature(case["result"])
            selected = tuple(case["selected"])
            # Every minimizing combination must be reachable by deleting
            # rules from this case (a sub-combination of its selected
            # indices) and reproduce its exact failure signature: a
            # combination that passes or that only shows other violations
            # is never selected. Fewer rules win; ties are broken by the
            # numeric lexicographic order of the index array. The case
            # itself always qualifies.
            minimal_selected, minimal_case_id = min(
                (
                    entry
                    for entry in by_signature[signature]
                    if set(entry[0]).issubset(selected)
                ),
                key=lambda entry: (len(entry[0]), entry[0]),
            )
            case["failureReports"] = list(signature)
            case["minimalSelected"] = list(minimal_selected)
            case["minimalCaseId"] = minimal_case_id

    return {
        "totalCases": len(cases),
        "passedCases": passed,
        "failedCases": len(cases) - passed,
        "cases": cases,
    }
