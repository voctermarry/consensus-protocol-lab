"""Bounded enumeration of message-fault combinations over a base scenario.

``explore`` reuses the deterministic simulator for every combination: it
never reads the wall clock, never uses randomness and never creates files,
so identical PLAN input produces byte-identical output.
"""

from __future__ import annotations

from itertools import combinations

from .scenario import ScenarioError, _require_int, parse_scenario
from .simulate import _Simulator

_PLAN_FIELDS = {
    "scenario",
    "candidates",
    "maxFaults",
    "maxCases",
    "minimizeFailures",
    "eventCandidates",
    "maxEventFaults",
}
_REQUIRED_PLAN_FIELDS = {"scenario", "candidates", "maxFaults", "maxCases"}
_EVENT_CANDIDATE_FIELDS = {"network", "node"}

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


def _merged_config(
    scenario_raw: dict,
    candidates: list,
    event_candidates: list,
    event_selected: tuple[int, ...],
) -> dict:
    """Validate and normalize the scenario for one event combination: the
    base scenario with every message-fault candidate presented as its
    messageFaults and the selected events appended to the corresponding
    collections. Base entries keep their positions, so at one timestamp
    same-kind base events are processed before the selected candidates (in
    candidate-index order), and network events still precede node events."""
    merged = dict(scenario_raw)
    merged["messageFaults"] = candidates
    network = [
        event_candidates[i]["network"] for i in event_selected if "network" in event_candidates[i]
    ]
    node = [event_candidates[i]["node"] for i in event_selected if "node" in event_candidates[i]]
    if network or "faults" in merged:
        merged["faults"] = list(merged.get("faults", [])) + network
    if node or "nodeEvents" in merged:
        merged["nodeEvents"] = list(merged.get("nodeEvents", [])) + node
    return parse_scenario(merged)


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

    has_event_candidates = "eventCandidates" in raw
    has_max_event_faults = "maxEventFaults" in raw
    if has_event_candidates != has_max_event_faults:
        raise ScenarioError(
            "eventCandidates and maxEventFaults must be provided together"
        )
    events_enabled = has_event_candidates

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

    event_candidates: list = []
    max_event_faults = 0
    if events_enabled:
        event_candidates = raw["eventCandidates"]
        if not isinstance(event_candidates, list):
            raise ScenarioError("eventCandidates must be a list")
        if not event_candidates:
            raise ScenarioError("eventCandidates must not be empty")
        for index, item in enumerate(event_candidates):
            label = f"eventCandidates[{index}]"
            if not isinstance(item, dict):
                raise ScenarioError(f"{label} must be an object")
            unknown = sorted(set(item) - _EVENT_CANDIDATE_FIELDS)
            if unknown:
                raise ScenarioError(
                    f"{label} has unknown field(s): {', '.join(unknown)}"
                )
            if len(set(item) & _EVENT_CANDIDATE_FIELDS) != 1:
                raise ScenarioError(
                    f"{label} must declare exactly one of network or node"
                )
        max_event_faults = _require_int(raw["maxEventFaults"], "maxEventFaults", 0)
        if max_event_faults > len(event_candidates):
            raise ScenarioError(
                "maxEventFaults must not exceed the number of event candidates"
            )

    # Validate the base scenario and every candidate rule with the original
    # scenario validation by presenting the candidates as its messageFaults;
    # this also rejects duplicated full selectors. Each normalized rule keeps
    # its candidate index in its "index" field.
    combined = dict(scenario_raw)
    combined["messageFaults"] = candidates
    config = parse_scenario(combined)
    rules = config["messageFaults"]

    # The empty combination comes first, then combinations of ascending size;
    # itertools.combinations yields index tuples in numeric lexicographic
    # order, and each tuple is ascending, so selected rules and events are
    # always injected in original candidate-index order.
    message_combos = [
        combo
        for size in range(max_faults + 1)
        for combo in combinations(range(len(candidates)), size)
    ]
    event_combos = [
        combo
        for size in range(max_event_faults + 1)
        for combo in combinations(range(len(event_candidates)), size)
    ]

    total = len(message_combos) * len(event_combos)
    if total > max_cases:
        raise ScenarioError(
            f"combination count {total} exceeds maxCases {max_cases}"
        )

    if events_enabled:
        # Every merged scenario is validated before any simulation runs:
        # first each event candidate on its own (in the base context, exactly
        # like the message-fault candidates above), then every enumerated
        # combination, whose appended events may jointly violate a constraint
        # such as crash/restart alternation.
        for index in range(len(event_candidates)):
            _merged_config(scenario_raw, candidates, event_candidates, (index,))
        event_configs = [
            _merged_config(scenario_raw, candidates, event_candidates, combo)
            for combo in event_combos
        ]
    else:
        event_configs = [config]

    # Message-fault combinations form the outer loop in their established
    # order; within each, the empty event combination comes first, then
    # event combinations of ascending size in numeric lexicographic order.
    cases = []
    # Only populated while minimizing: signature -> list of
    # (selected index tuple, selected event index tuple, caseId).
    # Minimization never runs extra simulations; it only references cases
    # enumerated here, so those references do not count against maxCases.
    by_signature: dict[
        tuple[str, ...], list[tuple[tuple[int, ...], tuple[int, ...], int]]
    ] = {}
    passed = 0
    case_id = 0
    for selected in message_combos:
        for event_index, event_selected in enumerate(event_combos):
            case_config = dict(event_configs[event_index])
            case_config["messageFaults"] = [rules[i] for i in selected]
            result = _Simulator(case_config).run()
            signature = _failure_signature(result)
            if signature:
                status = "failed"
            else:
                status = "passed"
                passed += 1
            case = {"caseId": case_id, "selected": list(selected)}
            if events_enabled:
                case["selectedEvents"] = list(event_selected)
            case["status"] = status
            case["result"] = result
            if minimize_failures and signature:
                by_signature.setdefault(signature, []).append(
                    (selected, event_selected, case_id)
                )
            cases.append(case)
            case_id += 1

    if minimize_failures:
        for case in cases:
            if case["status"] != "failed":
                continue
            signature = _failure_signature(case["result"])
            selected = tuple(case["selected"])
            selected_events = tuple(case.get("selectedEvents", ()))
            # Every minimizing combination must be reachable by deleting
            # candidates of both kinds from this case (sub-combinations of
            # its selected indices) and reproduce its exact failure
            # signature: a combination that passes or that only shows other
            # violations is never selected. The fewest total candidates win;
            # ties are broken by the numeric lexicographic order of the
            # selected index array, then of the selectedEvents array. The
            # case itself always qualifies.
            minimal_selected, minimal_events, minimal_case_id = min(
                (
                    entry
                    for entry in by_signature[signature]
                    if set(entry[0]).issubset(selected)
                    and set(entry[1]).issubset(selected_events)
                ),
                key=lambda entry: (len(entry[0]) + len(entry[1]), entry[0], entry[1]),
            )
            case["failureReports"] = list(signature)
            case["minimalSelected"] = list(minimal_selected)
            if events_enabled:
                case["minimalSelectedEvents"] = list(minimal_events)
            case["minimalCaseId"] = minimal_case_id

    return {
        "totalCases": len(cases),
        "passedCases": passed,
        "failedCases": len(cases) - passed,
        "cases": cases,
    }
