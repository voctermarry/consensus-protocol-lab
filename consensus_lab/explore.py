"""Bounded enumeration of message-fault and event combinations over a base
scenario.

``explore`` reuses the deterministic simulator for every combination: it
never reads the wall clock, never uses randomness and never creates files,
so identical PLAN input produces byte-identical output.
"""

from __future__ import annotations

import math
from itertools import combinations

from .simulate import ScenarioError, _Simulator, _require_int, parse_scenario

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

# Reports whose violation lists decide a case's status. A report the scenario
# does not enable is absent from the result and plays no part in the verdict.
_CHECKED_REPORTS = (
    "electionSafety",
    "logMatching",
    "stateMachineSafety",
    "linearizability",
    "liveness",
)

_NETWORK_ACTIONS = ("partition", "heal")
_NODE_ACTIONS = ("crash", "restart")
# Union of the fields a faults entry and a nodeEvents entry may carry; the
# per-kind allowed sets narrow this once the candidate is classified.
_EVENT_CANDIDATE_FIELDS = {"time", "action", "groups", "node"}


def _failure_signature(result: dict) -> tuple[str, ...]:
    """Names of the enabled reports whose violation list is non-empty, in
    fixed check-priority order. Two cases share a failure signature exactly
    when they violate the same set of invariants."""
    return tuple(
        key
        for key in _CHECKED_REPORTS
        if key in result and result[key]["violations"]
    )


def _classify_event_candidate(item: object, index: int) -> str:
    """Return ``"network"`` (a ``faults`` entry) or ``"node"`` (a
    ``nodeEvents`` entry) for one eventCandidates item, rejecting items that
    declare neither kind or carry fields that kind cannot hold. Semantic
    constraints (time bounds, node references, partition groups, crash/
    restart alternation) are checked by ``parse_scenario`` on every merge."""
    label = f"eventCandidates[{index}]"
    if not isinstance(item, dict):
        raise ScenarioError(f"{label} must be an object")
    unknown = sorted(set(item) - _EVENT_CANDIDATE_FIELDS)
    if unknown:
        raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
    action = item.get("action")
    if action in _NODE_ACTIONS:
        kind = "node"
        allowed = {"time", "node", "action"}
    elif action in _NETWORK_ACTIONS:
        kind = "network"
        allowed = {"time", "action", "groups"}
    else:
        raise ScenarioError(
            f"{label} must declare a network event (partition/heal) "
            f"or a node event (crash/restart)"
        )
    extra = sorted(set(item) - allowed)
    if extra:
        raise ScenarioError(f"{label} has unknown field(s): {', '.join(extra)}")
    return kind


def run_explore(raw: object) -> dict:
    """Validate a decoded JSON PLAN and run every fault/event combination."""
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

    # eventCandidates and maxEventFaults are an inseparable, optional pair:
    # both omitted keeps the legacy byte-for-byte behavior.
    events_enabled = "eventCandidates" in raw or "maxEventFaults" in raw
    if ("eventCandidates" in raw) != ("maxEventFaults" in raw):
        raise ScenarioError(
            "eventCandidates and maxEventFaults must be provided together"
        )

    event_candidates: list = []
    max_event_faults = 0
    if events_enabled:
        event_candidates = raw["eventCandidates"]
        if not isinstance(event_candidates, list):
            raise ScenarioError("eventCandidates must be a list")
        if not event_candidates:
            raise ScenarioError("eventCandidates must not be empty")
        max_event_faults = _require_int(raw["maxEventFaults"], "maxEventFaults", 0)
        if max_event_faults > len(event_candidates):
            raise ScenarioError(
                "maxEventFaults must not exceed the number of eventCandidates"
            )

    # Validate the base scenario and every message candidate rule with the
    # original scenario validation by presenting the candidates as its
    # messageFaults; this also rejects duplicated full selectors. Each
    # normalized rule keeps its candidate index in its "index" field. This
    # config doubles as the empty-event-combination config.
    combined = dict(scenario_raw)
    combined["messageFaults"] = candidates
    config = parse_scenario(combined)
    rules = config["messageFaults"]

    # Classify each event candidate (network -> appended to faults, node ->
    # appended to nodeEvents) before any merge is built.
    event_kinds = [
        _classify_event_candidate(item, index)
        for index, item in enumerate(event_candidates)
    ]

    # Event combinations in execution order: the empty combination first,
    # then combinations of ascending size; itertools.combinations yields
    # index tuples in numeric lexicographic order. Selected events append to
    # the base scenario's fixed faults/nodeEvents in candidate-index order,
    # so same-time events process base items first, network items before
    # node events as in the simulator's heap ordering.
    event_count = len(event_candidates)
    event_combinations: list[tuple[int, ...]] = [()]
    if events_enabled:
        event_combinations.extend(
            selected
            for size in range(1, max_event_faults + 1)
            for selected in combinations(range(event_count), size)
        )

    # Every merged scenario must validate before any simulation runs. Each
    # event combination is parsed exactly once and then reused for every
    # message combination; message faults cannot affect this validation.
    base_faults = list(scenario_raw.get("faults", []))
    base_node_events = list(scenario_raw.get("nodeEvents", []))
    event_configs: dict[tuple[int, ...], dict] = {(): config}

    def parse_event_config(selected_events: tuple[int, ...]) -> dict:
        merged = dict(scenario_raw)
        selected_faults = [
            event_candidates[i]
            for i in selected_events
            if event_kinds[i] == "network"
        ]
        selected_node_events = [
            event_candidates[i]
            for i in selected_events
            if event_kinds[i] == "node"
        ]
        # Only introduce the key when the base scenario had it or the
        # selection adds that kind of event: nodeEventsProvided gates the
        # online/restartCount report fields, so a combination with no node
        # events must keep looking like the original scenario.
        if "faults" in scenario_raw or selected_faults:
            merged["faults"] = base_faults + selected_faults
        if "nodeEvents" in scenario_raw or selected_node_events:
            merged["nodeEvents"] = base_node_events + selected_node_events
        return parse_scenario(merged)

    # Even when maxEventFaults is 0 (so no event combination selects
    # anything), every candidate must still satisfy its entry constraints on
    # its own, exactly as message candidates are validated regardless of
    # maxFaults. Cross-candidate alternation only matters for combinations
    # that can actually be selected, which the loop below already parses.
    if max_event_faults == 0 and events_enabled:
        for index in range(event_count):
            parse_event_config((index,))
    for selected_events in event_combinations:
        if selected_events == ():
            continue
        event_configs[selected_events] = parse_event_config(selected_events)

    message_total = sum(
        math.comb(len(candidates), size) for size in range(max_faults + 1)
    )
    event_total = sum(
        math.comb(event_count, size) for size in range(max_event_faults + 1)
    ) if events_enabled else 1
    total = message_total * event_total
    if total > max_cases:
        raise ScenarioError(
            f"combination count {total} exceeds maxCases {max_cases}"
        )

    # Message combinations stay the outer loop in their existing order; the
    # event combinations (empty first) form the inner loop. Only populated
    # while minimizing: failure signature -> list of
    # (selected message indices, selected event indices, caseId).
    # Minimization never runs extra simulations; it only references cases
    # enumerated here, so those references do not count against maxCases.
    cases = []
    by_signature: dict[
        tuple[str, ...], list[tuple[tuple[int, ...], tuple[int, ...], int]]
    ] = {}
    passed = 0
    case_id = 0
    for size in range(max_faults + 1):
        for selected in combinations(range(len(candidates)), size):
            for selected_events in event_combinations:
                case_config = dict(event_configs[selected_events])
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
                }
                if events_enabled:
                    case["selectedEvents"] = list(selected_events)
                case["status"] = status
                case["result"] = result
                if minimize_failures and signature:
                    by_signature.setdefault(signature, []).append(
                        (selected, selected_events, case_id)
                    )
                cases.append(case)
                case_id += 1

    if minimize_failures:
        for case in cases:
            if case["status"] != "failed":
                continue
            signature = _failure_signature(case["result"])
            selected = tuple(case["selected"])
            selected_events = (
                tuple(case["selectedEvents"]) if events_enabled else ()
            )
            # Every minimizing combination must be reachable by deleting
            # candidates of either kind from this case (sub-combinations of
            # its selected indices) and reproduce its exact failure
            # signature: a combination that passes or that only shows other
            # violations is never selected. Fewer selected candidates win;
            # ties break on the numeric lexicographic order of selected,
            # then of selectedEvents. The case itself always qualifies.
            minimal_selected, minimal_events, minimal_case_id = min(
                (
                    entry
                    for entry in by_signature[signature]
                    if set(entry[0]).issubset(selected)
                    and set(entry[1]).issubset(selected_events)
                ),
                key=lambda entry: (
                    len(entry[0]) + len(entry[1]),
                    entry[0],
                    entry[1],
                ),
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
