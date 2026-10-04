"""End-to-end consistency across the explore, simulate and replay entry points.

The same fault combination must produce exactly one deterministic result no
matter which public entry point computes it: explore enumerates a
combination, simulate recomputes it from a scenario rebuilt out of the case's
``selected`` / ``selectedEvents`` indices using the documented merge rules,
and replay reproduces the saved simulate result field by field.

The fixtures are fixed virtual-time scenarios: no wall clock, no randomness,
no reliance on directory iteration order and no state shared across runs.
"""

from __future__ import annotations

import copy
import json

from consensus_lab.cli import main


# -- fixtures ---------------------------------------------------------------


def _base_scenario(**overrides):
    # a wins the first election (timeout t=100), leads term 1 and can commit
    # the client command w1 (accepted at t=140, committed at t=160).
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 350,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 10,
        "clientCommands": [
            {"time": 140, "node": "a", "id": "w1", "command": "set x=1"}
        ],
        "livenessChecks": [
            {"id": "k1", "type": "clientCommitted",
             "startTime": 140, "deadline": 175, "target": "w1"}
        ],
    }
    scenario.update(overrides)
    return scenario


def _chain_candidates(count):
    # One selector (a -> b, appendEntries), one rule per occurrence. The
    # occurrences form a chain: after a drop the leader retries 50ms later, so
    # occurrence k+1 is ever sent only when occurrence k was dropped by the
    # rule at candidate index k-1. A rebuilt standalone scenario therefore
    # numbers its positional messageFaults rules exactly as explore numbers
    # the original candidate indices, keeping every messageFault event (and
    # its "rule" field) identical between the two computations.
    return [
        {"from": "a", "to": "b", "message": "appendEntries",
         "occurrence": occurrence, "action": "drop"}
        for occurrence in range(1, count + 1)
    ]


def _pre_vote_plan():
    # Message-fault-only enumeration with preVote and a liveness check.
    # c crashes after the command is accepted, so a plus b is the remaining
    # majority: dropping a -> b appendEntries pushes the commit to t=210 and
    # past the t=175 deadline, while selections without candidate 0 commit in
    # time. Both passing and failing cases occur among the eight combinations.
    scenario = _base_scenario(
        preVote=True,
        nodeEvents=[{"time": 145, "node": "c", "action": "crash"}],
    )
    return {
        "scenario": scenario,
        "candidates": _chain_candidates(3),
        "maxFaults": 3,
        "maxCases": 100,
    }


_PRE_VOTE_FAILED = {(0,), (0, 1), (0, 2), (0, 1, 2)}
_PRE_VOTE_PASSED = {(), (1,), (2,), (1, 2)}


def _cartesian_plan(**overrides):
    # Message faults form a cartesian product with event candidates: a
    # partition isolating c at t=150 and a crash of c at t=150. Both events
    # only touch c, so the a -> b occurrence chain behaves exactly as in the
    # message-only plan. Without an event, one dropped replication to b is
    # retried in time for the t=175 deadline; together with any event the same
    # commit path fails liveness (10 passed / 6 failed of 16 cases).
    plan = {
        "scenario": _base_scenario(),
        "candidates": _chain_candidates(2),
        "maxFaults": 2,
        "maxCases": 100,
        "eventCandidates": [
            {"network": {"time": 150, "action": "partition",
                         "groups": [["c"], ["a", "b"]]}},
            {"node": {"time": 150, "node": "c", "action": "crash"}},
        ],
        "maxEventFaults": 2,
    }
    plan.update(overrides)
    return plan


_CARTESIAN_FAILED_EVENTS = {(0,), (1,), (0, 1)}


# -- helpers ----------------------------------------------------------------


def _shuffled(value):
    """Recursively reverse object member order; array order is preserved. A
    deep comparison must not depend on JSON object member ordering."""
    if isinstance(value, dict):
        return {key: _shuffled(value[key]) for key in reversed(list(value))}
    if isinstance(value, list):
        return [_shuffled(item) for item in value]
    return value


def _write_json(tmp_path, name, value, *, shuffled=False):
    path = tmp_path / name
    payload = _shuffled(value) if shuffled else value
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _run(capsys, argv):
    code = main(argv)
    out, err = capsys.readouterr()
    return code, out, err


def _run_explore(tmp_path, capsys, plan):
    path = _write_json(tmp_path, "plan.json", plan)
    code, out, err = _run(capsys, ["explore", path])
    assert code == 0, err
    assert err == ""
    return json.loads(out)


def _rebuild_scenario(plan, case):
    """Apply the documented merge rules: selected candidate rules become the
    scenario's messageFaults in candidate-index order, and selected event
    candidates are appended in candidate-index order to the base faults /
    nodeEvents collections."""
    scenario = copy.deepcopy(plan["scenario"])
    scenario["messageFaults"] = [
        copy.deepcopy(plan["candidates"][i]) for i in case["selected"]
    ]
    events = plan.get("eventCandidates", [])
    for i in case.get("selectedEvents", []):
        candidate = events[i]
        if "network" in candidate:
            scenario.setdefault("faults", []).append(
                copy.deepcopy(candidate["network"])
            )
        else:
            scenario.setdefault("nodeEvents", []).append(
                copy.deepcopy(candidate["node"])
            )
    return scenario


def _simulate_independently(tmp_path, capsys, scenario, *, shuffled=False):
    """Run simulate as a standalone entry point twice; identical input must
    yield byte-identical stdout. Returns the parsed result."""
    path = _write_json(tmp_path, "scenario.json", scenario, shuffled=shuffled)
    code1, out1, err1 = _run(capsys, ["simulate", path])
    assert code1 == 0, err1
    assert err1 == ""
    code2, out2, err2 = _run(capsys, ["simulate", path])
    assert code2 == 0, err2 == ""
    assert out2 == out1
    return json.loads(out1)


def _assert_full_result_equivalence(simulated, case):
    expected = case["result"]
    # Whole-structure deep comparison: object member order is irrelevant to
    # the parsed Python values, arrays must match index by index.
    assert simulated == expected

    # Timeline event order and the global, gap-free seq.
    sim_seq = [event["seq"] for event in simulated["timeline"]]
    assert sim_seq == list(range(1, len(sim_seq) + 1))
    assert [event["seq"] for event in expected["timeline"]] == sim_seq
    assert [
        (event["time"], event["type"]) for event in simulated["timeline"]
    ] == [(event["time"], event["type"]) for event in expected["timeline"]]

    # Final node state and the client summary.
    assert simulated["nodes"] == expected["nodes"]
    assert simulated["clients"] == expected["clients"]

    # Every report the scenario enables is present and identical; the
    # scenarios never enable reads or membership, so those reports stay
    # absent.
    for report in ("electionSafety", "logMatching",
                   "stateMachineSafety", "liveness"):
        assert simulated[report] == expected[report]
    for absent in ("reads", "linearizability", "membership"):
        assert absent not in simulated
        assert absent not in expected


def _replay_must_match(tmp_path, capsys, scenario, result, *, shuffled=False):
    scenario_path = _write_json(
        tmp_path, "replay_scenario.json", scenario, shuffled=shuffled
    )
    result_path = _write_json(
        tmp_path, "replay_result.json", result, shuffled=shuffled
    )
    code, out, err = _run(capsys, ["replay", scenario_path, result_path])
    assert code == 0, err
    assert err == ""
    assert out == '{"status":"matched"}\n'


def _verify_case(tmp_path, capsys, plan, case, *, shuffled=False):
    scenario = _rebuild_scenario(plan, case)
    simulated = _simulate_independently(
        tmp_path, capsys, scenario, shuffled=shuffled
    )
    _assert_full_result_equivalence(simulated, case)
    _replay_must_match(
        tmp_path, capsys, scenario, simulated, shuffled=shuffled
    )
    return simulated


# -- message-fault-only enumeration -----------------------------------------


def test_message_fault_plan_cases_match_independent_simulate_and_replay(
    tmp_path, capsys
):
    plan = _pre_vote_plan()
    summary = _run_explore(tmp_path, capsys, plan)

    assert summary["totalCases"] == 8
    assert summary["passedCases"] == 4
    assert summary["failedCases"] == 4

    seen_passed, seen_failed = set(), set()
    for case in summary["cases"]:
        # Omitting eventCandidates preserves the legacy case shape.
        assert "selectedEvents" not in case
        selected = tuple(case["selected"])
        (seen_passed if case["status"] == "passed" else seen_failed).add(
            selected
        )
        # The verdict comes from the liveness report only; every other
        # invariant report stays clean in passing and failing cases alike.
        assert case["result"]["electionSafety"]["violations"] == []
        assert case["result"]["logMatching"]["violations"] == []
        assert case["result"]["stateMachineSafety"]["violations"] == []
        _verify_case(tmp_path, capsys, plan, case)

    assert seen_passed == _PRE_VOTE_PASSED
    assert seen_failed == _PRE_VOTE_FAILED


# -- cartesian product with network and node event candidates ---------------


def test_cartesian_plan_cases_match_independent_simulate_and_replay(
    tmp_path, capsys
):
    plan = _cartesian_plan()
    summary = _run_explore(tmp_path, capsys, plan)

    assert summary["totalCases"] == 16
    assert summary["passedCases"] == 10
    assert summary["failedCases"] == 6

    pairs = []
    for case in summary["cases"]:
        selected = tuple(case["selected"])
        selected_events = tuple(case["selectedEvents"])
        pairs.append((selected, selected_events))
        # No preVote: no pre-candidate roles and no pre-vote traffic.
        assert not [
            event
            for event in case["result"]["timeline"]
            if event.get("role") == "preCandidate"
            or event.get("message") in ("preVote", "preVoteReply")
        ]
        # A case fails liveness exactly when candidate 0 is selected together
        # with at least one event; every other combination passes.
        expect_failed = (
            selected == (0,) or selected == (0, 1)
        ) and selected_events in _CARTESIAN_FAILED_EVENTS
        assert (case["status"] == "failed") == expect_failed
        if expect_failed:
            assert case["result"]["liveness"]["violations"]
        else:
            assert case["result"]["liveness"]["violations"] == []
        _verify_case(tmp_path, capsys, plan, case)

    # Enumeration order: message combinations outer, events inner in the
    # documented ascending-size / numeric lexicographic order.
    assert pairs == [
        (selected, events)
        for selected in ((), (0,), (1,), (0, 1))
        for events in ((), (0,), (1,), (0, 1))
    ]

    # The selected event candidates really appear in the rebuilt simulation:
    # the partition fault and the c crash lifecycle event show up in the case
    # that selects both, and absent events leave no trace.
    both = next(
        case
        for case in summary["cases"]
        if case["selected"] == [] and case["selectedEvents"] == [0, 1]
    )
    timeline = both["result"]["timeline"]
    assert [
        (event["time"], event["action"])
        for event in timeline
        if event["type"] == "fault"
    ] == [(150, "partition")]
    assert [
        (event["time"], event["node"], event["action"])
        for event in timeline
        if event["type"] == "nodeLifecycle"
    ] == [(150, "c", "crash")]
    assert both["result"]["nodes"]["c"]["online"] is False


# -- replay flags a single tampered timeline field --------------------------


def test_replay_detects_one_tampered_nested_timeline_field(
    tmp_path, capsys
):
    # Take one failing preVote case, reproduce it via an independent simulate,
    # then alter exactly one nested timeline field. Replay must mismatch and
    # point at the first difference with a correct RFC 6901 pointer.
    plan = _pre_vote_plan()
    summary = _run_explore(tmp_path, capsys, plan)
    case = next(
        case for case in summary["cases"] if tuple(case["selected"]) == (0,)
    )
    scenario = _rebuild_scenario(plan, case)
    result = _simulate_independently(tmp_path, capsys, scenario)
    assert result == case["result"]

    tampered = copy.deepcopy(result)
    tampered["timeline"][0]["seq"] = result["timeline"][0]["seq"] + 1000
    scenario_path = _write_json(tmp_path, "tamper_scenario.json", scenario)
    result_path = _write_json(tmp_path, "tamper_result.json", tampered)
    code, out, err = _run(
        capsys, ["replay", scenario_path, result_path]
    )
    assert code == 1
    assert err == ""
    report = json.loads(out)
    assert report["status"] == "mismatched"
    assert report["path"] == "/timeline/0/seq"
    assert report["expectedPresent"] is True
    assert report["actualPresent"] is True
    assert report["expected"] == result["timeline"][0]["seq"]
    assert report["actual"] == result["timeline"][0]["seq"] + 1000


def test_replay_detects_tampered_timeline_field_deep_in_an_event(
    tmp_path, capsys
):
    # A non-seq nested field (a peer name inside a timeline messageSend event)
    # must be reported with the same pointer discipline.
    plan = _cartesian_plan()
    summary = _run_explore(tmp_path, capsys, plan)
    case = next(
        case
        for case in summary["cases"]
        if case["selected"] == [0] and case["selectedEvents"] == [1]
    )
    scenario = _rebuild_scenario(plan, case)
    result = _simulate_independently(tmp_path, capsys, scenario)

    send_index = next(
        index
        for index, event in enumerate(result["timeline"])
        if event["type"] == "messageSend" and event["message"] == "appendEntries"
    )
    tampered = copy.deepcopy(result)
    tampered["timeline"][send_index]["peer"] = "c"
    scenario_path = _write_json(tmp_path, "deep_scenario.json", scenario)
    result_path = _write_json(tmp_path, "deep_result.json", tampered)
    code, out, err = _run(
        capsys, ["replay", scenario_path, result_path]
    )
    assert code == 1
    assert err == ""
    report = json.loads(out)
    assert report["status"] == "mismatched"
    assert report["path"] == f"/timeline/{send_index}/peer"
    assert report["actual"] == "c"


# -- object member order must not affect equality ---------------------------


def test_shuffled_member_order_still_deep_equal_and_replays_matched(
    tmp_path, capsys
):
    # Write the rebuilt scenario and the saved result with every object's
    # member order reversed; the deep comparison and replay must be unaffected.
    plan = _cartesian_plan()
    summary = _run_explore(tmp_path, capsys, plan)
    case = next(
        case
        for case in summary["cases"]
        if case["selected"] == [0, 1] and case["selectedEvents"] == [0, 1]
    )
    _verify_case(tmp_path, capsys, plan, case, shuffled=True)


# -- determinism of the explore entry point itself --------------------------


def test_explore_stdout_is_byte_identical_across_runs(tmp_path, capsys):
    for plan in (_pre_vote_plan(), _cartesian_plan()):
        path = _write_json(tmp_path, "plan.json", plan)
        code1, out1, err1 = _run(capsys, ["explore", path])
        code2, out2, err2 = _run(capsys, ["explore", path])
        assert (code1, err1) == (0, "")
        assert (code2, err2) == (0, "")
        assert out2 == out1


# -- deterministic failure when the combination count exceeds maxCases ------


def test_explore_fails_when_combination_count_exceeds_max_cases(
    tmp_path, capsys
):
    # Four message combinations x four event combinations = 16 cases; a limit
    # of 15 is rejected before any simulation runs.
    plan = _cartesian_plan(maxCases=15)
    path = _write_json(tmp_path, "plan.json", plan)
    code, out, err = _run(capsys, ["explore", path])
    assert code == 2
    assert out == ""
    assert err == "error: combination count 16 exceeds maxCases 15\n"

    # No partial result artefact is left behind: the only file in the working
    # directory is the input plan itself.
    entries = sorted(entry.name for entry in tmp_path.iterdir())
    assert entries == ["plan.json"]


def test_explore_message_only_count_exceeding_max_cases_fails_cleanly(
    tmp_path, capsys
):
    # Eight message combinations against a limit of seven: one single error
    # line, empty stdout, exit code 2, and no result files created.
    plan = _pre_vote_plan()
    plan["maxCases"] = 7
    path = _write_json(tmp_path, "plan.json", plan)
    code, out, err = _run(capsys, ["explore", path])
    assert code == 2
    assert out == ""
    assert err == "error: combination count 8 exceeds maxCases 7\n"
    assert sorted(entry.name for entry in tmp_path.iterdir()) == ["plan.json"]
