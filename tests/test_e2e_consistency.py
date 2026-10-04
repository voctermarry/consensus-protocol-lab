"""End-to-end consistency between the explore, simulate and replay entries.

Every case explore enumerates is rebuilt here as a standalone simulate
scenario purely through the public rules (selected message-fault candidates
become ``messageFaults``, selected event candidates are appended to the base
``faults``/``nodeEvents``), and the independently computed result must equal
``case.result`` under a full JSON deep comparison. Each rebuilt result is
then handed to replay, which must report ``matched``; tampering with a
single nested timeline field must flip replay to ``mismatched`` with the
RFC 6901 pointer of the first difference.

All fixtures are fixed literals: no wall clock, no randomness, no file
traversal order and no state shared between tests.
"""

from __future__ import annotations

import copy
import json

from consensus_lab.cli import main
from consensus_lab.replay import _first_difference


# -- fixed PLAN fixtures ------------------------------------------------------


def _pre_vote_plan():
    """Message-fault-only enumeration over a preVote scenario.

    Seven combinations: the empty one, three singletons and three pairs.
    Dropping both first-round preVoteReply grants to ``a`` (case [0, 1])
    delays the election past the L1 deadline, so passed and failed cases
    both appear; the client command w1 commits in every healthy-enough
    case.
    """
    return {
        "scenario": {
            "nodes": ["a", "b", "c"],
            "duration": 600,
            "electionTimeouts": {"a": 100, "b": 150, "c": 200},
            "heartbeatInterval": 50,
            "messageDelay": 10,
            "preVote": True,
            "clientCommands": [
                {"time": 300, "node": "a", "id": "w1", "command": "x"}
            ],
            "livenessChecks": [
                {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": 160},
                {"id": "C1", "type": "clientCommitted",
                 "startTime": 300, "deadline": 500, "target": "w1"},
            ],
        },
        "candidates": [
            {"from": "b", "to": "a", "message": "preVoteReply",
             "occurrence": 1, "action": "drop"},
            {"from": "c", "to": "a", "message": "preVoteReply",
             "occurrence": 1, "action": "drop"},
            {"from": "b", "to": "a", "message": "voteReply",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 2,
        "maxCases": 100,
    }


def _event_cartesian_plan():
    """Message faults x event candidates (partition, crash) cartesian product.

    Three message combinations times four event combinations give twelve
    cases. Isolating leader ``a`` at t=180 strands the uncommitted command
    w1 past the C1 deadline, so every partition case fails while the rest
    pass.
    """
    return {
        "scenario": {
            "nodes": ["a", "b", "c"],
            "duration": 500,
            "electionTimeouts": {"a": 100, "b": 300, "c": 400},
            "heartbeatInterval": 50,
            "messageDelay": 10,
            "clientCommands": [
                {"time": 200, "node": "a", "id": "w1", "command": "x"}
            ],
            "livenessChecks": [
                {"id": "C1", "type": "clientCommitted",
                 "startTime": 200, "deadline": 350, "target": "w1"},
            ],
        },
        "candidates": [
            {"from": "b", "to": "a", "message": "appendReply",
             "occurrence": 1, "action": "drop"},
            {"from": "a", "to": "c", "message": "heartbeat",
             "occurrence": 2, "action": "delay", "delay": 40},
        ],
        "maxFaults": 1,
        "maxCases": 100,
        "eventCandidates": [
            {"network": {"time": 180, "action": "partition",
                         "groups": [["a"], ["b", "c"]]}},
            {"node": {"time": 220, "node": "c", "action": "crash"}},
        ],
        "maxEventFaults": 2,
    }


# -- helpers ------------------------------------------------------------------


def _write_json(directory, name, value):
    path = directory / name
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _run(capsys, argv):
    code = main(argv)
    out, err = capsys.readouterr()
    return code, out, err


def _run_explore_ok(tmp_path, capsys, plan):
    path = _write_json(tmp_path, "plan.json", plan)
    code, out, err = _run(capsys, ["explore", path])
    assert code == 0 and err == ""
    return json.loads(out)


def _run_simulate_ok(tmp_path, capsys, scenario, name="scenario.json"):
    path = _write_json(tmp_path, name, scenario)
    code, out, err = _run(capsys, ["simulate", path])
    assert code == 0 and err == ""
    return out


def _assert_deep_equal(expected, actual):
    """Full JSON deep comparison: object member order is irrelevant, array
    order (hence timeline order and global seq) is significant."""
    diff = _first_difference(expected, actual, [])
    assert diff is None, f"first difference at {diff!r}"


def _placeholder_rule(nodes, index):
    """A message-fault rule that can never fire: no real send is ever the
    1_000_000th occurrence of its selector. Padding non-selected candidate
    positions with such rules keeps every selected candidate at its original
    candidate index, which is what the ``rule`` field of explore's
    ``messageFault`` events refers to."""
    return {
        "from": nodes[0],
        "to": nodes[1],
        "message": "heartbeat",
        "occurrence": 1_000_000 + index,
        "action": "drop",
    }


def _rebuild_scenario(plan, case):
    """Rebuild the standalone simulate scenario for one explore case using
    only the public rules: the selected candidates become the scenario's
    ``messageFaults`` (in candidate-index order, at their original indices),
    and the selected event candidates are appended to the base ``faults``
    (network) and ``nodeEvents`` (node) collections in candidate-index
    order."""
    scenario = copy.deepcopy(plan["scenario"])
    candidates = plan["candidates"]
    selected = set(case["selected"])
    scenario["messageFaults"] = [
        candidate if index in selected else _placeholder_rule(scenario["nodes"], index)
        for index, candidate in enumerate(candidates)
    ]
    if "selectedEvents" in case:
        event_candidates = plan["eventCandidates"]
        network = [
            event_candidates[i]["network"]
            for i in case["selectedEvents"]
            if "network" in event_candidates[i]
        ]
        node = [
            event_candidates[i]["node"]
            for i in case["selectedEvents"]
            if "node" in event_candidates[i]
        ]
        if network or "faults" in scenario:
            scenario["faults"] = list(scenario.get("faults", [])) + network
        if node or "nodeEvents" in scenario:
            scenario["nodeEvents"] = list(scenario.get("nodeEvents", [])) + node
    return scenario


def _reverse_members(value):
    """Recursively reverse every object's member order. Deterministic; the
    JSON value is unchanged under the order-insensitive deep comparison."""
    if isinstance(value, dict):
        return {key: _reverse_members(value[key]) for key in reversed(list(value))}
    if isinstance(value, list):
        return [_reverse_members(item) for item in value]
    return value


def _assert_case_rebuilds(tmp_path, capsys, plan, case):
    """One explore case -> standalone simulate -> deep equality, with
    byte-identical standard output across repeated identical runs."""
    case_dir = tmp_path / f"case{case['caseId']}"
    case_dir.mkdir()
    scenario = _rebuild_scenario(plan, case)
    out1 = _run_simulate_ok(case_dir, capsys, scenario)
    out2 = _run_simulate_ok(case_dir, capsys, scenario)
    assert out1 == out2
    _assert_deep_equal(case["result"], json.loads(out1))
    return scenario, out1


# -- explore <-> simulate consistency -----------------------------------------


def test_message_fault_only_cases_rebuild_to_identical_simulation(tmp_path, capsys):
    plan = _pre_vote_plan()
    summary = _run_explore_ok(tmp_path, capsys, plan)

    # Enumeration shape: empty combination, singletons, then pairs in
    # numeric lexicographic order; no event fields without eventCandidates.
    assert summary["totalCases"] == 7
    assert [case["caseId"] for case in summary["cases"]] == list(range(7))
    assert [case["selected"] for case in summary["cases"]] == [
        [], [0], [1], [2], [0, 1], [0, 2], [1, 2],
    ]
    assert all("selectedEvents" not in case for case in summary["cases"])
    # Both outcomes appear: exactly the pair dropping both first-round
    # preVoteReply grants misses the election deadline.
    assert {case["status"] for case in summary["cases"]} == {"passed", "failed"}
    assert summary["passedCases"] + summary["failedCases"] == 7

    for case in summary["cases"]:
        result = case["result"]
        # The enabled optional features are visible in every case: preVote
        # traffic, the liveness report and the client summary.
        assert any(
            event["type"] == "messageSend" and event["message"] == "preVote"
            for event in result["timeline"]
        )
        assert "liveness" in result and len(result["liveness"]["checks"]) == 2
        assert result["clients"]["committed"] or result["clients"]["rejected"]
        # Global seq is strictly increasing along the timeline.
        seqs = [event["seq"] for event in result["timeline"]]
        assert seqs == sorted(set(seqs))
        # Selected rules that fired keep their original candidate index.
        fired = [e["rule"] for e in result["timeline"] if e["type"] == "messageFault"]
        assert set(fired) <= set(case["selected"])

        _assert_case_rebuilds(tmp_path, capsys, plan, case)


def test_event_cartesian_cases_rebuild_to_identical_simulation(tmp_path, capsys):
    plan = _event_cartesian_plan()
    summary = _run_explore_ok(tmp_path, capsys, plan)

    # 3 message combinations x 4 event combinations, message combos outer.
    assert summary["totalCases"] == 12
    assert [case["caseId"] for case in summary["cases"]] == list(range(12))
    assert [(case["selected"], case["selectedEvents"]) for case in summary["cases"]] == [
        ([], []), ([], [0]), ([], [1]), ([], [0, 1]),
        ([0], []), ([0], [0]), ([0], [1]), ([0], [0, 1]),
        ([1], []), ([1], [0]), ([1], [1]), ([1], [0, 1]),
    ]
    # Every partition case strands the uncommitted command past its deadline.
    assert {case["status"] for case in summary["cases"]} == {"passed", "failed"}
    assert summary["passedCases"] + summary["failedCases"] == 12

    for case in summary["cases"]:
        result = case["result"]
        assert "liveness" in result
        events = case["selectedEvents"]
        fault_events = [e for e in result["timeline"] if e["type"] == "fault"]
        lifecycle = [e for e in result["timeline"] if e["type"] == "nodeLifecycle"]
        # The selected event candidates materialize in the timeline exactly
        # when chosen: candidate 0 is the partition, candidate 1 the crash.
        assert any(e["action"] == "partition" for e in fault_events) == (0 in events)
        assert any(e["action"] == "crash" for e in lifecycle) == (1 in events)
        seqs = [event["seq"] for event in result["timeline"]]
        assert seqs == sorted(set(seqs))

        _assert_case_rebuilds(tmp_path, capsys, plan, case)


def test_explore_output_is_byte_identical_across_runs(tmp_path, capsys):
    for plan in (_pre_vote_plan(), _event_cartesian_plan()):
        path = _write_json(tmp_path, "plan.json", plan)
        code1, out1, err1 = _run(capsys, ["explore", path])
        code2, out2, err2 = _run(capsys, ["explore", path])
        assert code1 == code2 == 0
        assert err1 == err2 == ""
        assert out1 == out2


def test_object_member_order_does_not_affect_the_comparison(tmp_path, capsys):
    plan = _event_cartesian_plan()
    summary = _run_explore_ok(tmp_path, capsys, plan)
    case = next(c for c in summary["cases"] if c["selectedEvents"] == [0, 1])

    # The same rebuilt scenario with every object's members reversed still
    # deep-equals the explore case result.
    scenario = _rebuild_scenario(plan, case)
    out = _run_simulate_ok(tmp_path, capsys, _reverse_members(scenario))
    _assert_deep_equal(case["result"], json.loads(out))

    # And replay still matches a result file whose members are reversed.
    scenario_path = _write_json(tmp_path, "scenario.json", scenario)
    result_path = _write_json(tmp_path, "result.json", _reverse_members(case["result"]))
    code, out, err = _run(capsys, ["replay", scenario_path, result_path])
    assert code == 0 and err == ""
    assert out == '{"status":"matched"}\n'


# -- rebuilt results through replay --------------------------------------------


def _cases_with_scenarios(tmp_path, capsys):
    """Yield (label, scenario, result) for every case of both plans."""
    for label, plan in (("prevote", _pre_vote_plan()), ("events", _event_cartesian_plan())):
        plan_dir = tmp_path / label
        plan_dir.mkdir()
        summary = _run_explore_ok(plan_dir, capsys, plan)
        for case in summary["cases"]:
            scenario = _rebuild_scenario(plan, case)
            yield f"{label}#{case['caseId']}", scenario, case["result"]


def test_replay_matches_every_rebuilt_simulate_result(tmp_path, capsys):
    seen = 0
    for label, scenario, result in _cases_with_scenarios(tmp_path, capsys):
        case_dir = tmp_path / label
        case_dir.mkdir()
        scenario_path = _write_json(case_dir, "scenario.json", scenario)
        result_path = _write_json(case_dir, "result.json", result)
        code, out, err = _run(capsys, ["replay", scenario_path, result_path])
        assert code == 0 and err == "", label
        assert out == '{"status":"matched"}\n', label
        seen += 1
    assert seen == 7 + 12


def test_replay_reports_a_single_tampered_nested_timeline_field(tmp_path, capsys):
    checked = 0
    for label, scenario, result in _cases_with_scenarios(tmp_path, capsys):
        # Only cases that actually recorded a messageFault event give a
        # nested timeline field worth tampering with here.
        fault_index = next(
            (i for i, e in enumerate(result["timeline"]) if e["type"] == "messageFault"),
            None,
        )
        if fault_index is None:
            continue
        tampered = copy.deepcopy(result)
        original = tampered["timeline"][fault_index]["rule"]
        tampered["timeline"][fault_index]["rule"] = original + 100

        case_dir = tmp_path / label
        case_dir.mkdir()
        scenario_path = _write_json(case_dir, "scenario.json", scenario)
        result_path = _write_json(case_dir, "result.json", tampered)
        code, out, err = _run(capsys, ["replay", scenario_path, result_path])
        assert code == 1 and err == "", label
        report = json.loads(out)
        assert report["status"] == "mismatched"
        # The first difference is exactly the tampered nested field.
        assert report["path"] == f"/timeline/{fault_index}/rule"
        assert report["expectedPresent"] is True
        assert report["actualPresent"] is True
        assert report["expected"] == original
        assert report["actual"] == original + 100
        checked += 1
    # Both plans contribute faulted cases, including pairs whose rule
    # indices (0 and 2) differ from their list positions.
    assert checked > 0


def test_replay_reports_a_tampered_nested_event_field(tmp_path, capsys):
    plan = _event_cartesian_plan()
    summary = _run_explore_ok(tmp_path, capsys, plan)
    case = next(c for c in summary["cases"] if c["selectedEvents"] == [0, 1])
    result = case["result"]
    crash_index = next(
        i for i, e in enumerate(result["timeline"])
        if e["type"] == "nodeLifecycle" and e["action"] == "crash"
    )
    tampered = copy.deepcopy(result)
    tampered["timeline"][crash_index]["node"] = "b"

    scenario_path = _write_json(tmp_path, "scenario.json", _rebuild_scenario(plan, case))
    result_path = _write_json(tmp_path, "result.json", tampered)
    code, out, err = _run(capsys, ["replay", scenario_path, result_path])
    assert code == 1 and err == ""
    report = json.loads(out)
    assert report["status"] == "mismatched"
    assert report["path"] == f"/timeline/{crash_index}/node"
    assert report["expected"] == "c"
    assert report["actual"] == "b"


# -- maxCases overflow is a clean, partial-result-free failure -----------------


def test_combinations_exceeding_max_cases_fail_cleanly(tmp_path, capsys):
    # Message-only enumeration: 7 combinations > 6.
    plan = _pre_vote_plan()
    plan["maxCases"] = 6
    path = _write_json(tmp_path, "plan.json", plan)
    code, out, err = _run(capsys, ["explore", path])
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1
    assert "exceeds maxCases" in err
    # Nothing that could be mistaken for a partial result is left behind.
    assert [p.name for p in tmp_path.iterdir()] == ["plan.json"]


def test_cartesian_product_exceeding_max_cases_fails_cleanly(tmp_path, capsys):
    # Full cartesian product counts: 3 message combinations x 4 event
    # combinations = 12 > 11.
    plan = _event_cartesian_plan()
    plan["maxCases"] = 11
    path = _write_json(tmp_path, "plan.json", plan)
    code, out, err = _run(capsys, ["explore", path])
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1
    assert "combination count 12 exceeds maxCases 11" in err
    assert [p.name for p in tmp_path.iterdir()] == ["plan.json"]
