"""Tests for the explore subcommand's bounded fault-combination enumeration."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main


def _write_plan(tmp_path, plan):
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan), encoding="utf-8")
    return str(path)


def _base_scenario(**overrides):
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 500,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 10,
    }
    scenario.update(overrides)
    return scenario


def _candidate(occurrence, **overrides):
    rule = {
        "from": "a",
        "to": "b",
        "message": "heartbeat",
        "occurrence": occurrence,
        "action": "drop",
    }
    rule.update(overrides)
    return rule


def _base_plan(**overrides):
    plan = {
        "scenario": _base_scenario(),
        "candidates": [_candidate(1)],
        "maxFaults": 1,
        "maxCases": 100,
    }
    plan.update(overrides)
    return plan


def _run(tmp_path, capsys, plan):
    path = _write_plan(tmp_path, plan)
    code = main(["explore", path])
    out, err = capsys.readouterr()
    return code, out, err


def _run_ok(tmp_path, capsys, plan):
    code, out, err = _run(tmp_path, capsys, plan)
    assert code == 0 and err == ""
    return json.loads(out)


def _run_error(tmp_path, capsys, plan):
    code, out, err = _run(tmp_path, capsys, plan)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1
    return err


def test_enumeration_order_and_counts(tmp_path, capsys):
    # Three candidates, at most two faults: the empty combination, then
    # singletons, then pairs in numeric lexicographic order. High occurrence
    # numbers never match a send, so every case passes.
    plan = _base_plan(
        candidates=[_candidate(91), _candidate(92), _candidate(93)],
        maxFaults=2,
    )
    summary = _run_ok(tmp_path, capsys, plan)
    assert summary["totalCases"] == 7
    assert summary["passedCases"] == 7
    assert summary["failedCases"] == 0
    assert [case["caseId"] for case in summary["cases"]] == list(range(7))
    assert [case["selected"] for case in summary["cases"]] == [
        [],
        [0],
        [1],
        [2],
        [0, 1],
        [0, 2],
        [1, 2],
    ]
    assert all(case["status"] == "passed" for case in summary["cases"])
    assert all("timeline" in case["result"] for case in summary["cases"])


def test_max_faults_zero_runs_only_the_no_fault_case(tmp_path, capsys):
    plan = _base_plan(maxFaults=0)
    summary = _run_ok(tmp_path, capsys, plan)
    assert summary["totalCases"] == 1
    assert summary["cases"][0]["selected"] == []
    assert summary["cases"][0]["status"] == "passed"


def test_selected_rules_are_injected(tmp_path, capsys):
    # The candidate drops the first heartbeat a -> b; only the case that
    # selects it may record a messageFault event.
    plan = _base_plan()
    summary = _run_ok(tmp_path, capsys, plan)
    assert summary["totalCases"] == 2
    by_selection = {tuple(case["selected"]): case for case in summary["cases"]}
    plain = by_selection[()]["result"]["timeline"]
    faulted = by_selection[(0,)]["result"]["timeline"]
    assert not [e for e in plain if e["type"] == "messageFault"]
    fault_events = [e for e in faulted if e["type"] == "messageFault"]
    assert len(fault_events) == 1
    assert fault_events[0]["rule"] == 0
    assert fault_events[0]["action"] == "drop"


def test_violations_mark_cases_failed_without_stopping(tmp_path, capsys):
    # A leaderElected check with a deadline before any election timeout fails
    # in every case; exploration still runs all cases and exits 0.
    scenario = _base_scenario(
        livenessChecks=[
            {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": 50}
        ]
    )
    plan = _base_plan(scenario=scenario, candidates=[_candidate(91), _candidate(92)], maxFaults=1)
    code, out, err = _run(tmp_path, capsys, plan)
    assert code == 0 and err == ""
    summary = json.loads(out)
    assert summary["totalCases"] == 3
    assert summary["passedCases"] == 0
    assert summary["failedCases"] == 3
    assert all(case["status"] == "failed" for case in summary["cases"])
    for case in summary["cases"]:
        assert case["result"]["liveness"]["violations"]


def test_output_is_byte_identical_across_runs(tmp_path, capsys):
    plan = _base_plan(candidates=[_candidate(1), _candidate(2)], maxFaults=2)
    code1, out1, err1 = _run(tmp_path, capsys, plan)
    code2, out2, err2 = _run(tmp_path, capsys, plan)
    assert code1 == code2 == 0 and err1 == err2 == ""
    assert out1 == out2


def test_plan_must_be_an_object(tmp_path, capsys):
    path = tmp_path / "plan.json"
    path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    code = main(["explore", str(path)])
    out, err = capsys.readouterr()
    assert code == 2 and out == ""
    assert err.startswith("error: ")


def test_unknown_and_missing_fields(tmp_path, capsys):
    err = _run_error(tmp_path, capsys, _base_plan(extra=1))
    assert "unknown field(s): extra" in err
    plan = _base_plan()
    del plan["maxCases"]
    err = _run_error(tmp_path, capsys, plan)
    assert "missing field(s): maxCases" in err


def test_scenario_must_not_contain_message_faults(tmp_path, capsys):
    scenario = _base_scenario(messageFaults=[])
    err = _run_error(tmp_path, capsys, _base_plan(scenario=scenario))
    assert "messageFaults" in err


def test_scenario_validation_errors_propagate(tmp_path, capsys):
    scenario = _base_scenario()
    del scenario["duration"]
    err = _run_error(tmp_path, capsys, _base_plan(scenario=scenario))
    assert "missing field(s): duration" in err


def test_candidates_must_be_a_non_empty_list(tmp_path, capsys):
    err = _run_error(tmp_path, capsys, _base_plan(candidates="x"))
    assert "candidates must be a list" in err
    err = _run_error(tmp_path, capsys, _base_plan(candidates=[]))
    assert "candidates must not be empty" in err


def test_candidate_rules_use_original_validation(tmp_path, capsys):
    err = _run_error(tmp_path, capsys, _base_plan(candidates=[_candidate(1, **{"from": "zzz"})]))
    assert "unknown node" in err
    err = _run_error(tmp_path, capsys, _base_plan(candidates=[{"from": "a"}]))
    assert "missing field(s)" in err


def test_duplicate_selectors_are_rejected(tmp_path, capsys):
    err = _run_error(tmp_path, capsys, _base_plan(candidates=[_candidate(1), _candidate(1)]))
    assert "duplicates the selector" in err


def test_max_faults_bounds(tmp_path, capsys):
    err = _run_error(tmp_path, capsys, _base_plan(maxFaults=-1))
    assert "maxFaults" in err
    err = _run_error(tmp_path, capsys, _base_plan(maxFaults=True))
    assert "maxFaults must be an integer" in err
    err = _run_error(tmp_path, capsys, _base_plan(maxFaults=2))
    assert "maxFaults must not exceed the number of candidates" in err


def test_max_cases_bounds(tmp_path, capsys):
    err = _run_error(tmp_path, capsys, _base_plan(maxCases=0))
    assert "maxCases must be a positive integer" in err
    err = _run_error(tmp_path, capsys, _base_plan(maxCases=False))
    assert "maxCases must be an integer" in err


def test_combination_count_exceeding_max_cases_fails_before_simulation(tmp_path, capsys):
    # Three candidates with maxFaults 2 give 7 combinations.
    plan = _base_plan(
        candidates=[_candidate(91), _candidate(92), _candidate(93)],
        maxFaults=2,
        maxCases=6,
    )
    err = _run_error(tmp_path, capsys, plan)
    assert "exceeds maxCases" in err


def test_unreadable_and_invalid_plan_files(tmp_path, capsys):
    code = main(["explore", str(tmp_path / "missing.json")])
    out, err = capsys.readouterr()
    assert code == 2 and out == ""
    assert err.startswith("error: cannot read plan file:")

    path = tmp_path / "plan.json"
    path.write_text("{not json", encoding="utf-8")
    code = main(["explore", str(path)])
    out, err = capsys.readouterr()
    assert code == 2 and out == ""
    assert err.startswith("error: invalid JSON:")

    path.write_bytes(b"\xff\xfe")
    code = main(["explore", str(path)])
    out, err = capsys.readouterr()
    assert code == 2 and out == ""
    assert err.startswith("error: plan file is not valid UTF-8:")


# -- minimizeFailures -------------------------------------------------------


def _membership_scenario(deadline):
    # Adding learner d commits via joint consensus around t=321 when healthy;
    # a single fault on the learner's catch-up path pushes the stable commit
    # to t=330, so a deadline of 325 turns exactly that singleton case red.
    return _base_scenario(
        nodes=["a", "b", "c", "d"],
        duration=700,
        electionTimeouts={"a": 80, "b": 150, "c": 200, "d": 250},
        heartbeatInterval=50,
        messageDelay=5,
        initialMembers=["a", "b", "c"],
        membershipChanges=[
            {"time": 300, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        clientCommands=[{"time": 291, "node": "a", "id": "w1", "command": 1}],
        livenessChecks=[
            {"id": "M", "type": "membershipCommitted",
             "startTime": 300, "deadline": deadline, "target": "m1"}
        ],
    )


def test_minimize_failures_must_be_a_boolean(tmp_path, capsys):
    for value in (0, 1, "true", None, [], {}):
        plan = _base_plan(minimizeFailures=value)
        err = _run_error(tmp_path, capsys, plan)
        assert "minimizeFailures must be a boolean" in err


def test_minimize_false_and_omitted_outputs_are_identical(tmp_path, capsys):
    plan = _base_plan(candidates=[_candidate(1), _candidate(2)], maxFaults=2)
    _, out_omitted, _ = _run(tmp_path, capsys, plan)
    plan_false = dict(plan)
    plan_false["minimizeFailures"] = False
    _, out_false, _ = _run(tmp_path, capsys, plan_false)
    assert out_false == out_omitted
    summary = json.loads(out_omitted)
    for case in summary["cases"]:
        assert "failureReports" not in case
        assert "minimalSelected" not in case
        assert "minimalCaseId" not in case


def test_minimize_failures_fields_on_failed_and_passed_cases(tmp_path, capsys):
    plan = {
        "scenario": _membership_scenario(325),
        "candidates": [
            {"from": "d", "to": "a", "message": "appendReply",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 1,
        "maxCases": 100,
        "minimizeFailures": True,
    }
    summary = _run_ok(tmp_path, capsys, plan)
    assert summary["totalCases"] == 2
    assert summary["failedCases"] == 1
    plain, faulted = summary["cases"]
    assert plain["status"] == "passed"
    assert "failureReports" not in plain
    assert "minimalSelected" not in plain
    assert "minimalCaseId" not in plain
    assert faulted["status"] == "failed"
    assert faulted["failureReports"] == ["liveness"]
    assert faulted["minimalSelected"] == [0]
    assert faulted["minimalCaseId"] == faulted["caseId"] == 1
    # The reproduction reference points at the existing case; no new cases
    # are enumerated for minimization.
    assert [c["caseId"] for c in summary["cases"]] == [0, 1]


def test_minimize_selects_a_passing_base_only_when_the_signature_matches(tmp_path, capsys):
    # The stale read is intrinsic to the crashed-leader scenario: the no-fault
    # case fails linearizability and is the minimum of every case with that
    # signature. The pair additionally misses the leaderElected deadline, so
    # its signature is liveness-only and must not collapse onto the base.
    scenario = _base_scenario(
        duration=700,
        clientCommands=[{"time": 140, "node": "a", "id": "w1", "command": "x"}],
        nodeEvents=[{"time": 165, "node": "a", "action": "crash"}],
        readQueries=[{"time": 400, "node": "b", "id": "r1"}],
        livenessChecks=[
            {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": 125}
        ],
    )
    plan = {
        "scenario": scenario,
        "candidates": [
            {"from": "b", "to": "a", "message": "voteReply",
             "occurrence": 1, "action": "drop"},
            {"from": "c", "to": "a", "message": "voteReply",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 2,
        "maxCases": 100,
        "minimizeFailures": True,
    }
    summary = _run_ok(tmp_path, capsys, plan)
    by_id = {case["caseId"]: case for case in summary["cases"]}
    assert by_id[0]["failureReports"] == ["linearizability"]
    assert by_id[0]["minimalSelected"] == []
    assert by_id[0]["minimalCaseId"] == 0
    for case_id in (1, 2):
        assert by_id[case_id]["failureReports"] == ["linearizability"]
        assert by_id[case_id]["minimalSelected"] == []
        assert by_id[case_id]["minimalCaseId"] == 0
    pair = by_id[3]
    assert pair["selected"] == [0, 1]
    assert pair["failureReports"] == ["liveness"]
    assert pair["minimalSelected"] == [0, 1]
    assert pair["minimalCaseId"] == 3


def test_minimize_tie_breaks_on_numeric_lexicographic_order(tmp_path, capsys):
    # Two unrelated single faults each push the stable membership commit past
    # the deadline; a subset reachable from the pair but carrying the other
    # report never wins, and the lexicographically smallest minimum is [0].
    plan = {
        "scenario": _membership_scenario(325),
        "candidates": [
            {"from": "d", "to": "a", "message": "appendReply",
             "occurrence": 1, "action": "drop"},
            {"from": "a", "to": "d", "message": "appendEntries",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 2,
        "maxCases": 100,
        "minimizeFailures": True,
    }
    summary = _run_ok(tmp_path, capsys, plan)
    by_selection = {tuple(c["selected"]): c for c in summary["cases"]}
    assert by_selection[()]["status"] == "passed"
    assert by_selection[(0,)]["minimalSelected"] == [0]
    assert by_selection[(1,)]["minimalSelected"] == [1]
    pair = by_selection[(0, 1)]
    assert pair["failureReports"] == ["liveness"]
    assert pair["minimalSelected"] == [0]
    assert pair["minimalCaseId"] == by_selection[(0,)]["caseId"]


def test_minimize_only_deletes_rules_from_the_case(tmp_path, capsys):
    # Two candidates at different selectors each fail on their own; the
    # singleton minimum of [1] must be [1], never the unrelated [0].
    plan = {
        "scenario": _membership_scenario(325),
        "candidates": [
            {"from": "d", "to": "a", "message": "appendReply",
             "occurrence": 1, "action": "drop"},
            {"from": "a", "to": "d", "message": "appendEntries",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 2,
        "maxCases": 100,
        "minimizeFailures": True,
    }
    summary = _run_ok(tmp_path, capsys, plan)
    by_selection = {tuple(c["selected"]): c for c in summary["cases"]}
    assert set(by_selection[(1,)]["minimalSelected"]).issubset({1})
    assert by_selection[(1,)]["minimalSelected"] == [1]


def test_minimize_references_do_not_count_against_max_cases(tmp_path, capsys):
    # Four combinations exactly hit maxCases; the extra minimization
    # bookkeeping must not inflate the combination count.
    plan = {
        "scenario": _membership_scenario(325),
        "candidates": [
            {"from": "d", "to": "a", "message": "appendReply",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 1,
        "maxCases": 2,
        "minimizeFailures": True,
    }
    summary = _run_ok(tmp_path, capsys, plan)
    assert summary["totalCases"] == 2
    assert summary["failedCases"] == 1


def test_minimize_failure_reports_follow_check_priority(tmp_path, capsys):
    # The baseline both serves a stale read and misses a deadline; an enabled
    # report with no violations (electionSafety) stays out of the list, and
    # the present names follow the documented check priority.
    scenario = _base_scenario(
        duration=700,
        clientCommands=[{"time": 140, "node": "a", "id": "w1", "command": "x"}],
        nodeEvents=[{"time": 165, "node": "a", "action": "crash"}],
        readQueries=[{"time": 400, "node": "b", "id": "r1"}],
        livenessChecks=[
            {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": 50}
        ],
    )
    plan = {
        "scenario": scenario,
        "candidates": [_candidate(91)],
        "maxFaults": 1,
        "maxCases": 100,
        "minimizeFailures": True,
    }
    summary = _run_ok(tmp_path, capsys, plan)
    assert all(
        case["failureReports"] == ["linearizability", "liveness"]
        for case in summary["cases"]
    )
    assert all(case["minimalSelected"] == [] for case in summary["cases"])


def test_minimize_does_not_change_enumeration_or_summary(tmp_path, capsys):
    # Same plan with and without minimization: scope, caseId, selected,
    # status, result and the counters are byte-identical apart from the three
    # added fields on failed cases.
    def strip(cases):
        return [
            {k: v for k, v in case.items()
             if k not in ("failureReports", "minimalSelected", "minimalCaseId")}
            for case in cases
        ]

    plan = {
        "scenario": _membership_scenario(325),
        "candidates": [
            {"from": "d", "to": "a", "message": "appendReply",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 1,
        "maxCases": 100,
    }
    _, out_plain, _ = _run(tmp_path, capsys, plan)
    minimized = dict(plan)
    minimized["minimizeFailures"] = True
    summary_min = _run_ok(tmp_path, capsys, minimized)
    summary_plain = json.loads(out_plain)
    for key in ("totalCases", "passedCases", "failedCases"):
        assert summary_min[key] == summary_plain[key]
    assert strip(summary_min["cases"]) == strip(summary_plain["cases"])


def test_simulate_and_version_are_unchanged(tmp_path, capsys):
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(_base_scenario()), encoding="utf-8")
    code = main(["simulate", str(path)])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    assert "timeline" in json.loads(out)
    code = main(["version"])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    assert out.strip() != ""


# -- eventCandidates / maxEventFaults -----------------------------------------


def _event_plan(**overrides):
    plan = _base_plan(
        candidates=[_candidate(91), _candidate(92)],
        maxFaults=1,
        eventCandidates=[
            {"network": {"time": 200, "action": "partition",
                         "groups": [["a"], ["b", "c"]]}},
            {"node": {"time": 300, "node": "c", "action": "crash"}},
        ],
        maxEventFaults=2,
    )
    plan.update(overrides)
    return plan


def test_event_enumeration_is_cartesian_with_message_combos_outer(tmp_path, capsys):
    # 3 message combinations x 4 event combinations; within each message
    # combination the empty event combination comes first, then ascending
    # event counts in numeric lexicographic order.
    summary = _run_ok(tmp_path, capsys, _event_plan())
    assert summary["totalCases"] == 12
    assert [case["caseId"] for case in summary["cases"]] == list(range(12))
    assert [(case["selected"], case["selectedEvents"]) for case in summary["cases"]] == [
        ([], []), ([], [0]), ([], [1]), ([], [0, 1]),
        ([0], []), ([0], [0]), ([0], [1]), ([0], [0, 1]),
        ([1], []), ([1], [0]), ([1], [1]), ([1], [0, 1]),
    ]
    assert summary["passedCases"] + summary["failedCases"] == 12


def test_selected_events_are_appended_to_base_collections(tmp_path, capsys):
    # The base scenario carries a fixed heal and a fixed crash; the selected
    # candidate partition and crash must show up alongside them.
    scenario = _base_scenario(
        faults=[{"time": 400, "action": "heal"}],
        nodeEvents=[{"time": 250, "node": "b", "action": "crash"}],
    )
    plan = _event_plan(scenario=scenario, maxFaults=0)
    summary = _run_ok(tmp_path, capsys, plan)
    both = next(c for c in summary["cases"] if c["selectedEvents"] == [0, 1])
    timeline = both["result"]["timeline"]
    assert [(e["time"], e["action"]) for e in timeline if e["type"] == "fault"] == [
        (200, "partition"),
        (400, "heal"),
    ]
    lifecycle = [(e["time"], e["node"], e["action"])
                 for e in timeline if e["type"] == "nodeLifecycle"]
    assert (250, "b", "crash") in lifecycle
    assert (300, "c", "crash") in lifecycle


def test_same_timestamp_base_events_run_before_candidates(tmp_path, capsys):
    # At t=100 the base partition and the base crash precede the candidate
    # heal and the candidate restart, and the network events precede the
    # node events.
    scenario = _base_scenario(
        duration=300,
        faults=[{"time": 100, "action": "partition", "groups": [["a"], ["b", "c"]]}],
        nodeEvents=[{"time": 100, "node": "c", "action": "crash"}],
    )
    plan = _event_plan(
        scenario=scenario,
        maxFaults=0,
        eventCandidates=[
            {"network": {"time": 100, "action": "heal"}},
            {"node": {"time": 100, "node": "c", "action": "restart"}},
        ],
    )
    summary = _run_ok(tmp_path, capsys, plan)
    both = next(c for c in summary["cases"] if c["selectedEvents"] == [0, 1])
    kinds = [
        (e["type"], e["action"])
        for e in both["result"]["timeline"]
        if e["time"] == 100 and e["type"] in ("fault", "nodeLifecycle")
    ]
    assert kinds == [
        ("fault", "partition"),
        ("fault", "heal"),
        ("nodeLifecycle", "crash"),
        ("nodeLifecycle", "restart"),
    ]


def test_max_event_faults_zero_still_marks_cases(tmp_path, capsys):
    plan = _event_plan(maxEventFaults=0)
    summary = _run_ok(tmp_path, capsys, plan)
    assert summary["totalCases"] == 3
    assert all(case["selectedEvents"] == [] for case in summary["cases"])


def test_event_fields_must_be_provided_together(tmp_path, capsys):
    plan = _base_plan(eventCandidates=[{"network": {"time": 1, "action": "heal"}}])
    err = _run_error(tmp_path, capsys, plan)
    assert "provided together" in err
    err = _run_error(tmp_path, capsys, _base_plan(maxEventFaults=1))
    assert "provided together" in err


def test_event_candidates_structural_validation(tmp_path, capsys):
    err = _run_error(tmp_path, capsys, _event_plan(eventCandidates="x"))
    assert "eventCandidates must be a list" in err
    err = _run_error(tmp_path, capsys, _event_plan(eventCandidates=[]))
    assert "eventCandidates must not be empty" in err
    err = _run_error(tmp_path, capsys, _event_plan(eventCandidates=[5]))
    assert "eventCandidates[0] must be an object" in err
    err = _run_error(tmp_path, capsys, _event_plan(
        eventCandidates=[{"network": {"time": 1, "action": "heal"}, "extra": 1}]))
    assert "unknown field(s): extra" in err
    err = _run_error(tmp_path, capsys, _event_plan(eventCandidates=[{}]))
    assert "exactly one of network or node" in err
    err = _run_error(tmp_path, capsys, _event_plan(eventCandidates=[
        {"network": {"time": 1, "action": "heal"},
         "node": {"time": 1, "node": "a", "action": "crash"}},
    ]))
    assert "exactly one of network or node" in err


def test_max_event_faults_bounds(tmp_path, capsys):
    err = _run_error(tmp_path, capsys, _event_plan(maxEventFaults=-1))
    assert "maxEventFaults" in err
    err = _run_error(tmp_path, capsys, _event_plan(maxEventFaults=True))
    assert "maxEventFaults must be an integer" in err
    err = _run_error(tmp_path, capsys, _event_plan(maxEventFaults=3))
    assert "maxEventFaults must not exceed the number of event candidates" in err


def test_event_content_is_validated_before_any_simulation(tmp_path, capsys):
    # Illegal partition shape, unknown node, out-of-range time and broken
    # crash/restart alternation in a merged combination are all plan errors.
    err = _run_error(tmp_path, capsys, _event_plan(eventCandidates=[
        {"network": {"time": 1, "action": "partition", "groups": [["a"], ["b"]]}},
    ], maxEventFaults=1))
    assert "cover every node" in err
    err = _run_error(tmp_path, capsys, _event_plan(eventCandidates=[
        {"node": {"time": 1, "node": "zz", "action": "crash"}},
    ], maxEventFaults=1))
    assert "unknown node" in err
    err = _run_error(tmp_path, capsys, _event_plan(eventCandidates=[
        {"network": {"time": 9999, "action": "heal"}},
    ], maxEventFaults=0))
    assert "beyond the simulation duration" in err
    # Base crash plus a second candidate crash for the same node only breaks
    # alternation in the merged combination that selects the candidate.
    scenario = _base_scenario(nodeEvents=[{"time": 1, "node": "a", "action": "crash"}])
    err = _run_error(tmp_path, capsys, _event_plan(
        scenario=scenario,
        eventCandidates=[{"node": {"time": 2, "node": "a", "action": "crash"}}],
        maxEventFaults=1,
    ))
    assert "alternate" in err


def test_max_cases_counts_the_full_cartesian_product(tmp_path, capsys):
    # 3 message combinations x 4 event combinations = 12 cases.
    err = _run_error(tmp_path, capsys, _event_plan(maxCases=11))
    assert "combination count 12 exceeds maxCases 11" in err
    summary = _run_ok(tmp_path, capsys, _event_plan(maxCases=12))
    assert summary["totalCases"] == 12


def _stale_read_event_plan(deadline=125):
    # 4-node scenario: a leads and commits w1 at t=160; crashing a at 165 or
    # partitioning it away at 170 both strand the commit, so b's later read
    # is stale and fails linearizability.
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 700,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 10,
        "clientCommands": [{"time": 140, "node": "a", "id": "w1", "command": "x"}],
        "readQueries": [{"time": 400, "node": "b", "id": "r1"}],
        "livenessChecks": [
            {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": deadline}
        ],
    }
    return {
        "scenario": scenario,
        "candidates": [
            {"from": "a", "to": "b", "message": "heartbeat",
             "occurrence": 91, "action": "drop"},
        ],
        "maxFaults": 0,
        "maxCases": 100,
        "minimizeFailures": True,
        "eventCandidates": [
            {"node": {"time": 165, "node": "a", "action": "crash"}},
            {"network": {"time": 170, "action": "partition",
                         "groups": [["a"], ["b", "c", "d"]]}},
        ],
        "maxEventFaults": 2,
    }


def test_minimize_deletes_both_candidate_kinds(tmp_path, capsys):
    summary = _run_ok(tmp_path, capsys, _stale_read_event_plan())
    by_events = {tuple(c["selectedEvents"]): c for c in summary["cases"]}
    assert by_events[()]["status"] == "passed"
    assert "minimalSelectedEvents" not in by_events[()]
    for key in ((0,), (1,)):
        case = by_events[key]
        assert case["failureReports"] == ["linearizability"]
        assert case["minimalSelected"] == []
        assert case["minimalSelectedEvents"] == list(key)
        assert case["minimalCaseId"] == case["caseId"]
    pair = by_events[(0, 1)]
    assert pair["failureReports"] == ["linearizability"]
    # Both singletons reproduce the pair's exact failureReports; the fewest
    # total candidates win, ties broken by selected then selectedEvents.
    assert pair["minimalSelected"] == []
    assert pair["minimalSelectedEvents"] == [0]
    assert pair["minimalCaseId"] == by_events[(0,)]["caseId"]
    # minimalCaseId always references an enumerated case.
    assert {c["caseId"] for c in summary["cases"]} == {0, 1, 2, 3}


def test_minimize_prefers_fewest_total_candidates_across_kinds(tmp_path, capsys):
    # A message fault and an event each reproduce the same liveness failure;
    # a case combining both collapses onto the single message fault because
    # one candidate beats two, regardless of kind.
    scenario = _base_scenario(
        livenessChecks=[
            {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": 50}
        ]
    )
    plan = _base_plan(
        scenario=scenario,
        candidates=[_candidate(1)],
        maxFaults=1,
        minimizeFailures=True,
        eventCandidates=[{"node": {"time": 10, "node": "a", "action": "crash"}}],
        maxEventFaults=1,
    )
    summary = _run_ok(tmp_path, capsys, plan)
    assert summary["failedCases"] == 4
    for case in summary["cases"]:
        assert case["failureReports"] == ["liveness"]
        assert case["minimalSelected"] == []
        assert case["minimalSelectedEvents"] == []
        assert case["minimalCaseId"] == 0


def test_legacy_plan_output_is_byte_identical_without_event_fields(tmp_path, capsys):
    # Omitting eventCandidates and maxEventFaults keeps the legacy output;
    # in particular cases carry no selectedEvents key.
    plan = _base_plan(candidates=[_candidate(1), _candidate(2)], maxFaults=2,
                      minimizeFailures=True)
    summary = _run_ok(tmp_path, capsys, plan)
    for case in summary["cases"]:
        assert "selectedEvents" not in case
        assert "minimalSelectedEvents" not in case


# -- strict JSON boundary ---------------------------------------------------


def _run_raw(tmp_path, capsys, raw):
    path = tmp_path / "plan.json"
    path.write_bytes(raw)
    code = main(["explore", str(path)])
    out, err = capsys.readouterr()
    return code, out, err


def test_explore_rejects_non_finite_numbers(tmp_path, capsys):
    template = (
        b'{"scenario":{"nodes":["a","b","c"],"duration":50,'
        b'"electionTimeouts":{"a":100,"b":150,"c":200},'
        b'"heartbeatInterval":50,"messageDelay":10,'
        b'"clientCommands":[{"time":1,"node":"a","id":"x","command":{"v":[%s]}}]},'
        b'"candidates":[{"from":"a","to":"b","message":"heartbeat",'
        b'"occurrence":1,"action":"drop"}],'
        b'"maxFaults":0,"maxCases":10}'
    )
    for token in (b"NaN", b"Infinity", b"-Infinity", b"1e999", b"-1e999"):
        code, out, err = _run_raw(tmp_path, capsys, template % token)
        assert code == 2, token
        assert out == "", token
        assert err.startswith("error: invalid JSON:"), token
        assert "non-finite" in err, token
        assert err.count("\n") == 1, token


def test_explore_accepts_nan_like_strings_and_keys(tmp_path, capsys):
    plan = _base_plan()
    plan["scenario"]["clientCommands"] = [
        {"time": 1, "node": "a", "id": "x",
         "command": {"NaN": "Infinity", "-Infinity": ["NaN"]}}
    ]
    _run_ok(tmp_path, capsys, plan)
