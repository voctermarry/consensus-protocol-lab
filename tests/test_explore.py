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
