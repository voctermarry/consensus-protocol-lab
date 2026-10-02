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


def _liveness_plan(deadline, candidates, **overrides):
    scenario = _base_scenario(
        livenessChecks=[
            {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": deadline}
        ]
    )
    plan = _base_plan(scenario=scenario, candidates=candidates, **overrides)
    return plan


def test_minimize_failures_omitted_or_false_keeps_output_unchanged(tmp_path, capsys):
    plan = _base_plan(candidates=[_candidate(1), _candidate(2)], maxFaults=2)
    code1, out1, _ = _run(tmp_path, capsys, plan)
    plan["minimizeFailures"] = False
    code2, out2, _ = _run(tmp_path, capsys, plan)
    assert code1 == code2 == 0
    assert out1 == out2
    summary = json.loads(out1)
    for case in summary["cases"]:
        assert "failureReports" not in case
        assert "minimalSelected" not in case
        assert "minimalCaseId" not in case


def test_minimize_failures_must_be_a_boolean(tmp_path, capsys):
    for bad in (1, 0, "true", None, [True]):
        err = _run_error(tmp_path, capsys, _base_plan(minimizeFailures=bad))
        assert "minimizeFailures must be a boolean" in err


def test_minimize_shared_signature_points_at_no_fault_case(tmp_path, capsys):
    # The liveness deadline expires before any election timeout, so every
    # case fails with the same signature; the no-fault case is the minimal
    # reproduction for all of them.
    plan = _liveness_plan(
        50,
        [_candidate(91), _candidate(92)],
        maxFaults=1,
        minimizeFailures=True,
    )
    summary = _run_ok(tmp_path, capsys, plan)
    assert summary["failedCases"] == 3
    for case in summary["cases"]:
        assert case["status"] == "failed"
        assert case["failureReports"] == ["liveness"]
        assert case["minimalSelected"] == []
        assert case["minimalCaseId"] == 0


def test_minimize_keeps_the_faults_needed_for_the_violation(tmp_path, capsys):
    # Dropping a's first RequestVote to both peers delays the election past
    # the deadline; either drop alone still lets a win, so only the pair
    # fails and it is already minimal. maxCases equals the combination
    # count: minimization references do not count against it.
    plan = _liveness_plan(
        120,
        [
            _candidate(1, message="requestVote", **{"from": "a", "to": "b"}),
            _candidate(1, message="requestVote", **{"from": "a", "to": "c"}),
        ],
        maxFaults=2,
        maxCases=4,
        minimizeFailures=True,
    )
    summary = _run_ok(tmp_path, capsys, plan)
    assert (summary["totalCases"], summary["passedCases"], summary["failedCases"]) == (4, 3, 1)
    assert [case["caseId"] for case in summary["cases"]] == [0, 1, 2, 3]
    assert [case["selected"] for case in summary["cases"]] == [[], [0], [1], [0, 1]]
    for case in summary["cases"][:3]:
        assert case["status"] == "passed"
        assert "failureReports" not in case
        assert "minimalSelected" not in case
        assert "minimalCaseId" not in case
    failed = summary["cases"][3]
    assert failed["status"] == "failed"
    assert failed["failureReports"] == ["liveness"]
    assert failed["minimalSelected"] == [0, 1]
    assert failed["minimalCaseId"] == 3


def test_minimize_does_not_change_existing_fields(tmp_path, capsys):
    plan = _liveness_plan(120, [_candidate(1), _candidate(2)], maxFaults=2)
    plain = _run_ok(tmp_path, capsys, plan)
    plan["minimizeFailures"] = True
    minimized = _run_ok(tmp_path, capsys, plan)
    for key in ("totalCases", "passedCases", "failedCases"):
        assert plain[key] == minimized[key]
    for before, after in zip(plain["cases"], minimized["cases"]):
        for key in ("caseId", "selected", "status", "result"):
            assert before[key] == after[key]


def _synthetic_case(case_id, selected, reports):
    # Reports are inserted in reverse priority order to prove that
    # failureReports follows the fixed check priority, not dict order.
    result = {
        name: {"violations": ["v"]}
        for name in reversed(reports)
    }
    return {
        "caseId": case_id,
        "selected": selected,
        "status": "failed" if reports else "passed",
        "result": result,
    }


def test_attach_minimization_signature_and_tie_breaking():
    from consensus_lab.explore import _attach_minimization

    cases = [
        _synthetic_case(0, [], ["liveness"]),
        _synthetic_case(1, [0], ["electionSafety", "liveness"]),
        _synthetic_case(2, [1], []),
        _synthetic_case(3, [0, 1], ["liveness", "electionSafety"]),
    ]
    _attach_minimization(cases)
    # Check-priority order, not insertion order.
    assert cases[1]["failureReports"] == ["electionSafety", "liveness"]
    assert cases[3]["failureReports"] == ["electionSafety", "liveness"]
    # Deleting rule 1 leaves case 1 with the same signature; deleting rule 0
    # leaves a passed case and deleting both leaves only the other violation,
    # so neither may be chosen.
    assert cases[3]["minimalSelected"] == [0]
    assert cases[3]["minimalCaseId"] == 1
    assert "failureReports" not in cases[2]

    tied = [
        _synthetic_case(0, [], []),
        _synthetic_case(1, [0], ["liveness"]),
        _synthetic_case(2, [1], ["liveness"]),
        _synthetic_case(3, [0, 1], ["liveness"]),
    ]
    _attach_minimization(tied)
    # Fewest rules wins; equal sizes take the lexicographically smallest
    # index list, and the passed no-fault case is never a match.
    assert tied[3]["minimalSelected"] == [0]
    assert tied[3]["minimalCaseId"] == 1
