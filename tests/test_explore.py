"""Tests for the explore subcommand: bounded message-fault enumeration."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main
from consensus_lab.explore import PlanError, run_exploration
from consensus_lab.simulate import run_simulation


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


def _drop_vote_reply(src):
    return {
        "from": src, "to": "a", "message": "voteReply",
        "occurrence": 1, "action": "drop",
    }


def _plan(**overrides):
    plan = {
        "scenario": _base_scenario(),
        "candidates": [_drop_vote_reply("b"), _drop_vote_reply("c")],
        "maxFaults": 2,
        "maxCases": 10,
    }
    plan.update(overrides)
    return plan


def _run(tmp_path, capsys, plan):
    path = _write_plan(tmp_path, plan)
    code = main(["explore", path])
    out, err = capsys.readouterr()
    return code, out, err


# -- enumeration -------------------------------------------------------------

def test_empty_combination_first_then_by_size_and_lexicographic_order():
    report = run_exploration(_plan(maxFaults=2))
    selected = [case["selected"] for case in report["cases"]]
    assert selected == [[], [0], [1], [0, 1]]
    assert [case["caseId"] for case in report["cases"]] == [0, 1, 2, 3]


def test_enumeration_three_candidates_full_order():
    plan = _plan(
        candidates=[_drop_vote_reply("b"), _drop_vote_reply("c"),
                    {"from": "a", "to": "c", "message": "heartbeat",
                     "occurrence": 1, "action": "drop"}],
        maxFaults=3,
        maxCases=100,
    )
    report = run_exploration(plan)
    assert [case["selected"] for case in report["cases"]] == [
        [],
        [0], [1], [2],
        [0, 1], [0, 2], [1, 2],
        [0, 1, 2],
    ]


def test_max_faults_zero_runs_only_empty_combination():
    report = run_exploration(_plan(maxFaults=0))
    assert report["totalCases"] == 1
    assert report["cases"][0]["selected"] == []


def test_combination_count_respects_max_faults():
    report = run_exploration(_plan(maxFaults=1))
    assert report["totalCases"] == 3
    assert [case["selected"] for case in report["cases"]] == [[], [0], [1]]


# -- per-case semantics ------------------------------------------------------

def test_empty_case_result_equals_plain_simulation():
    plan = _plan()
    report = run_exploration(plan)
    assert report["cases"][0]["result"] == run_simulation(_base_scenario())


def test_case_result_equals_simulation_with_rules_in_original_order():
    plan = _plan()
    report = run_exploration(plan)
    # The [0, 1] case matches a direct simulation with both rules, including
    # the rule indices recorded in the messageFault timeline entries.
    direct = run_simulation(
        _base_scenario(messageFaults=plan["candidates"])
    )
    assert report["cases"][3]["selected"] == [0, 1]
    assert report["cases"][3]["result"] == direct


def test_selected_rules_keep_their_candidate_index_in_timeline():
    # With only candidate 1 selected, the timeline's rule index is still 1.
    report = run_exploration(_plan(maxFaults=1))
    result = report["cases"][2]["result"]
    assert report["cases"][2]["selected"] == [1]
    faults = [e for e in result["timeline"] if e["type"] == "messageFault"]
    assert faults and all(e["rule"] == 1 for e in faults)


def test_case_result_retains_full_timeline_and_reports():
    report = run_exploration(_plan())
    for case in report["cases"]:
        result = case["result"]
        assert "timeline" in result and result["timeline"]
        for report_name in (
            "electionSafety", "logMatching", "stateMachineSafety",
        ):
            assert report_name in result


# -- pass/fail judgement -----------------------------------------------------

def test_passed_when_all_violations_empty():
    report = run_exploration(_plan())
    # The single drops and the empty run keep election safety intact.
    assert report["cases"][0]["status"] == "passed"
    assert report["cases"][1]["status"] == "passed"
    assert report["cases"][2]["status"] == "passed"


def test_liveness_violation_marks_case_failed_and_is_reproducible():
    plan = _plan(
        scenario=_base_scenario(
            livenessChecks=[
                {"id": "L1", "type": "leaderElected",
                 "startTime": 0, "deadline": 140}
            ]
        )
    )
    report = run_exploration(plan)
    assert report["failedCases"] == 1
    assert report["passedCases"] == 3
    counterexample = report["cases"][3]
    assert counterexample["status"] == "failed"
    assert counterexample["selected"] == [0, 1]
    assert counterexample["result"]["liveness"]["violations"] == [
        {"id": "L1", "checkType": "leaderElected", "status": "failed",
         "time": 140, "reason": "deadlineExceeded"}
    ]
    # The counterexample is reproducible through plain simulate.
    direct = run_simulation(
        _base_scenario(
            livenessChecks=[
                {"id": "L1", "type": "leaderElected",
                 "startTime": 0, "deadline": 140}
            ],
            messageFaults=[plan["candidates"][0], plan["candidates"][1]],
        )
    )
    assert direct["liveness"]["violations"] != []
    assert counterexample["result"] == direct


def test_failure_does_not_stop_later_cases():
    plan = _plan(
        scenario=_base_scenario(
            livenessChecks=[
                {"id": "L1", "type": "leaderElected",
                 "startTime": 0, "deadline": 140}
            ]
        )
    )
    report = run_exploration(plan)
    # Case 3 fails, but it is still the last one reported and totals cover
    # every combination.
    assert report["totalCases"] == 4
    assert len(report["cases"]) == 4


def test_disabled_reports_do_not_participate_in_judgement():
    # No readQueries and no livenessChecks: linearizability/liveness keys are
    # simply absent and must not count as failures.
    report = run_exploration(_plan())
    assert all(
        "linearizability" not in case["result"]
        and "liveness" not in case["result"]
        for case in report["cases"]
    )
    assert report["failedCases"] == 0


def test_totals_consistent_with_case_statuses():
    report = run_exploration(
        _plan(
            scenario=_base_scenario(
                livenessChecks=[
                    {"id": "L1", "type": "leaderElected",
                     "startTime": 0, "deadline": 140}
                ]
            )
        )
    )
    assert report["totalCases"] == len(report["cases"])
    assert (report["passedCases"] + report["failedCases"]) == report["totalCases"]
    assert report["passedCases"] == sum(
        1 for c in report["cases"] if c["status"] == "passed"
    )
    assert report["failedCases"] == sum(
        1 for c in report["cases"] if c["status"] == "failed"
    )


# -- limit -------------------------------------------------------------------

def test_too_many_combinations_fails_before_any_simulation():
    # maxFaults=1 over two candidates yields three combinations.
    plan = _plan(maxFaults=1, maxCases=2)
    with pytest.raises(PlanError, match="exceeds maxCases"):
        run_exploration(plan)


def test_exact_combination_count_is_allowed():
    plan = _plan(maxFaults=1, maxCases=3)
    report = run_exploration(plan)
    assert report["totalCases"] == 3


# -- determinism -------------------------------------------------------------

def test_output_is_byte_identical_across_runs(tmp_path, capsys):
    plan = _plan(
        scenario=_base_scenario(
            livenessChecks=[
                {"id": "L1", "type": "leaderElected",
                 "startTime": 0, "deadline": 140}
            ]
        )
    )
    _, out1, err1 = _run(tmp_path, capsys, plan)
    _, out2, err2 = _run(tmp_path, capsys, plan)
    assert out1 == out2
    assert err1 == err2 == ""


# -- CLI ---------------------------------------------------------------------

def test_cli_success_shape_and_exit_code(tmp_path, capsys):
    code, out, err = _run(tmp_path, capsys, _plan())
    assert code == 0
    assert err == ""
    report = json.loads(out)
    assert set(report) == {"totalCases", "passedCases", "failedCases", "cases"}
    for case in report["cases"]:
        assert set(case) == {"caseId", "selected", "status", "result"}


def test_cli_missing_file(capsys):
    code = main(["explore", "/nonexistent/plan.json"])
    out, err = capsys.readouterr()
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_cli_invalid_json(tmp_path, capsys):
    path = tmp_path / "bad.json"
    path.write_text("{not json", encoding="utf-8")
    code = main(["explore", str(path)])
    out, err = capsys.readouterr()
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_cli_non_utf8_file(tmp_path, capsys):
    path = tmp_path / "bad.json"
    path.write_bytes(b"\xff\xfe{}")
    code = main(["explore", str(path)])
    out, err = capsys.readouterr()
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_cli_limit_exceeded_writes_no_partial_output(tmp_path, capsys):
    code, out, err = _run(tmp_path, capsys, _plan(maxFaults=1, maxCases=2))
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


# -- invalid PLANs -----------------------------------------------------------

@pytest.mark.parametrize("mutate", [
    lambda p: p.clear(),
    lambda p: p.pop("scenario"),
    lambda p: p.pop("candidates"),
    lambda p: p.pop("maxFaults"),
    lambda p: p.pop("maxCases"),
    lambda p: p.update(extra=1),
    lambda p: p.update(scenario=[]),
    lambda p: p["scenario"].update(messageFaults=[]),
    lambda p: p.update(candidates=[]),
    lambda p: p.update(candidates={}),
    lambda p: p.update(maxFaults=-1),
    lambda p: p.update(maxFaults=3),
    lambda p: p.update(maxFaults=True),
    lambda p: p.update(maxFaults=1.5),
    lambda p: p.update(maxCases=0),
    lambda p: p.update(maxCases=False),
    lambda p: p.update(maxCases=1.5),
    lambda p: p.update(maxCases="5"),
    lambda p: p.update(candidates=[1]),
    lambda p: p.update(candidates=[{"from": "z", "to": "a", "message": "voteReply",
                                   "occurrence": 1, "action": "drop"}]),
    lambda p: p.update(candidates=[{"from": "a", "to": "a", "message": "voteReply",
                                   "occurrence": 1, "action": "drop"}]),
    lambda p: p.update(candidates=[{"from": "b", "to": "a", "message": "bogus",
                                   "occurrence": 1, "action": "drop"}]),
    lambda p: p.update(candidates=[{"from": "b", "to": "a", "message": "voteReply",
                                   "occurrence": 0, "action": "drop"}]),
    lambda p: p.update(candidates=[{"from": "b", "to": "a", "message": "voteReply",
                                   "occurrence": 1, "action": "lose"}]),
    lambda p: p.update(candidates=[{"from": "b", "to": "a", "message": "voteReply",
                                   "occurrence": 1, "action": "drop", "delay": 1}]),
    lambda p: p.update(candidates=[{"from": "b", "to": "a", "message": "voteReply",
                                   "occurrence": 1, "action": "delay"}]),
])
def test_invalid_plans(tmp_path, capsys, mutate):
    plan = _plan()
    mutate(plan)
    code, out, err = _run(tmp_path, capsys, plan)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_duplicate_candidate_selectors_rejected():
    rule = _drop_vote_reply("b")
    with pytest.raises(PlanError, match="duplicates the selector"):
        run_exploration(_plan(candidates=[rule, dict(rule)], maxFaults=2, maxCases=10))


def test_invalid_base_scenario_rejected():
    with pytest.raises(PlanError, match="nodes"):
        run_exploration(
            _plan(scenario=_base_scenario(nodes=["a", "b"]))
        )
