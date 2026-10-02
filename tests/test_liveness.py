"""Tests for the optional livenessChecks scenario field."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main


def _write_scenario(tmp_path, scenario):
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(scenario), encoding="utf-8")
    return str(path)


def _base_scenario(**overrides):
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 500,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 0,
    }
    scenario.update(overrides)
    return scenario


def _run(tmp_path, capsys, scenario):
    path = _write_scenario(tmp_path, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _checks(result):
    return {check["id"]: check for check in result["liveness"]["checks"]}


def _liveness_events(result):
    return [e for e in result["timeline"] if e["type"] == "livenessResult"]


# -- happy paths -------------------------------------------------------------


def test_leader_elected_satisfied_at_first_moment(tmp_path, capsys):
    scenario = _base_scenario(livenessChecks=[
        {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": 150},
    ])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    check = _checks(result)["L1"]
    assert check == {
        "id": "L1",
        "checkType": "leaderElected",
        "status": "satisfied",
        "time": 100,
    }
    assert result["liveness"]["violations"] == []
    events = _liveness_events(result)
    assert [(e["id"], e["status"]) for e in events] == [("L1", "satisfied")]


def test_leader_elected_deadline_exceeded(tmp_path, capsys):
    scenario = _base_scenario(livenessChecks=[
        {"id": "late", "type": "leaderElected", "startTime": 0, "deadline": 50},
    ])
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0  # failed checks never change the exit code
    result = json.loads(out)
    assert _checks(result)["late"] == {
        "id": "late",
        "checkType": "leaderElected",
        "status": "failed",
        "time": 50,
        "reason": "deadlineExceeded",
    }
    assert result["liveness"]["violations"] == [{
        "id": "late",
        "checkType": "leaderElected",
        "status": "failed",
        "time": 50,
        "reason": "deadlineExceeded",
    }]


def test_condition_holding_before_start_time_resolves_at_start_time(tmp_path, capsys):
    # a wins at t=100; the window opens later while a still leads.
    scenario = _base_scenario(livenessChecks=[
        {"id": "already", "type": "leaderElected", "startTime": 120, "deadline": 200},
    ])
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert _checks(json.loads(out))["already"]["time"] == 120


def test_client_committed_resolves_at_commit_time(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        livenessChecks=[
            {"id": "c", "type": "clientCommitted", "startTime": 150, "deadline": 300, "target": "x1"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert _checks(json.loads(out))["c"] == {
        "id": "c",
        "checkType": "clientCommitted",
        "status": "satisfied",
        "time": 200,
        "target": "x1",
    }


def test_target_rejected_command(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "b", "id": "bad", "command": 1}],
        livenessChecks=[
            {"id": "r", "type": "clientCommitted", "startTime": 100, "deadline": 300, "target": "bad"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    check = _checks(json.loads(out))["r"]
    assert check["status"] == "failed"
    assert check["reason"] == "targetRejected"
    assert check["time"] == 300


def test_target_superseded_command(tmp_path, capsys):
    scenario = _base_scenario(
        duration=900,
        heartbeatInterval=40,
        faults=[
            {"time": 200, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 600, "action": "heal"},
        ],
        clientCommands=[
            {"time": 150, "node": "a", "id": "x1", "command": "one"},
            {"time": 350, "node": "a", "id": "stale", "command": "two"},
            {"time": 450, "node": "b", "id": "fresh", "command": "TWO"},
        ],
        livenessChecks=[
            {"id": "s", "type": "clientCommitted", "startTime": 350, "deadline": 900, "target": "stale"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    check = _checks(json.loads(out))["s"]
    assert check["status"] == "failed"
    assert check["reason"] == "targetSuperseded"


def test_read_completed_satisfied_and_rejected(tmp_path, capsys):
    scenario = _base_scenario(
        duration=400,
        readQueries=[
            {"time": 220, "node": "a", "id": "q1"},
            {"time": 230, "node": "b", "id": "q2"},
        ],
        livenessChecks=[
            {"id": "ok", "type": "readCompleted", "startTime": 220, "deadline": 300, "target": "q1"},
            {"id": "no", "type": "readCompleted", "startTime": 230, "deadline": 300, "target": "q2"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    checks = _checks(result)
    assert checks["ok"]["status"] == "satisfied"
    assert checks["ok"]["time"] == 220
    assert checks["no"]["status"] == "failed"
    assert checks["no"]["reason"] == "targetRejected"
    assert [v["id"] for v in result["liveness"]["violations"]] == ["no"]


def test_membership_committed_satisfied_and_rejected(tmp_path, capsys):
    scenario = _base_scenario(
        duration=600,
        electionTimeouts={"a": 80, "b": 150, "c": 200, "d": 250},
        nodes=["a", "b", "c", "d"],
        initialMembers=["a", "b", "c"],
        membershipChanges=[
            {"time": 120, "node": "a", "id": "m1", "action": "add", "member": "d"},
            {"time": 130, "node": "b", "id": "m2", "action": "add", "member": "d"},
        ],
        livenessChecks=[
            {"id": "done", "type": "membershipCommitted", "startTime": 120, "deadline": 400, "target": "m1"},
            {"id": "refused", "type": "membershipCommitted", "startTime": 130, "deadline": 200, "target": "m2"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    checks = _checks(json.loads(out))
    assert checks["done"]["status"] == "satisfied"
    # With zero message delay the learner catches up and both config entries
    # commit in the same timestamp as the request.
    assert checks["done"]["time"] == 120
    assert checks["refused"]["status"] == "failed"
    assert checks["refused"]["reason"] == "targetRejected"


# -- ordering, seq and shape -------------------------------------------------


def test_checks_evaluate_after_events_and_zero_delay_reactions(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        livenessChecks=[
            {"id": "c", "type": "clientCommitted", "startTime": 200, "deadline": 300, "target": "x1"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    at_200 = [e for e in result["timeline"] if e["time"] == 200]
    liveness = [e for e in at_200 if e["type"] == "livenessResult"]
    assert len(liveness) == 1
    # The result follows every pre-existing event at the timestamp, including
    # the command's zero-delay replication cascade.
    assert at_200.index(liveness[0]) == len(at_200) - 1
    assert liveness[0]["status"] == "satisfied"


def test_checks_at_same_timestamp_run_in_input_order(tmp_path, capsys):
    scenario = _base_scenario(
        duration=400,
        readQueries=[{"time": 220, "node": "a", "id": "q1"}],
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        livenessChecks=[
            {"id": "first", "type": "clientCommitted", "startTime": 220, "deadline": 300, "target": "x1"},
            {"id": "second", "type": "readCompleted", "startTime": 220, "deadline": 300, "target": "q1"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    at_220 = [e for e in _liveness_events(result) if e["time"] == 220]
    assert [e["id"] for e in at_220] == ["first", "second"]
    seqs = [e["seq"] for e in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def test_liveness_event_shapes(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "b", "id": "bad", "command": 1}],
        livenessChecks=[
            {"id": "lead", "type": "leaderElected", "startTime": 0, "deadline": 150},
            {"id": "rej", "type": "clientCommitted", "startTime": 100, "deadline": 300, "target": "bad"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    events = {e["id"]: e for e in _liveness_events(json.loads(out))}
    assert events["lead"] == {
        "seq": events["lead"]["seq"],
        "time": 100,
        "type": "livenessResult",
        "id": "lead",
        "checkType": "leaderElected",
        "status": "satisfied",
    }
    assert events["rej"] == {
        "seq": events["rej"]["seq"],
        "time": 300,
        "type": "livenessResult",
        "id": "rej",
        "checkType": "clientCommitted",
        "status": "failed",
        "target": "bad",
        "reason": "targetRejected",
    }


def test_empty_list_adds_section_without_events(tmp_path, capsys):
    code, out, _ = _run(tmp_path, capsys, _base_scenario(livenessChecks=[]))
    assert code == 0
    result = json.loads(out)
    assert result["liveness"] == {"checks": [], "violations": []}
    assert _liveness_events(result) == []


def test_omitted_field_keeps_legacy_shape(tmp_path, capsys):
    code, out, _ = _run(tmp_path, capsys, _base_scenario())
    assert code == 0
    result = json.loads(out)
    assert "liveness" not in result
    assert _liveness_events(result) == []


def test_omitted_output_is_deterministic(tmp_path, capsys):
    _, out1, _ = _run(tmp_path, capsys, _base_scenario())
    _, out2, _ = _run(tmp_path, capsys, _base_scenario())
    assert out1 == out2


def test_provided_output_is_deterministic(tmp_path, capsys):
    scenario = _base_scenario(livenessChecks=[
        {"id": "L1", "type": "leaderElected", "startTime": 0, "deadline": 150},
        {"id": "L2", "type": "leaderElected", "startTime": 0, "deadline": 50},
    ])
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2


def test_zero_duration_window(tmp_path, capsys):
    scenario = _base_scenario(
        duration=0,
        livenessChecks=[
            {"id": "z", "type": "leaderElected", "startTime": 0, "deadline": 0},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert _checks(json.loads(out))["z"] == {
        "id": "z",
        "checkType": "leaderElected",
        "status": "failed",
        "time": 0,
        "reason": "deadlineExceeded",
    }


# -- validation --------------------------------------------------------------


def _with_membership(**overrides):
    return _base_scenario(
        nodes=["a", "b", "c", "d"],
        electionTimeouts={"a": 80, "b": 150, "c": 200, "d": 250},
        initialMembers=["a", "b", "c"],
        membershipChanges=[],
        **overrides,
    )


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(livenessChecks={"id": "x"}),
    lambda s: s.update(livenessChecks="nope"),
    lambda s: s.update(livenessChecks=[1]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "startTime": 0, "deadline": 1, "extra": 2}]),
    lambda s: s.update(livenessChecks=[{"type": "leaderElected", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "startTime": 0}]),
    lambda s: s.update(livenessChecks=[{"id": "", "type": "leaderElected", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": 5, "type": "leaderElected", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "startTime": 0, "deadline": 1}] * 2),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "bogus", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "startTime": -1, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "startTime": 1.5, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "startTime": 0, "deadline": 501}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "startTime": 5, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "leaderElected", "startTime": 0, "deadline": 1, "target": "y"}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "clientCommitted", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "readCompleted", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "membershipCommitted", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "clientCommitted", "startTime": 0, "deadline": 1, "target": 123}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "clientCommitted", "startTime": 0, "deadline": 1, "target": "missing"}]),
    lambda s: s.update(livenessChecks=[{"id": "x", "type": "readCompleted", "startTime": 0, "deadline": 1, "target": "missing"}]),
])
def test_invalid_liveness_checks(tmp_path, capsys, mutate):
    scenario = _base_scenario(
        clientCommands=[{"time": 1, "node": "a", "id": "x1", "command": 1}],
        readQueries=[{"time": 1, "node": "a", "id": "q1"}],
    )
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_membership_target_requires_known_change(tmp_path, capsys):
    scenario = _with_membership(livenessChecks=[
        {"id": "x", "type": "membershipCommitted", "startTime": 0, "deadline": 1, "target": "nope"},
    ])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ") and err.count("\n") == 1


def test_unknown_top_level_field_still_rejected(tmp_path, capsys):
    scenario = _base_scenario()
    scenario["liveness"] = []
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2 and out == "" and err.startswith("error: ")
