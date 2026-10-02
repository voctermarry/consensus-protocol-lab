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
        "duration": 600,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 10,
    }
    scenario.update(overrides)
    return scenario


def _run(tmp_path, capsys, scenario):
    path = _write_scenario(tmp_path, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _run_ok(tmp_path, capsys, scenario):
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert err == ""
    return json.loads(out)


def _liveness_events(result):
    return [entry for entry in result["timeline"] if "status" in entry]


# -- leaderElected -----------------------------------------------------------


def test_leader_elected_satisfied_at_first_holding_time(tmp_path, capsys):
    # a's election timeout fires at 100; with message delay 10 it wins at 120.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        livenessChecks=[
            {"id": "lv", "type": "leaderElected", "startTime": 0, "deadline": 500}
        ],
    ))
    assert result["liveness"]["checks"] == [
        {"id": "lv", "type": "leaderElected", "status": "satisfied", "time": 120}
    ]
    assert result["liveness"]["violations"] == []
    events = _liveness_events(result)
    assert events == [
        {"seq": events[0]["seq"], "time": 120, "type": "leaderElected",
         "id": "lv", "status": "satisfied"}
    ]


def test_leader_already_present_reports_start_time(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        livenessChecks=[
            {"id": "lv", "type": "leaderElected", "startTime": 200, "deadline": 500}
        ],
    ))
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "leaderElected", "status": "satisfied", "time": 200
    }


def test_leader_check_requires_online_leader_in_window(tmp_path, capsys):
    # a leads from ~120 until it crashes at 250; b only wins around 350.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=700,
        nodeEvents=[{"time": 250, "node": "a", "action": "crash"}],
        livenessChecks=[
            {"id": "gap", "type": "leaderElected", "startTime": 300, "deadline": 305},
            {"id": "later", "type": "leaderElected", "startTime": 300, "deadline": 400},
        ],
    ))
    by_id = {check["id"]: check for check in result["liveness"]["checks"]}
    assert by_id["gap"] == {
        "id": "gap", "type": "leaderElected", "status": "failed",
        "time": 305, "reason": "deadlineExceeded",
    }
    assert by_id["later"]["status"] == "satisfied"
    # The first time an online leader exists again, taken from the actual
    # majority stateChange recorded by the run.
    elected_times = [
        entry["time"]
        for entry in result["timeline"]
        if entry["type"] == "stateChange" and entry["reason"] == "majority"
    ]
    assert by_id["later"]["time"] == next(t for t in elected_times if t >= 300)
    assert [v["id"] for v in result["liveness"]["violations"]] == ["gap"]


def test_leader_absent_at_zero_fails_zero_width_window(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        livenessChecks=[
            {"id": "lv", "type": "leaderElected", "startTime": 0, "deadline": 0}
        ],
    ))
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "leaderElected", "status": "failed",
        "time": 0, "reason": "deadlineExceeded",
    }


# -- clientCommitted ---------------------------------------------------------


def test_client_committed_satisfied(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=400,
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        livenessChecks=[
            {"id": "lv", "type": "clientCommitted", "target": "x1",
             "startTime": 200, "deadline": 400}
        ],
    ))
    assert result["liveness"]["checks"] == [
        {"id": "lv", "type": "clientCommitted", "target": "x1",
         "status": "satisfied", "time": 220}
    ]
    event = _liveness_events(result)[0]
    assert event == {
        "seq": event["seq"], "time": 220, "type": "clientCommitted",
        "id": "lv", "target": "x1", "status": "satisfied",
    }


def test_client_committed_rejected_target(tmp_path, capsys):
    # x2 goes to follower b and is rejected at submission time.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=400,
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": 1},
            {"time": 260, "node": "b", "id": "x2", "command": 2},
        ],
        livenessChecks=[
            {"id": "lv", "type": "clientCommitted", "target": "x2",
             "startTime": 260, "deadline": 400}
        ],
    ))
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "clientCommitted", "target": "x2",
        "status": "failed", "time": 400, "reason": "targetRejected",
    }


def _supersede_scenario(deadline):
    return _base_scenario(
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
            {"id": "lv", "type": "clientCommitted", "target": "stale",
             "startTime": 350, "deadline": deadline}
        ],
    )


def test_client_committed_superseded_target(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _supersede_scenario(750))
    assert result["clients"]["superseded"] == [{"id": "stale", "node": "a"}]
    assert result["liveness"]["checks"][0]["reason"] == "targetSuperseded"


def test_client_committed_still_pending_is_deadline_exceeded(tmp_path, capsys):
    # Partition never heals inside the window: the orphan entry is still in
    # the isolated leader's log at the deadline.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=700,
        faults=[{"time": 300, "action": "partition", "groups": [["a"], ["b", "c"]]}],
        clientCommands=[
            {"time": 200, "node": "a", "id": "ok", "command": 1},
            {"time": 400, "node": "a", "id": "orphan", "command": 2},
        ],
        livenessChecks=[
            {"id": "lv", "type": "clientCommitted", "target": "orphan",
             "startTime": 400, "deadline": 700}
        ],
    ))
    assert [c["id"] for c in result["clients"]["pending"]] == ["orphan"]
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "clientCommitted", "target": "orphan",
        "status": "failed", "time": 700, "reason": "deadlineExceeded",
    }


# -- readCompleted -----------------------------------------------------------


def test_read_completed_satisfied(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
        livenessChecks=[
            {"id": "lv", "type": "readCompleted", "target": "r1",
             "startTime": 300, "deadline": 500}
        ],
    ))
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "readCompleted", "target": "r1",
        "status": "satisfied", "time": 320,
    }


def test_read_completed_rejected_target(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        readQueries=[{"time": 300, "node": "b", "id": "r1"}],
        livenessChecks=[
            {"id": "lv", "type": "readCompleted", "target": "r1",
             "startTime": 300, "deadline": 500}
        ],
    ))
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "readCompleted", "target": "r1",
        "status": "failed", "time": 500, "reason": "targetRejected",
    }


def test_read_still_pending_is_deadline_exceeded(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=700,
        faults=[{"time": 300, "action": "partition", "groups": [["a"], ["b", "c"]]}],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
        livenessChecks=[
            {"id": "lv", "type": "readCompleted", "target": "r1",
             "startTime": 300, "deadline": 700}
        ],
    ))
    assert result["reads"][0]["outcome"] == "pending"
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "readCompleted", "target": "r1",
        "status": "failed", "time": 700, "reason": "deadlineExceeded",
    }


# -- membershipCommitted -----------------------------------------------------


def _membership_scenario(**overrides):
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 600,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "initialMembers": ["a", "b", "c"],
        "membershipChanges": [
            {"time": 140, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
    }
    scenario.update(overrides)
    return scenario


def test_membership_committed_satisfied(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _membership_scenario(
        livenessChecks=[
            {"id": "lv", "type": "membershipCommitted", "target": "m1",
             "startTime": 140, "deadline": 600}
        ],
    ))
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "membershipCommitted", "target": "m1",
        "status": "satisfied", "time": 160,
    }


def test_membership_rejected_target(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _membership_scenario(
        duration=400,
        membershipChanges=[
            {"time": 300, "node": "b", "id": "m1", "action": "add", "member": "d"}
        ],
        livenessChecks=[
            {"id": "lv", "type": "membershipCommitted", "target": "m1",
             "startTime": 300, "deadline": 400}
        ],
    ))
    assert result["liveness"]["checks"][0] == {
        "id": "lv", "type": "membershipCommitted", "target": "m1",
        "status": "failed", "time": 400, "reason": "targetRejected",
    }


# -- ordering, seq and exit code ---------------------------------------------


def test_checks_evaluate_after_reactions_in_input_order(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        messageDelay=0,
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        livenessChecks=[
            {"id": "lv1", "type": "clientCommitted", "target": "x1",
             "startTime": 200, "deadline": 200},
            {"id": "lv2", "type": "leaderElected", "startTime": 200, "deadline": 200},
        ],
    ))
    # The zero-width window is satisfied at 200: the command's zero-delay
    # replication, commit and apply cascade all drained first.
    assert [c["id"] for c in result["liveness"]["checks"]] == ["lv1", "lv2"]
    assert all(c["status"] == "satisfied" and c["time"] == 200
               for c in result["liveness"]["checks"])
    at_200 = [entry for entry in result["timeline"] if entry["time"] == 200]
    # Both liveness records are the final entries at that timestamp ...
    assert [entry["id"] for entry in at_200[-2:]] == ["lv1", "lv2"]
    # ... and no pre-existing event kind follows them.
    assert all("status" in entry for entry in at_200[-2:])
    assert not any("status" in entry for entry in at_200[:-2])
    seqs = [entry["seq"] for entry in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def test_failed_checks_do_not_change_exit_code(tmp_path, capsys):
    code, out, err = _run(tmp_path, capsys, _base_scenario(
        livenessChecks=[
            {"id": "lv", "type": "leaderElected", "startTime": 0, "deadline": 0}
        ],
    ))
    assert code == 0
    assert err == ""
    result = json.loads(out)
    assert result["liveness"]["violations"][0]["status"] == "failed"


def test_checks_reported_in_input_order_with_all_violations(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=400,
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": 1},
            {"time": 260, "node": "b", "id": "x2", "command": 2},
        ],
        livenessChecks=[
            {"id": "c-fail", "type": "leaderElected", "startTime": 0, "deadline": 0},
            {"id": "b-ok", "type": "clientCommitted", "target": "x1",
             "startTime": 200, "deadline": 400},
            {"id": "a-fail", "type": "clientCommitted", "target": "x2",
             "startTime": 260, "deadline": 300},
        ],
    ))
    assert [c["id"] for c in result["liveness"]["checks"]] == ["c-fail", "b-ok", "a-fail"]
    assert [v["id"] for v in result["liveness"]["violations"]] == ["c-fail", "a-fail"]
    assert result["liveness"]["violations"][1]["reason"] == "targetRejected"


# -- omission, empty list, determinism ---------------------------------------


def test_liveness_omitted_keeps_legacy_shape(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario())
    assert "liveness" not in result
    assert _liveness_events(result) == []


def test_liveness_empty_list_adds_empty_report(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(livenessChecks=[]))
    assert result["liveness"] == {"checks": [], "violations": []}
    assert _liveness_events(result) == []


def test_liveness_output_is_deterministic(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        livenessChecks=[
            {"id": "lv1", "type": "leaderElected", "startTime": 0, "deadline": 500},
            {"id": "lv2", "type": "clientCommitted", "target": "x1",
             "startTime": 300, "deadline": 500},
        ],
    )
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2


def test_providing_liveness_leaves_other_semantics_untouched(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        livenessChecks=[
            {"id": "lv", "type": "clientCommitted", "target": "x1",
             "startTime": 200, "deadline": 500}
        ],
    )
    with_liveness = _run_ok(tmp_path, capsys, scenario)
    scenario.pop("livenessChecks")
    without_liveness = _run_ok(tmp_path, capsys, scenario)

    def strip(events):
        return [
            {key: value for key, value in event.items() if key != "seq"}
            for event in events
            if "status" not in event
        ]

    assert strip(with_liveness["timeline"]) == strip(without_liveness["timeline"])
    for key in without_liveness:
        if key == "timeline":
            continue
        assert with_liveness[key] == without_liveness[key]
    assert set(with_liveness) - set(without_liveness) == {"liveness"}


# -- validation ---------------------------------------------------------------


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(livenessChecks="no"),
    lambda s: s.update(livenessChecks={}),
    lambda s: s.update(livenessChecks=[1]),
    lambda s: s.update(livenessChecks=[{"id": "", "type": "leaderElected",
                                        "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": 5, "type": "leaderElected",
                                        "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "nope",
                                        "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "leaderElected",
                                        "startTime": 1, "deadline": 0}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "leaderElected",
                                        "startTime": -1, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "leaderElected",
                                        "startTime": 0, "deadline": 601}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "leaderElected",
                                        "startTime": True, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "leaderElected",
                                        "startTime": 0, "deadline": 1, "extra": 2}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "leaderElected",
                                        "target": "x", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "clientCommitted",
                                        "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "clientCommitted",
                                        "target": "", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "clientCommitted",
                                        "target": "nope", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "readCompleted",
                                        "target": "nope", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "membershipCommitted",
                                        "target": "nope", "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[{"id": "z", "type": "leaderElected",
                                        "startTime": 0}]),
    lambda s: s.update(livenessChecks=[{"type": "leaderElected",
                                        "startTime": 0, "deadline": 1}]),
    lambda s: s.update(livenessChecks=[
        {"id": "z", "type": "leaderElected", "startTime": 0, "deadline": 1},
        {"id": "z", "type": "leaderElected", "startTime": 0, "deadline": 1},
    ]),
])
def test_invalid_liveness_checks(tmp_path, capsys, mutate):
    scenario = _base_scenario()
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_target_must_match_target_pool(tmp_path, capsys):
    # A read id must not satisfy a clientCommitted target reference.
    scenario = _base_scenario(
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
        livenessChecks=[
            {"id": "z", "type": "clientCommitted", "target": "r1",
             "startTime": 0, "deadline": 1}
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert "clientCommands" in err


def test_membership_target_requires_membership_feature(tmp_path, capsys):
    scenario = _base_scenario(
        livenessChecks=[
            {"id": "z", "type": "membershipCommitted", "target": "m1",
             "startTime": 0, "deadline": 1}
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
