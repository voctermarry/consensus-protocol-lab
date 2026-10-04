"""Tests for storage faults: failures injected at numbered persistence
barriers, with rollback to the last successfully saved state and optional
automatic restart."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main


def _write(tmp_path, name, payload):
    path = tmp_path / name
    path.write_text(json.dumps(payload), encoding="utf-8")
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


def _run(tmp_path, capsys, scenario):
    path = _write(tmp_path, "scenario.json", scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _events(result, *types):
    return [entry for entry in result["timeline"] if entry["type"] in types]


# -- validation --------------------------------------------------------------


@pytest.mark.parametrize(
    "storage_faults,message",
    [
        ({}, "error: storageFaults must be a list\n"),
        ([[]], "error: storageFaults[0] must be an object\n"),
        (
            [{"node": "a", "occurrence": 1, "time": 3}],
            "error: storageFaults[0] has unknown field(s): time\n",
        ),
        (
            [{"occurrence": 1}],
            "error: storageFaults[0] missing field(s): node\n",
        ),
        (
            [{"node": "a"}],
            "error: storageFaults[0] missing field(s): occurrence\n",
        ),
        (
            [{"node": "", "occurrence": 1}],
            "error: storageFaults[0].node must be a non-empty string\n",
        ),
        (
            [{"node": "z", "occurrence": 1}],
            "error: storageFaults[0].node references unknown node: 'z'\n",
        ),
        (
            [{"node": "a", "occurrence": 0}],
            "error: storageFaults[0].occurrence must be a positive integer\n",
        ),
        (
            [{"node": "a", "occurrence": -2}],
            "error: storageFaults[0].occurrence must be a positive integer\n",
        ),
        (
            [{"node": "a", "occurrence": 1.5}],
            "error: storageFaults[0].occurrence must be an integer\n",
        ),
        (
            [{"node": "a", "occurrence": True}],
            "error: storageFaults[0].occurrence must be an integer\n",
        ),
        (
            [{"node": "a", "occurrence": 1, "restartDelay": -1}],
            "error: storageFaults[0].restartDelay must be a non-negative integer\n",
        ),
        (
            [{"node": "a", "occurrence": 1, "restartDelay": "soon"}],
            "error: storageFaults[0].restartDelay must be an integer\n",
        ),
        (
            [{"node": "a", "occurrence": 1}, {"node": "a", "occurrence": 1}],
            "error: storageFaults[1] duplicates the selector of storageFaults[0]\n",
        ),
    ],
)
def test_invalid_storage_faults(tmp_path, capsys, storage_faults, message):
    code, out, err = _run(tmp_path, capsys, _base_scenario(storageFaults=storage_faults))
    assert code == 2
    assert out == ""
    assert err == message


def test_storage_faults_reject_explicit_node_events(tmp_path, capsys):
    scenario = _base_scenario(
        nodeEvents=[{"time": 10, "node": "a", "action": "crash"}],
        storageFaults=[{"node": "a", "occurrence": 1}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err == "error: storageFaults and nodeEvents must not be provided together\n"


def test_unmatched_rule_produces_no_events(tmp_path, capsys):
    scenario = _base_scenario(storageFaults=[{"node": "a", "occurrence": 99}])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert err == ""
    result = json.loads(out)
    assert _events(result, "storageFault", "nodeLifecycle") == []
    # A configured (but never hit) rule set already enables the lifecycle
    # fields, exactly like a provided nodeEvents list does.
    assert result["nodes"]["a"]["online"] is True
    assert result["nodes"]["a"]["restartCount"] == 0
    assert result["nodes"]["a"]["role"] == "leader"


def test_empty_storage_faults_is_byte_identical(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "c1", "command": "x"}]
    )
    code, baseline, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    code, out, err = _run(tmp_path, capsys, dict(scenario, storageFaults=[]))
    assert code == 0 and err == ""
    assert out == baseline


# -- fault injection -----------------------------------------------------------


def test_fault_at_election_barrier_with_zero_restart_delay(tmp_path, capsys):
    # a's first barrier is its election (term + votedFor); a zero restart
    # delay still records the crash and the restart, in that order, before
    # the remaining events at the same instant.
    scenario = _base_scenario(
        duration=300,
        storageFaults=[{"node": "a", "occurrence": 1, "restartDelay": 0}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    fault, crash, restart = _events(result, "storageFault", "nodeLifecycle")[:3]
    assert fault == {
        "seq": 1,
        "time": 100,
        "type": "storageFault",
        "node": "a",
        "occurrence": 1,
        "fields": ["term", "votedFor"],
    }
    assert (crash["time"], crash["action"]) == (100, "crash")
    assert (restart["time"], restart["action"]) == (100, "restart")
    assert crash["seq"] < restart["seq"]
    # The failed update was rolled back: a restarted from term 0 and later
    # followed b's election; the cluster still converges on one leader.
    assert result["electionSafety"]["violations"] == []
    assert result["nodes"]["a"]["online"] is True
    assert result["nodes"]["a"]["restartCount"] == 1
    assert result["nodes"]["a"]["votedFor"] == "b"
    assert result["nodes"]["b"]["role"] == "leader"


def test_fault_at_command_append_suppresses_every_dependent_effect(tmp_path, capsys):
    # a's barriers: 1 = election, 2 = appending c1. The failed append must
    # not reach the persistent image, and the acceptance plus the
    # replication traffic depending on it must stay invisible.
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "c1", "command": "x"}],
        storageFaults=[{"node": "a", "occurrence": 2, "restartDelay": 50}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    (fault,) = _events(result, "storageFault")
    assert (fault["time"], fault["node"], fault["occurrence"], fault["fields"]) == (
        200,
        "a",
        2,
        ["log"],
    )
    # No clientResult for c1 at all: the acceptance depended on the failed
    # save, and no rejection was recorded either.
    assert _events(result, "clientResult") == []
    # No appendEntries carried c1 anywhere; the command vanished with the
    # failed barrier.
    assert result["clients"]["superseded"] == [{"id": "c1", "node": "a"}]
    assert all(
        entry["id"] != "c1"
        for node in result["nodes"].values()
        for entry in node["log"] + node["applied"]
    )
    # a crashed at the same instant and restarted 50ms later from its last
    # saved state (term 1, voted for itself, empty log).
    lifecycle = _events(result, "nodeLifecycle")
    assert [(e["time"], e["action"]) for e in lifecycle] == [(200, "crash"), (250, "restart")]
    node_a = result["nodes"]["a"]
    assert node_a["online"] is True
    assert node_a["restartCount"] == 1
    assert node_a["log"] == []
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


def test_fault_at_commit_barrier_rolls_back_commit_and_apply(tmp_path, capsys):
    # a's barriers: 1 = election, 2 = append c1, 3 = commit/apply of c1.
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "c1", "command": "x"}],
        storageFaults=[{"node": "a", "occurrence": 3, "restartDelay": 10}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    (fault,) = _events(result, "storageFault")
    assert fault["fields"] == ["commitIndex", "lastApplied", "applied"]
    node_a = result["nodes"]["a"]
    # The commit never landed: c1 survives only as an uncommitted entry.
    assert node_a["commitIndex"] == 0
    assert node_a["lastApplied"] == 0
    assert node_a["applied"] == []
    assert result["clients"]["pending"] == [{"id": "c1", "node": "a", "index": 1, "term": 1}]


def test_messages_sent_before_the_fault_still_arrive(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "c1", "command": "x"}],
        storageFaults=[{"node": "a", "occurrence": 2, "restartDelay": 50}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    # a's pre-fault heartbeats (sent at 120 and 170) were delivered to both
    # peers; both had accepted a as leader before the crash.
    heartbeats = [
        entry
        for entry in result["timeline"]
        if entry["type"] == "messageResult"
        and entry["message"] == "heartbeat"
        and entry["result"] == "delivered"
        and entry["time"] < 200
    ]
    assert heartbeats
    assert result["electionSafety"]["leadersByTerm"]["1"] == ["a"]


def test_node_without_restart_delay_stays_offline(tmp_path, capsys):
    scenario = _base_scenario(
        duration=300,
        clientCommands=[{"time": 250, "node": "a", "id": "c2", "command": "y"}],
        storageFaults=[{"node": "a", "occurrence": 1}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    assert [(e["action"]) for e in _events(result, "nodeLifecycle")] == ["crash"]
    node_a = result["nodes"]["a"]
    assert node_a["online"] is False
    assert node_a["restartCount"] == 0
    # Rolled back to the last saved state: term 0, no vote, follower.
    assert node_a["term"] == 0
    assert node_a["votedFor"] is None
    # Traffic to the offline node follows nodeDown semantics.
    (command_result,) = _events(result, "clientResult")
    assert command_result["result"] == "rejected"
    assert command_result["reason"] == "nodeDown"
    dropped = [
        entry
        for entry in result["timeline"]
        if entry["type"] == "messageResult"
        and entry["node"] == "a"
        and entry["result"] == "dropped"
    ]
    assert dropped
    assert all(entry["reason"] == "nodeDown" for entry in dropped)


def test_restart_beyond_duration_is_not_executed(tmp_path, capsys):
    scenario = _base_scenario(
        duration=300,
        storageFaults=[{"node": "a", "occurrence": 1, "restartDelay": 1000}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    assert [(e["action"]) for e in _events(result, "nodeLifecycle")] == ["crash"]
    assert result["nodes"]["a"]["online"] is False
    assert result["nodes"]["a"]["restartCount"] == 0


def test_barrier_counting_continues_across_restarts(tmp_path, capsys):
    # a's barrier 1 (its election at t=100) fails with an immediate restart;
    # the next barrier for a — voting in b's election — is occurrence 2.
    scenario = _base_scenario(
        duration=300,
        storageFaults=[
            {"node": "a", "occurrence": 1, "restartDelay": 0},
            {"node": "a", "occurrence": 2},
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    faults = _events(result, "storageFault")
    assert [(f["time"], f["occurrence"]) for f in faults] == [(100, 1), (160, 2)]
    assert faults[1]["fields"] == ["term", "votedFor"]
    # The second rule has no restartDelay: a stays offline, still at the
    # state saved by its first successful barrier-less restart (term 0).
    node_a = result["nodes"]["a"]
    assert node_a["online"] is False
    assert node_a["restartCount"] == 1
    assert node_a["term"] == 0
    # b's election needed only b and c, so the cluster still has a leader.
    assert result["nodes"]["b"]["role"] == "leader"


def test_suppressed_send_does_not_consume_message_fault_occurrence(tmp_path, capsys):
    # b's barrier 2 (appending c1 at t=210) fails; the appendReply it would
    # have sent is suppressed and must not consume the first
    # (b -> a, appendReply) occurrence, so the drop rule hits the reply b
    # sends after its restart instead.
    scenario = _base_scenario(
        duration=400,
        clientCommands=[{"time": 200, "node": "a", "id": "c1", "command": "x"}],
        messageFaults=[
            {"from": "b", "to": "a", "message": "appendReply", "occurrence": 1, "action": "drop"}
        ],
        storageFaults=[{"node": "b", "occurrence": 2, "restartDelay": 0}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    (message_fault,) = _events(result, "messageFault")
    assert message_fault["time"] == 230
    assert message_fault["occurrence"] == 1
    # c1 still committed (a and c form a majority) and b caught up after
    # its restart.
    assert result["clients"]["committed"] == [{"id": "c1", "node": "a", "index": 1, "term": 1}]
    assert result["nodes"]["b"]["commitIndex"] == 1
    assert result["logMatching"]["violations"] == []


def test_fault_does_not_corrupt_unaffected_bookkeeping(tmp_path, capsys):
    # Two commands: c1 commits before the fault; the fault kills c2's
    # append. c1 must stay committed afterwards.
    scenario = _base_scenario(
        clientCommands=[
            {"time": 200, "node": "a", "id": "c1", "command": "x"},
            {"time": 300, "node": "a", "id": "c2", "command": "y"},
        ],
        # a's barriers: 1 election, 2 append c1, 3 commit c1, 4 append c2.
        storageFaults=[{"node": "a", "occurrence": 4, "restartDelay": 0}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    (fault,) = _events(result, "storageFault")
    assert (fault["time"], fault["occurrence"], fault["fields"]) == (300, 4, ["log"])
    assert result["clients"]["committed"] == [{"id": "c1", "node": "a", "index": 1, "term": 1}]
    assert result["clients"]["superseded"] == [{"id": "c2", "node": "a"}]
    node_a = result["nodes"]["a"]
    assert node_a["commitIndex"] == 1
    assert [entry["id"] for entry in node_a["applied"]] == ["c1"]


def test_determinism_byte_identical(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "c1", "command": "x"}],
        storageFaults=[
            {"node": "a", "occurrence": 2, "restartDelay": 50},
            {"node": "b", "occurrence": 3},
        ],
    )
    _, first, _ = _run(tmp_path, capsys, scenario)
    _, second, _ = _run(tmp_path, capsys, scenario)
    assert first == second


# -- explore and replay --------------------------------------------------------


def test_explore_accepts_storage_faults_in_base_scenario(tmp_path, capsys):
    plan = {
        "scenario": _base_scenario(
            duration=300,
            storageFaults=[{"node": "a", "occurrence": 1, "restartDelay": 20}],
        ),
        "candidates": [
            {"from": "b", "to": "c", "message": "heartbeat", "occurrence": 1, "action": "drop"}
        ],
        "maxFaults": 1,
        "maxCases": 10,
    }
    path = _write(tmp_path, "plan.json", plan)
    code = main(["explore", path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    result = json.loads(out)
    assert result["totalCases"] == 2
    for case in result["cases"]:
        faults = [
            entry for entry in case["result"]["timeline"] if entry["type"] == "storageFault"
        ]
        assert len(faults) == 1
        assert faults[0]["node"] == "a"


def test_replay_matches_saved_storage_fault_result(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "c1", "command": "x"}],
        storageFaults=[{"node": "a", "occurrence": 2, "restartDelay": 50}],
    )
    scenario_path = _write(tmp_path, "scenario.json", scenario)
    code = main(["simulate", scenario_path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    result_path = _write(tmp_path, "result.json", json.loads(out))
    code = main(["replay", scenario_path, result_path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    assert json.loads(out) == {"status": "matched"}
