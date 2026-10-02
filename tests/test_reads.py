"""Tests for read-only queries and the linearizability report."""

from __future__ import annotations

import json

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


def test_read_completes_with_state(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        clientCommands=[
            {"time": 140, "node": "a", "id": "w1", "command": {"op": "set", "key": "x"}},
            {"time": 300, "node": "a", "id": "w2", "command": {"op": "set", "key": "y"}},
        ],
        readQueries=[{"time": 400, "node": "a", "id": "r1"}],
    ))
    assert result["reads"] == [
        {
            "id": "r1",
            "node": "a",
            "outcome": "completed",
            "term": 1,
            "readIndex": 2,
            "state": [
                {"index": 1, "term": 1, "id": "w1", "command": {"op": "set", "key": "x"}},
                {"index": 2, "term": 1, "id": "w2", "command": {"op": "set", "key": "y"}},
            ],
        }
    ]
    assert result["linearizability"] == {"violations": []}
    # The probe exchange and the read results appear in the timeline.
    kinds = [
        (entry["type"], entry.get("message"), entry.get("result"))
        for entry in result["timeline"]
        if entry.get("id") == "r1" or entry.get("message") in ("readProbe", "readReply")
    ]
    assert ("readResult", None, "accepted") in kinds
    assert ("readResult", None, "completed") in kinds
    assert ("messageSend", "readProbe", None) in kinds
    assert ("messageResult", "readProbe", "delivered") in kinds
    assert ("messageSend", "readReply", None) in kinds
    assert ("messageResult", "readReply", "delivered") in kinds
    # Probe/reply timeline entries carry the query id.
    for entry in result["timeline"]:
        if entry.get("message") in ("readProbe", "readReply"):
            assert entry["id"] == "r1"


def test_read_rejected_not_leader(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        readQueries=[{"time": 400, "node": "b", "id": "r1"}],
    ))
    assert result["reads"] == [
        {
            "id": "r1",
            "node": "b",
            "outcome": "rejected",
            "reason": "notLeader",
            "knownLeader": "a",
        }
    ]
    events = [e for e in result["timeline"] if e["type"] == "readResult"]
    assert events == [
        {
            "seq": events[0]["seq"],
            "time": 400,
            "type": "readResult",
            "node": "b",
            "id": "r1",
            "result": "rejected",
            "reason": "notLeader",
            "knownLeader": "a",
        }
    ]


def test_read_rejected_node_down(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        nodeEvents=[{"time": 300, "node": "a", "action": "crash"}],
        readQueries=[{"time": 400, "node": "a", "id": "r1"}],
    ))
    assert result["reads"] == [
        {
            "id": "r1",
            "node": "a",
            "outcome": "rejected",
            "reason": "nodeDown",
            "knownLeader": None,
        }
    ]


def test_read_leadership_lost_on_crash(tmp_path, capsys):
    # The leader accepts the read at 300 and crashes at 301, before the
    # probes (delay 10) can be answered.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        nodeEvents=[{"time": 301, "node": "a", "action": "crash"}],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
    ))
    assert result["reads"] == [
        {
            "id": "r1",
            "node": "a",
            "outcome": "rejected",
            "reason": "leadershipLost",
            "knownLeader": None,
        }
    ]
    outcomes = [
        (e["time"], e["result"], e.get("reason"))
        for e in result["timeline"]
        if e["type"] == "readResult"
    ]
    assert (300, "accepted", None) in outcomes
    assert (301, "rejected", "leadershipLost") in outcomes


def test_read_leadership_lost_on_higher_term_reply(tmp_path, capsys):
    # c is partitioned away, campaigns to term 2, and rejects the probe with
    # its higher term; the readProbe to b is dropped, so no majority forms
    # before the higher-term readReply deposes a.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=700,
        faults=[
            {"time": 250, "action": "partition", "groups": [["a", "b"], ["c"]]},
            {"time": 450, "action": "heal"},
        ],
        messageFaults=[
            {"from": "a", "to": "b", "message": "readProbe", "occurrence": 1, "action": "drop"},
        ],
        readQueries=[{"time": 455, "node": "a", "id": "r1"}],
    ))
    assert result["reads"] == [
        {
            "id": "r1",
            "node": "a",
            "outcome": "rejected",
            "reason": "leadershipLost",
            "knownLeader": None,
        }
    ]
    replies = [
        e for e in result["timeline"]
        if e.get("message") == "readReply" and e["type"] == "messageResult"
    ]
    assert any(e["detail"] == "higherTerm" for e in replies)


def test_read_pending_when_majority_partitioned(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        faults=[{"time": 300, "action": "partition", "groups": [["a"], ["b", "c"]]}],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
    ))
    assert result["reads"] == [
        {"id": "r1", "node": "a", "outcome": "pending", "term": 1, "readIndex": 0}
    ]
    # The probes were dropped by the partition.
    drops = [
        e for e in result["timeline"]
        if e.get("message") == "readProbe" and e.get("result") == "dropped"
    ]
    assert len(drops) == 2
    assert {e["reason"] for e in drops} == {"partition"}


def test_read_zero_delay_completes_in_input_order(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        messageDelay=0,
        clientCommands=[{"time": 200, "node": "a", "id": "w1", "command": 1}],
        readQueries=[
            {"time": 300, "node": "a", "id": "r1"},
            {"time": 300, "node": "a", "id": "r2"},
        ],
    ))
    assert [read["outcome"] for read in result["reads"]] == ["completed", "completed"]
    # Acceptance happens in input order and each query's zero-delay cascade
    # drains before the next one is accepted, all at the same timestamp.
    events = [
        (e["time"], e["id"], e["result"])
        for e in result["timeline"]
        if e["type"] == "readResult"
    ]
    assert events == [
        (300, "r1", "accepted"),
        (300, "r1", "completed"),
        (300, "r2", "accepted"),
        (300, "r2", "completed"),
    ]


def test_read_stale_read_violation(tmp_path, capsys):
    # a commits w1 at ~160 and crashes before the followers learn the commit;
    # b wins the next election with commitIndex still 0 and serves the read
    # from readIndex 0, omitting the already-committed write.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=700,
        clientCommands=[{"time": 140, "node": "a", "id": "w1", "command": "x"}],
        nodeEvents=[{"time": 165, "node": "a", "action": "crash"}],
        readQueries=[{"time": 400, "node": "b", "id": "r1"}],
    ))
    assert result["reads"] == [
        {
            "id": "r1",
            "node": "b",
            "outcome": "completed",
            "term": 2,
            "readIndex": 0,
            "state": [],
        }
    ]
    assert result["linearizability"]["violations"] == [
        {"type": "staleRead", "id": "r1", "missing": ["w1"]}
    ]


def test_read_state_includes_compacted_commands(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        duration=900,
        snapshotThreshold=2,
        clientCommands=[
            {"time": 140, "node": "a", "id": "w1", "command": "x"},
            {"time": 200, "node": "a", "id": "w2", "command": "y"},
            {"time": 260, "node": "a", "id": "w3", "command": "z"},
        ],
        readQueries=[{"time": 500, "node": "a", "id": "r1"}],
    ))
    assert result["nodes"]["a"]["snapshot"] == {"lastIncludedIndex": 2, "lastIncludedTerm": 1}
    read = result["reads"][0]
    assert read["outcome"] == "completed"
    assert [entry["id"] for entry in read["state"]] == ["w1", "w2", "w3"]
    assert result["linearizability"] == {"violations": []}


def test_read_message_fault_drop_probe(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(
        messageFaults=[
            {"from": "a", "to": "b", "message": "readProbe", "occurrence": 1, "action": "drop"},
            {"from": "a", "to": "c", "message": "readProbe", "occurrence": 1, "action": "drop"},
        ],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
    ))
    assert result["reads"][0]["outcome"] == "pending"
    faults = [e for e in result["timeline"] if e["type"] == "messageFault"]
    assert {e["message"] for e in faults} == {"readProbe"}
    drops = [
        e for e in result["timeline"]
        if e.get("message") == "readProbe" and e.get("reason") == "messageFault"
    ]
    assert len(drops) == 2
    assert all(e["id"] == "r1" for e in drops)


def test_read_message_fault_delay_reply(tmp_path, capsys):
    # Delaying one readReply postpones completion but does not prevent it.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        messageFaults=[
            {"from": "b", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "delay", "delay": 100},
        ],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
    ))
    assert result["reads"][0]["outcome"] == "completed"
    completed = [
        e for e in result["timeline"]
        if e["type"] == "readResult" and e["result"] == "completed"
    ]
    # b's delayed reply lands at 420; c's reply already completes the
    # majority at 320.
    assert completed[0]["time"] == 320


def test_read_joint_quorum_requires_both_sets(tmp_path, capsys):
    base = _base_scenario(
        nodes=["a", "b", "c", "d"],
        duration=500,
        electionTimeouts={"a": 100, "b": 150, "c": 200, "d": 250},
        initialMembers=["a", "b", "c"],
        membershipChanges=[
            {"time": 140, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
    )
    # c and d crash while the joint configuration is in effect: the old set
    # majority {a, b} is reachable, but the new set {a, b, c, d} majority is
    # not, so the read stays pending.
    stuck = _run_ok(tmp_path, capsys, _base_scenario(
        **{**base, "nodeEvents": [
            {"time": 165, "node": "c", "action": "crash"},
            {"time": 165, "node": "d", "action": "crash"},
        ]},
    ))
    assert stuck["reads"][0]["outcome"] == "pending"
    # With only c down, a/b/d form a majority of both sets and the read
    # completes; the configuration entries are not part of the read state.
    ok = _run_ok(tmp_path, capsys, _base_scenario(
        **{**base, "nodeEvents": [{"time": 165, "node": "c", "action": "crash"}]},
    ))
    assert ok["reads"][0]["outcome"] == "completed"
    assert ok["reads"][0]["state"] == []
    assert ok["linearizability"] == {"violations": []}


def test_read_learner_reply_not_counted(tmp_path, capsys):
    # Learner d is down; the voter majority alone completes the read.
    result = _run_ok(tmp_path, capsys, _base_scenario(
        nodes=["a", "b", "c", "d"],
        electionTimeouts={"a": 100, "b": 150, "c": 200, "d": 250},
        initialMembers=["a", "b", "c"],
        membershipChanges=[],
        nodeEvents=[{"time": 250, "node": "d", "action": "crash"}],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
    ))
    assert result["reads"][0]["outcome"] == "completed"


def test_reads_omitted_output_unchanged(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 140, "node": "a", "id": "w1", "command": 1}],
    )
    result = _run_ok(tmp_path, capsys, scenario)
    assert "reads" not in result
    assert "linearizability" not in result
    assert not any(e["type"] == "readResult" for e in result["timeline"])
    assert not any(
        e.get("message") in ("readProbe", "readReply") for e in result["timeline"]
    )


def test_reads_empty_list_adds_empty_reports(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(readQueries=[]))
    assert result["reads"] == []
    assert result["linearizability"] == {"violations": []}


def test_read_determinism_byte_identical(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 140, "node": "a", "id": "w1", "command": "x"}],
        readQueries=[
            {"time": 300, "node": "a", "id": "r1"},
            {"time": 320, "node": "b", "id": "r2"},
        ],
    )
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2


def _assert_scenario_error(tmp_path, capsys, scenario):
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")


def test_read_queries_validation(tmp_path, capsys):
    bad_scenarios = [
        _base_scenario(readQueries={}),
        _base_scenario(readQueries=[1]),
        _base_scenario(readQueries=[{"time": 0, "node": "a", "id": "r", "x": 1}]),
        _base_scenario(readQueries=[{"time": 0, "node": "a"}]),
        _base_scenario(readQueries=[{"time": 601, "node": "a", "id": "r"}]),
        _base_scenario(readQueries=[{"time": -1, "node": "a", "id": "r"}]),
        _base_scenario(readQueries=[{"time": True, "node": "a", "id": "r"}]),
        _base_scenario(readQueries=[{"time": 0, "node": "z", "id": "r"}]),
        _base_scenario(readQueries=[{"time": 0, "node": "", "id": "r"}]),
        _base_scenario(readQueries=[{"time": 0, "node": "a", "id": ""}]),
        _base_scenario(readQueries=[
            {"time": 0, "node": "a", "id": "r"},
            {"time": 1, "node": "a", "id": "r"},
        ]),
        _base_scenario(
            clientCommands=[{"time": 0, "node": "a", "id": "r", "command": 1}],
            readQueries=[{"time": 0, "node": "a", "id": "r"}],
        ),
        _base_scenario(
            initialMembers=["a", "b", "c"],
            membershipChanges=[
                {"time": 0, "node": "a", "id": "r", "action": "remove", "member": "c"}
            ],
            readQueries=[{"time": 0, "node": "a", "id": "r"}],
        ),
    ]
    for scenario in bad_scenarios:
        _assert_scenario_error(tmp_path, capsys, scenario)


def test_read_probe_reply_are_valid_message_fault_kinds(tmp_path, capsys):
    # readProbe/readReply are accepted as messageFaults message kinds even
    # when the scenario has no read queries (the rules simply never match).
    result = _run_ok(tmp_path, capsys, _base_scenario(
        messageFaults=[
            {"from": "a", "to": "b", "message": "readProbe", "occurrence": 1, "action": "drop"},
            {"from": "b", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "delay", "delay": 3},
        ],
    ))
    assert "reads" not in result
    assert not [e for e in result["timeline"] if e["type"] == "messageFault"]
