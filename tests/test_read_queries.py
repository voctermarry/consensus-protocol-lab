"""Tests for optional readQueries: readIndex confirmation and linearizability."""

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
        "messageDelay": 10,
    }
    scenario.update(overrides)
    return scenario


def _run(tmp_path, capsys, scenario):
    path = _write_scenario(tmp_path, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _by_type(result, event_type):
    return [e for e in result["timeline"] if e["type"] == event_type]


# -- backward compatibility -------------------------------------------------


def test_omitted_read_queries_keeps_legacy_shape(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}]
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    assert "reads" not in result
    assert "linearizability" not in result
    assert not _by_type(result, "readResult")
    assert not [
        e for e in result["timeline"]
        if e.get("message") in ("readProbe", "readReply")
    ]


def test_empty_read_queries_adds_empty_reports(tmp_path, capsys):
    code, out, err = _run(tmp_path, capsys, _base_scenario(readQueries=[]))
    assert code == 0 and err == ""
    result = json.loads(out)
    assert result["reads"] == []
    assert result["linearizability"] == {"violations": []}
    assert not _by_type(result, "readResult")


# -- completed reads --------------------------------------------------------


def test_completed_read_returns_committed_prefix(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": "one"},
            {"time": 210, "node": "a", "id": "x2", "command": {"k": "v"}},
        ],
        readQueries=[{"time": 250, "node": "a", "id": "q1"}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    assert result["reads"] == [
        {
            "id": "q1",
            "node": "a",
            "result": "completed",
            "term": 1,
            "readIndex": 2,
            "state": [
                {"index": 1, "term": 1, "id": "x1", "command": "one"},
                {"index": 2, "term": 1, "id": "x2", "command": {"k": "v"}},
            ],
        }
    ]
    assert result["linearizability"] == {"violations": []}

    results = {e["id"]: e for e in _by_type(result, "readResult")}
    assert results["q1"]["result"] == "completed"
    # The accepted event precedes the probes; the completed event ends it.
    q_events = [
        e for e in result["timeline"]
        if e.get("readId") == "q1"
        or (e["type"] == "readResult" and e["id"] == "q1")
    ]
    kinds = [(e["type"], e.get("result") or e.get("message")) for e in q_events]
    assert kinds[0] == ("readResult", "accepted")
    completed_events = [e for e in q_events if e["type"] == "readResult"
                        and e.get("result") == "completed"]
    assert len(completed_events) == 1
    assert kinds.index(("readResult", "accepted")) < kinds.index(
        ("readResult", "completed")
    )

    # Probes go out to every other voter and carry the query id.
    sends = [
        e for e in _by_type(result, "messageSend")
        if e["message"] == "readProbe" and e["readId"] == "q1"
    ]
    assert sorted(e["peer"] for e in sends) == ["b", "c"]
    # The first reply completes the read; the second reply is ignored rather
    # than mixed into another round.
    replies = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "readReply" and e["readId"] == "q1"
    ]
    assert sorted(e["detail"] for e in replies) == ["acknowledged", "ignored"]


def test_read_with_no_writes_completes_with_empty_state(tmp_path, capsys):
    scenario = _base_scenario(readQueries=[{"time": 150, "node": "a", "id": "q0"}])
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"][0]["result"] == "completed"
    assert result["reads"][0]["readIndex"] == 0
    assert result["reads"][0]["state"] == []
    assert result["linearizability"]["violations"] == []


def test_zero_delay_completes_at_same_timestamp(tmp_path, capsys):
    # No later write is needed: with the majority reachable the zero-delay
    # probes and replies drain inside the read timestamp.
    scenario = _base_scenario(
        messageDelay=0,
        readQueries=[{"time": 150, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    events = [
        e for e in result["timeline"]
        if e["type"] == "readResult"
        or e.get("message") in ("readProbe", "readReply")
    ]
    assert {e["time"] for e in events} == {150}
    assert result["reads"][0]["result"] == "completed"
    assert result["reads"][0]["readIndex"] == 0


def test_read_admitted_after_same_time_command_cascade(tmp_path, capsys):
    # The client command at t=200 (zero delay) replicates and commits before
    # the read sharing the timestamp is admitted.
    scenario = _base_scenario(
        messageDelay=0,
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        readQueries=[{"time": 200, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    read = result["reads"][0]
    assert read["result"] == "completed"
    assert read["readIndex"] == 1
    assert [c["id"] for c in read["state"]] == ["x1"]
    timeline = _by_type(result, "readResult")
    accepted_client = next(
        e for e in _by_type(result, "clientResult") if e["id"] == "x1"
    )
    accepted_read = next(e for e in timeline if e["result"] == "accepted")
    assert accepted_client["seq"] < accepted_read["seq"]


def test_reads_same_timestamp_follow_input_order(tmp_path, capsys):
    scenario = _base_scenario(
        messageDelay=0,
        readQueries=[
            {"time": 150, "node": "a", "id": "q1"},
            {"time": 150, "node": "b", "id": "q2"},
            {"time": 150, "node": "a", "id": "q3"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    read_events = [
        e for e in _by_type(result, "readResult") if e["time"] == 150
    ]
    # Both admissions (and the rejection) happen in input order before either
    # zero-delay confirmation cascade drains.
    assert [
        (e["id"], e["result"]) for e in read_events
    ] == [
        ("q1", "accepted"),
        ("q2", "rejected"),
        ("q3", "accepted"),
        ("q1", "completed"),
        ("q3", "completed"),
    ]
    accepted = [
        e["id"] for e in _by_type(result, "readResult") if e["result"] == "accepted"
    ]
    assert accepted == ["q1", "q3"]
    assert [row["id"] for row in result["reads"]] == ["q1", "q2", "q3"]
    by_id = {row["id"]: row for row in result["reads"]}
    assert by_id["q1"]["result"] == "completed"
    assert by_id["q2"]["result"] == "rejected"
    assert by_id["q2"]["reason"] == "notLeader"
    assert by_id["q2"]["knownLeader"] == "a"
    assert by_id["q3"]["result"] == "completed"


# -- immediate rejections ---------------------------------------------------


def test_read_to_offline_node_rejected_node_down(tmp_path, capsys):
    scenario = _base_scenario(
        nodeEvents=[{"time": 140, "node": "a", "action": "crash"}],
        readQueries=[{"time": 200, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"] == [
        {"id": "q1", "node": "a", "result": "rejected",
         "reason": "nodeDown", "knownLeader": None}
    ]
    event = _by_type(result, "readResult")[0]
    assert event == {
        "seq": event["seq"], "time": 200, "type": "readResult",
        "node": "a", "id": "q1", "result": "rejected",
        "reason": "nodeDown", "knownLeader": None,
    }
    assert not [
        e for e in _by_type(result, "messageSend") if e["message"] == "readProbe"
    ]


def test_read_to_non_leader_carries_known_leader(tmp_path, capsys):
    scenario = _base_scenario(
        readQueries=[{"time": 200, "node": "b", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"][0]["reason"] == "notLeader"
    assert result["reads"][0]["knownLeader"] == "a"


# -- leadership loss and pending --------------------------------------------


def test_accepted_read_rejected_when_leader_crashes(tmp_path, capsys):
    scenario = _base_scenario(
        messageDelay=20,
        nodeEvents=[{"time": 160, "node": "a", "action": "crash"}],
        readQueries=[{"time": 150, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"] == [
        {"id": "q1", "node": "a", "result": "rejected",
         "reason": "leadershipLost"}
    ]
    results = _by_type(result, "readResult")
    assert [e["result"] for e in results] == ["accepted", "rejected"]
    assert results[-1]["reason"] == "leadershipLost"


def test_accepted_read_rejected_on_term_change(tmp_path, capsys):
    # Partitioned right after admission, the old leader is deposed after the
    # partition heals; the open read ends leadershipLost.
    scenario = _base_scenario(
        faults=[
            {"time": 255, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 450, "action": "heal"},
        ],
        readQueries=[{"time": 250, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"][0]["result"] == "rejected"
    assert result["reads"][0]["reason"] == "leadershipLost"


def test_read_pending_when_all_replies_dropped(tmp_path, capsys):
    scenario = _base_scenario(
        duration=280,
        electionTimeouts={"a": 100, "b": 1500, "c": 2000},
        messageFaults=[
            {"from": "b", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "drop"},
            {"from": "c", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "drop"},
        ],
        readQueries=[{"time": 150, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"] == [{"id": "q1", "node": "a", "result": "pending"}]
    results = _by_type(result, "readResult")
    assert [e["result"] for e in results] == ["accepted", "pending"]
    # The leader kept its identity; it simply never got a same-term majority.
    assert result["nodes"]["a"]["role"] == "leader"
    assert result["nodes"]["a"]["term"] == 1


def test_dropped_probes_block_the_majority(tmp_path, capsys):
    scenario = _base_scenario(
        duration=280,
        electionTimeouts={"a": 100, "b": 1500, "c": 2000},
        messageFaults=[
            {"from": "a", "to": "b", "message": "readProbe", "occurrence": 1,
             "action": "drop"},
            {"from": "a", "to": "c", "message": "readProbe", "occurrence": 1,
             "action": "drop"},
        ],
        readQueries=[{"time": 150, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"][0]["result"] == "pending"
    drops = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "readProbe" and e.get("reason") == "messageFault"
    ]
    assert sorted(e["node"] for e in drops) == ["b", "c"]
    assert all(e["readId"] == "q1" for e in drops)


def test_probe_partitioned_at_arrival_is_dropped_and_read_pending(tmp_path, capsys):
    scenario = _base_scenario(
        duration=400,
        electionTimeouts={"a": 100, "b": 1500, "c": 2000},
        faults=[
            {"time": 155, "action": "partition", "groups": [["a"], ["b", "c"]]},
        ],
        readQueries=[{"time": 150, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"][0]["result"] == "pending"
    drops = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "readProbe" and e.get("reason") == "partition"
    ]
    assert {e["node"] for e in drops} == {"b", "c"}
    assert all(e["readId"] == "q1" for e in drops)


def test_higher_term_reply_deposes_leader_via_read_round(tmp_path, capsys):
    # The probe to b is delayed until after b won term 2 across the healed
    # partition. b's heartbeats to a are dropped, so the staleTerm probe
    # result and term-2 readReply are the first term-2 traffic a sees: the
    # read round itself ends the read.
    scenario = _base_scenario(
        duration=600,
        heartbeatInterval=200,
        faults=[
            {"time": 155, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 505, "action": "heal"},
        ],
        messageFaults=[
            {"from": "a", "to": "b", "message": "readProbe", "occurrence": 1,
             "action": "delay", "delay": 400},
            {"from": "b", "to": "a", "message": "heartbeat", "occurrence": 1,
             "action": "drop"},
            {"from": "b", "to": "a", "message": "heartbeat", "occurrence": 2,
             "action": "drop"},
        ],
        readQueries=[{"time": 150, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["reads"][0]["result"] == "rejected"
    assert result["reads"][0]["reason"] == "leadershipLost"
    probes = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "readProbe" and e.get("readId") == "q1"
    ]
    assert any(e.get("detail") == "staleTerm" and e["node"] == "b" for e in probes)
    replies = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "readReply" and e.get("readId") == "q1"
    ]
    assert any(e["detail"] == "higherTerm" for e in replies)


# -- learners, joint consensus, snapshots -----------------------------------


def _membership_base(**overrides):
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 600,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "initialMembers": ["a", "b", "c"],
        "membershipChanges": [],
    }
    scenario.update(overrides)
    return scenario


def test_learner_is_not_probed_or_counted(tmp_path, capsys):
    scenario = _membership_base(readQueries=[{"time": 150, "node": "a", "id": "q1"}])
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    # Stable {a,b,c}: only b and c are probed; any majority of two including
    # the leader completes the read.
    probes = [
        e for e in _by_type(result, "messageSend") if e["message"] == "readProbe"
    ]
    assert sorted(e["peer"] for e in probes) == ["b", "c"]
    assert result["reads"][0]["result"] == "completed"
    # No probe traffic ever involves the learner.
    assert not [
        e for e in result["timeline"]
        if e.get("node") == "d" and e.get("message") in ("readProbe", "readReply")
    ]


def test_joint_phase_requires_majorities_of_both_sets(tmp_path, capsys):
    # Five voters removing e: joint old={a,b,c,d,e} new={a,b,c,d}. Dropping
    # replies from c and d leaves the leader with acks {a,b,e}: old set has
    # 3/5 (majority), new set only 2/4 (no majority) -> the read stays open.
    common = dict(
        nodes=["a", "b", "c", "d", "e"],
        duration=350,
        electionTimeouts={"a": 80, "b": 150, "c": 200, "d": 250, "e": 300},
        heartbeatInterval=50,
        messageDelay=5,
        initialMembers=["a", "b", "c", "d", "e"],
        membershipChanges=[
            {"time": 200, "node": "a", "id": "rm-e", "action": "remove", "member": "e"}
        ],
        readQueries=[{"time": 205, "node": "a", "id": "q1"}],
    )
    blocked = _membership_base(
        **common,
        messageFaults=[
            {"from": "c", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "drop"},
            {"from": "d", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "drop"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, blocked)
    assert code == 0
    result = json.loads(out)
    assert result["reads"][0]["result"] == "pending"
    probes = [
        e for e in _by_type(result, "messageSend") if e["message"] == "readProbe"
    ]
    # The removed voter still belongs to the old joint set and is probed.
    assert sorted(e["peer"] for e in probes) == ["b", "c", "d", "e"]

    # With only one new-set reply missing, both majorities are satisfied.
    code, out, _ = _run(
        tmp_path,
        capsys,
        _membership_base(
            **common,
            messageFaults=[
                {"from": "d", "to": "a", "message": "readReply", "occurrence": 1,
                 "action": "drop"},
            ],
        ),
    )
    assert code == 0
    result = json.loads(out)
    assert result["reads"][0]["result"] == "completed"


def test_state_excludes_config_entries_and_includes_compacted(tmp_path, capsys):
    scenario = _membership_base(
        duration=700,
        snapshotThreshold=2,
        membershipChanges=[
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        clientCommands=[
            {"time": 300, "node": "a", "id": "x1", "command": "one"},
            {"time": 320, "node": "a", "id": "x2", "command": "two"},
        ],
        readQueries=[{"time": 500, "node": "a", "id": "q1"}],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    read = result["reads"][0]
    assert read["result"] == "completed"
    # Indices 1 and 2 are the joint/stable config entries: skipped, while
    # snapshot compaction still returns the commands.
    assert read["readIndex"] == 4
    assert read["state"] == [
        {"index": 3, "term": 1, "id": "x1", "command": "one"},
        {"index": 4, "term": 1, "id": "x2", "command": "two"},
    ]
    assert result["linearizability"]["violations"] == []


# -- determinism -------------------------------------------------------------


def test_read_output_is_deterministic_and_seq_contiguous(tmp_path, capsys):
    scenario = _base_scenario(
        faults=[
            {"time": 255, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 400, "action": "heal"},
        ],
        messageFaults=[
            {"from": "a", "to": "b", "message": "readProbe", "occurrence": 1,
             "action": "delay", "delay": 30},
        ],
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        readQueries=[
            {"time": 150, "node": "a", "id": "q1"},
            {"time": 260, "node": "b", "id": "q2"},
        ],
    )
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2
    parsed = json.loads(out1)
    seqs = [e["seq"] for e in parsed["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


# -- validation --------------------------------------------------------------


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(readQueries="not-a-list"),
    lambda s: s.update(readQueries=[1]),
    lambda s: s.update(readQueries=[{"time": 1, "node": "a"}]),
    lambda s: s.update(readQueries=[{"node": "a", "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": 1, "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": 1, "node": "a", "id": "q", "x": 2}]),
    lambda s: s.update(readQueries=[{"time": "x", "node": "a", "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": -1, "node": "a", "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": 1.5, "node": "a", "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": 501, "node": "a", "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": 1, "node": "z", "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": 1, "node": "", "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": 1, "node": 5, "id": "q"}]),
    lambda s: s.update(readQueries=[{"time": 1, "node": "a", "id": ""}]),
    lambda s: s.update(readQueries=[{"time": 1, "node": "a", "id": 4}]),
    lambda s: s.update(readQueries=[
        {"time": 1, "node": "a", "id": "q"},
        {"time": 2, "node": "a", "id": "q"},
    ]),
    lambda s: s.update(
        readQueries=[{"time": 1, "node": "a", "id": "dup"}],
        clientCommands=[{"time": 2, "node": "a", "id": "dup", "command": 1}],
    ),
])
def test_invalid_read_queries(tmp_path, capsys, mutate):
    scenario = _base_scenario()
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_read_id_duplicate_with_membership_id(tmp_path, capsys):
    scenario = _membership_base(
        readQueries=[{"time": 100, "node": "a", "id": "m1"}],
        membershipChanges=[
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")


def test_unknown_read_message_fault_still_rejected(tmp_path, capsys):
    # readProbe/readReply are valid selectors; an unknown name is not.
    scenario = _base_scenario(
        readQueries=[{"time": 150, "node": "a", "id": "q1"}],
        messageFaults=[
            {"from": "a", "to": "b", "message": "readProbe", "occurrence": 1,
             "action": "drop"},
            {"from": "a", "to": "c", "message": "readPing", "occurrence": 1,
             "action": "drop"},
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
