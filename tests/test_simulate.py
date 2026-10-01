"""Tests for the deterministic Raft election simulation."""

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


def test_basic_election(tmp_path, capsys):
    code, out, err = _run(tmp_path, capsys, _base_scenario())
    assert code == 0
    assert err == ""
    result = json.loads(out)
    assert set(result) == {
        "timeline",
        "nodes",
        "clients",
        "electionSafety",
        "logMatching",
        "stateMachineSafety",
    }
    assert result["nodes"]["a"]["role"] == "leader"
    assert result["nodes"]["a"]["term"] == 1
    assert result["nodes"]["b"]["knownLeader"] == "a"
    assert result["nodes"]["c"]["knownLeader"] == "a"
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}
    # No clientCommands means empty logs, empty client buckets and empty reports.
    assert result["clients"] == {
        "committed": [],
        "superseded": [],
        "pending": [],
        "rejected": [],
    }
    assert result["logMatching"] == {"violations": []}
    assert result["stateMachineSafety"] == {"violations": []}
    for node in result["nodes"].values():
        assert node["log"] == []
        assert node["commitIndex"] == 0
        assert node["lastApplied"] == 0
        assert node["applied"] == []
    seqs = [entry["seq"] for entry in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))
    types = {entry["type"] for entry in result["timeline"]}
    assert {"stateChange", "messageSend", "messageResult", "timeout"} <= types


def test_determinism_byte_identical(tmp_path, capsys):
    scenario = _base_scenario(faults=[
        {"time": 120, "action": "partition", "groups": [["a"], ["b", "c"]]},
        {"time": 300, "action": "heal"},
    ])
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2


def test_partition_drops_messages_and_heals(tmp_path, capsys):
    scenario = _base_scenario(
        duration=800,
        faults=[
            {"time": 0, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 500, "action": "heal"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    dropped = [e for e in result["timeline"] if e.get("result") == "dropped"]
    assert dropped, "expected messages to be dropped across the partition"
    assert all(e["reason"] == "partition" for e in dropped)
    fault_entries = [e for e in result["timeline"] if e["type"] == "fault"]
    assert [e["action"] for e in fault_entries] == ["partition", "heal"]
    # The majority side elects a leader while partitioned.
    assert result["electionSafety"]["violations"] == []
    leaders = set()
    for names in result["electionSafety"]["leadersByTerm"].values():
        leaders.update(names)
    assert leaders


def test_single_election_timeout_value(tmp_path, capsys):
    # A bare positive integer applies the same timeout to every node; with
    # identical timeouts all nodes campaign at once and split the vote.
    code, out, _ = _run(tmp_path, capsys, _base_scenario(electionTimeouts=100, duration=150))
    assert code == 0
    result = json.loads(out)
    assert result["electionSafety"]["leadersByTerm"] == {}
    assert all(n["role"] == "candidate" for n in result["nodes"].values())


def test_events_beyond_duration_are_ignored(tmp_path, capsys):
    code, out, _ = _run(tmp_path, capsys, _base_scenario(duration=50))
    assert code == 0
    result = json.loads(out)
    assert result["timeline"] == []
    assert all(n["role"] == "follower" and n["term"] == 0 for n in result["nodes"].values())


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(nodes=["a", "b"]),
    lambda s: s.update(nodes=["a", "a", "c"]),
    lambda s: s.update(nodes=["a", "", "c"]),
    lambda s: s.update(duration=-1),
    lambda s: s.update(duration=1.5),
    lambda s: s.update(heartbeatInterval=0),
    lambda s: s.update(messageDelay=-1),
    lambda s: s.update(electionTimeouts={"a": 100, "b": 150}),
    lambda s: s.update(electionTimeouts={"a": 100, "b": 150, "c": 200, "z": 1}),
    lambda s: s.update(electionTimeouts={"a": 0, "b": 150, "c": 200}),
    lambda s: s.update(unknownField=1),
    lambda s: s.pop("nodes"),
    lambda s: s.update(faults=[{"time": 9999, "action": "heal"}]),
    lambda s: s.update(faults=[{"time": -1, "action": "heal"}]),
    lambda s: s.update(faults=[{"time": 10, "action": "explode"}]),
    lambda s: s.update(faults=[{"time": 10, "action": "partition"}]),
    lambda s: s.update(faults=[{"time": 10, "action": "partition", "groups": [["a"], ["b"]]}]),
    lambda s: s.update(faults=[{"time": 10, "action": "partition", "groups": [["a", "b"], ["b", "c"]]}]),
    lambda s: s.update(faults=[{"time": 10, "action": "partition", "groups": [["a", "b"], ["c", "z"]]}]),
    lambda s: s.update(faults=[{"time": 10, "action": "heal", "groups": [["a"], ["b", "c"]]}]),
    lambda s: s.update(faults=[{"time": 10, "action": "partition", "groups": [["a"], ["b", "c"]], "extra": 1}]),
])
def test_invalid_scenarios(tmp_path, capsys, mutate):
    scenario = _base_scenario()
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_missing_file(capsys):
    code = main(["simulate", "/nonexistent/scenario.json"])
    out, err = capsys.readouterr()
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")


def test_invalid_json(tmp_path, capsys):
    path = tmp_path / "bad.json"
    path.write_text("{not json", encoding="utf-8")
    code = main(["simulate", str(path)])
    out, err = capsys.readouterr()
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")


def test_non_utf8_file(tmp_path, capsys):
    path = tmp_path / "bad.json"
    path.write_bytes(b"\xff\xfe{}")
    code = main(["simulate", str(path)])
    out, err = capsys.readouterr()
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")


def test_version_and_help_preserved(capsys):
    assert main(["version"]) == 0
    out, _ = capsys.readouterr()
    assert out.strip()
    assert main([]) == 0
    out, _ = capsys.readouterr()
    assert "simulate" in out


# -- log replication -------------------------------------------------------


def _log_replication_scenario(**overrides):
    return _base_scenario(
        duration=400,
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": {"k": "v"}},
            {"time": 260, "node": "b", "id": "x2", "command": "raw"},
        ],
        **overrides,
    )


def _by_type(result, event_type):
    return [e for e in result["timeline"] if e["type"] == event_type]


def test_command_replicated_and_committed(tmp_path, capsys):
    code, out, err = _run(tmp_path, capsys, _log_replication_scenario())
    assert code == 0
    assert err == ""
    result = json.loads(out)

    clients = result["clients"]
    assert clients["committed"] == [{"id": "x1", "node": "a", "index": 1, "term": 1}]
    assert clients["superseded"] == []
    assert clients["pending"] == []
    assert clients["rejected"] == [
        {"id": "x2", "node": "b", "reason": "notLeader", "knownLeader": "a"}
    ]

    for name, node in result["nodes"].items():
        assert node["log"] == [{"index": 1, "term": 1, "id": "x1", "command": {"k": "v"}}]
        assert node["commitIndex"] == 1
        assert node["lastApplied"] == 1
        assert node["applied"] == [{"index": 1, "term": 1, "id": "x1", "command": {"k": "v"}}]

    # One accepted client result, one rejected; exactly one applied event per node.
    client_results = _by_type(result, "clientResult")
    accepted = [e for e in client_results if e["result"] == "accepted"]
    rejected = [e for e in client_results if e["result"] == "rejected"]
    assert [(e["node"], e["id"], e["index"], e["term"]) for e in accepted] == [("a", "x1", 1, 1)]
    assert [(e["node"], e["id"], e["reason"], e["knownLeader"]) for e in rejected] == [
        ("b", "x2", "notLeader", "a")
    ]
    applied = _by_type(result, "applied")
    assert sorted((e["node"], e["index"], e["id"]) for e in applied) == [
        ("a", 1, "x1"),
        ("b", 1, "x1"),
        ("c", 1, "x1"),
    ]

    # Replication traffic and replies are recorded with globally increasing seq.
    messages = {e["message"] for e in _by_type(result, "messageSend")}
    assert {"appendEntries"} <= messages
    reply_results = [
        e for e in _by_type(result, "messageResult") if e["message"] == "appendReply"
    ]
    assert reply_results
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


def test_seq_is_global_and_contiguous(tmp_path, capsys):
    code, out, _ = _run(tmp_path, capsys, _log_replication_scenario())
    assert code == 0
    result = json.loads(out)
    seqs = [e["seq"] for e in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def test_same_time_commands_processed_in_input_order(tmp_path, capsys):
    scenario = _base_scenario(
        duration=300,
        messageDelay=0,
        clientCommands=[
            {"time": 200, "node": "a", "id": "c1", "command": 1},
            {"time": 200, "node": "a", "id": "c2", "command": 2},
            {"time": 200, "node": "b", "id": "c3", "command": 3},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    at_200 = [e for e in result["timeline"] if e["time"] == 200 and e["type"] == "clientResult"]
    assert [e["id"] for e in at_200] == ["c1", "c2", "c3"]
    assert [e["result"] for e in at_200] == ["accepted", "accepted", "rejected"]
    ids = {c["id"] for c in result["clients"]["committed"]}
    assert ids == {"c1", "c2"}


def test_stale_uncommitted_entry_is_superseded_not_applied(tmp_path, capsys):
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
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)

    clients = result["clients"]
    committed = [(c["id"], c["index"], c["term"]) for c in clients["committed"]]
    assert committed == [("x1", 1, 1), ("fresh", 2, 2)]
    assert clients["superseded"] == [{"id": "stale", "node": "a"}]
    assert clients["pending"] == []
    assert clients["rejected"] == []

    # The stale entry must never have been applied by any node.
    applied_ids = {e["id"] for e in _by_type(result, "applied")}
    assert "stale" not in applied_ids
    # Every node converges on the same log.
    logs = {
        name: [(e["index"], e["term"], e["id"]) for e in node["log"]]
        for name, node in result["nodes"].items()
    }
    assert len({tuple(log) for log in logs.values()}) == 1
    assert result["stateMachineSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []


def test_unreplicated_entry_stays_pending(tmp_path, capsys):
    scenario = _base_scenario(
        duration=700,
        faults=[{"time": 300, "action": "partition", "groups": [["a"], ["b", "c"]]}],
        clientCommands=[
            {"time": 200, "node": "a", "id": "ok", "command": 1},
            {"time": 400, "node": "a", "id": "orphan", "command": 2},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    clients = result["clients"]
    assert [c["id"] for c in clients["committed"]] == ["ok"]
    assert clients["pending"] == [{"id": "orphan", "node": "a", "index": 2, "term": 1}]
    assert clients["superseded"] == []
    # The orphan entry exists only on the isolated leader.
    holders = [
        name
        for name, node in result["nodes"].items()
        if any(e["id"] == "orphan" for e in node["log"])
    ]
    assert holders == ["a"]
    assert all(e["id"] != "orphan" for e in _by_type(result, "applied"))


def test_replication_output_is_deterministic(tmp_path, capsys):
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
    )
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(clientCommands="not-a-list"),
    lambda s: s.update(clientCommands=[1]),
    lambda s: s.update(clientCommands=[{"time": 1, "node": "a", "id": "z", "command": 1, "extra": 2}]),
    lambda s: s.update(clientCommands=[{"time": 1, "node": "a", "id": "z"}]),
    lambda s: s.update(clientCommands=[{"time": 1, "node": "a", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 1, "id": "z", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": "x", "node": "a", "id": "z", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": -1, "node": "a", "id": "z", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 501, "node": "a", "id": "z", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 1, "node": "zz", "id": "z", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 1, "node": "", "id": "z", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 1, "node": "a", "id": "", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 1, "node": "a", "id": 5, "command": 1}]),
    lambda s: s.update(
        clientCommands=[
            {"time": 1, "node": "a", "id": "z", "command": 1},
            {"time": 2, "node": "a", "id": "z", "command": 2},
        ]
    ),
])
def test_invalid_client_commands(tmp_path, capsys, mutate):
    scenario = _base_scenario()
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1
