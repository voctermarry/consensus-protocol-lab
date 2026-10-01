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
    assert set(result) == {"timeline", "nodes", "clients", "electionSafety", "logMatching", "stateMachineSafety"}
    assert result["nodes"]["a"]["role"] == "leader"
    assert result["nodes"]["a"]["term"] == 1
    assert result["nodes"]["b"]["knownLeader"] == "a"
    assert result["nodes"]["c"]["knownLeader"] == "a"
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}
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


def test_no_client_commands_leaves_new_fields_empty(tmp_path, capsys):
    code, out, _ = _run(tmp_path, capsys, _base_scenario())
    assert code == 0
    result = json.loads(out)
    assert result["clients"] == {}
    assert result["logMatching"] == {"violations": []}
    assert result["stateMachineSafety"] == {"violations": []}
    for node in result["nodes"].values():
        assert node["log"] == []
        assert node["commitIndex"] == 0
        assert node["lastApplied"] == 0
        assert node["applied"] == []


def test_client_command_replicated_and_committed(tmp_path, capsys):
    scenario = _base_scenario(clientCommands=[
        {"time": 300, "node": "a", "id": "cmd-1", "command": {"op": "set", "key": "x", "value": 1}},
    ])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert err == ""
    result = json.loads(out)
    assert result["clients"] == {
        "cmd-1": {"node": "a", "time": 300, "status": "committed", "index": 1, "term": 1}
    }
    entry = {"index": 1, "term": 1, "id": "cmd-1", "command": {"op": "set", "key": "x", "value": 1}}
    for node in result["nodes"].values():
        assert node["log"] == [entry]
        assert node["commitIndex"] == 1
        assert node["lastApplied"] == 1
        assert node["applied"] == [{"index": 1, "id": "cmd-1", "command": {"op": "set", "key": "x", "value": 1}}]
    types = [e["type"] for e in result["timeline"]]
    assert "clientResult" in types and "apply" in types
    messages = {e.get("message") for e in result["timeline"] if e["type"] == "messageSend"}
    assert {"appendEntries", "appendEntriesReply"} <= messages
    results = [e for e in result["timeline"] if e["type"] == "clientResult"]
    assert [e["result"] for e in results] == ["accepted", "committed"]
    assert result["logMatching"] == {"violations": []}
    assert result["stateMachineSafety"] == {"violations": []}


def test_client_command_to_non_leader_is_rejected(tmp_path, capsys):
    scenario = _base_scenario(clientCommands=[
        {"time": 300, "node": "b", "id": "cmd-1", "command": "noop"},
    ])
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["clients"] == {
        "cmd-1": {"node": "b", "time": 300, "status": "rejected", "knownLeader": "a"}
    }
    assert all(node["log"] == [] for node in result["nodes"].values())
    rejected = [e for e in result["timeline"] if e["type"] == "clientResult"]
    assert len(rejected) == 1
    assert rejected[0]["result"] == "rejected"
    assert rejected[0]["reason"] == "notLeader"
    assert rejected[0]["knownLeader"] == "a"


def test_client_commands_processed_before_timeouts_at_same_time(tmp_path, capsys):
    # At t=100 node a's election timeout fires; the client command at the same
    # time is processed first, while a is still a follower.
    scenario = _base_scenario(clientCommands=[
        {"time": 100, "node": "a", "id": "early", "command": 1},
    ])
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["clients"]["early"]["status"] == "rejected"
    assert result["clients"]["early"]["knownLeader"] is None


def test_same_time_client_commands_keep_input_order(tmp_path, capsys):
    scenario = _base_scenario(clientCommands=[
        {"time": 300, "node": "a", "id": "first", "command": 1},
        {"time": 300, "node": "a", "id": "second", "command": 2},
    ])
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["clients"]["first"]["index"] == 1
    assert result["clients"]["second"]["index"] == 2
    assert [e["id"] for e in result["nodes"]["a"]["log"]] == ["first", "second"]


def test_conflicting_entry_is_superseded(tmp_path, capsys):
    # a is elected in term 1 and accepts x while partitioned away; b wins
    # term 2 on the majority side and commits y at the same index, so x can
    # never commit. After the heal, a's log converges to the leader's.
    scenario = _base_scenario(
        duration=900,
        faults=[
            {"time": 200, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 600, "action": "heal"},
        ],
        clientCommands=[
            {"time": 250, "node": "a", "id": "x", "command": "from-a"},
            {"time": 500, "node": "b", "id": "y", "command": "from-b"},
            {"time": 700, "node": "b", "id": "z", "command": "after-heal"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["clients"]["x"]["status"] == "superseded"
    assert result["clients"]["x"]["index"] == 1
    assert result["clients"]["y"]["status"] == "committed"
    assert result["clients"]["z"]["status"] == "committed"
    committed = [e for e in result["timeline"] if e["type"] == "clientResult" and e["result"] == "committed"]
    assert [e["id"] for e in committed] == ["y", "z"]  # each id committed at most once
    logs = {name: [(e["index"], e["term"], e["id"]) for e in node["log"]] for name, node in result["nodes"].items()}
    assert logs == {name: [(1, 2, "y"), (2, 2, "z")] for name in ("a", "b", "c")}
    assert result["logMatching"] == {"violations": []}
    assert result["stateMachineSafety"] == {"violations": []}


def test_candidate_with_stale_log_is_denied_votes(tmp_path, capsys):
    # c misses the committed entry while partitioned; after the heal its
    # RequestVote arrives at nodes with a newer log and is denied.
    scenario = _base_scenario(
        duration=900,
        faults=[
            {"time": 150, "action": "partition", "groups": [["a", "b"], ["c"]]},
            {"time": 400, "action": "heal"},
        ],
        clientCommands=[
            {"time": 200, "node": "a", "id": "x", "command": 1},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    denials = [e for e in result["timeline"] if e.get("detail") == "voteDenied"]
    assert any(e["peer"] == "c" for e in denials)
    for term, leaders in result["electionSafety"]["leadersByTerm"].items():
        assert "c" not in leaders
    assert result["clients"]["x"]["status"] == "committed"


def test_client_commands_determinism_byte_identical(tmp_path, capsys):
    scenario = _base_scenario(
        duration=900,
        faults=[
            {"time": 200, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 600, "action": "heal"},
        ],
        clientCommands=[
            {"time": 250, "node": "a", "id": "x", "command": {"v": [1, 2, 3]}},
            {"time": 500, "node": "b", "id": "y", "command": "from-b"},
            {"time": 700, "node": "b", "id": "z", "command": None},
        ],
    )
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(clientCommands={}),
    lambda s: s.update(clientCommands=["not-an-object"]),
    lambda s: s.update(clientCommands=[{"time": 10, "node": "a", "id": "i", "command": 1, "extra": 1}]),
    lambda s: s.update(clientCommands=[{"time": 10, "node": "a", "id": "i"}]),
    lambda s: s.update(clientCommands=[{"time": -1, "node": "a", "id": "i", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 1.5, "node": "a", "id": "i", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 9999, "node": "a", "id": "i", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 10, "node": "z", "id": "i", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 10, "node": 1, "id": "i", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 10, "node": "a", "id": "", "command": 1}]),
    lambda s: s.update(clientCommands=[{"time": 10, "node": "a", "id": 7, "command": 1}]),
    lambda s: s.update(clientCommands=[
        {"time": 10, "node": "a", "id": "dup", "command": 1},
        {"time": 20, "node": "b", "id": "dup", "command": 2},
    ]),
])
def test_invalid_client_commands(tmp_path, capsys, mutate):
    scenario = _base_scenario()
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1
