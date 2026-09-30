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
    assert set(result) == {"timeline", "nodes", "electionSafety"}
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
