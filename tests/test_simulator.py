"""Tests for the deterministic Raft leader-election simulator."""

from __future__ import annotations

import json

import pytest

from consensus_lab.simulator import (
    SimulationError,
    parse_scenario,
    run_simulation,
)


def scenario(**overrides):
    base = {
        "nodes": ["n1", "n2", "n3"],
        "duration": 100,
        "electionTimeouts": [10, 15, 20],
        "heartbeatInterval": 5,
        "messageDelay": 2,
    }
    base.update(overrides)
    return base


def run(raw):
    return run_simulation(parse_scenario(raw))


def states(result):
    return [(e["time"], e["node"], e["role"], e["term"], e["reason"]) for e in result["timeline"] if e["type"] == "state"]


# ---------------------------------------------------------------- validation

@pytest.mark.parametrize(
    "patch",
    [
        lambda s: s.update(nodes=["a", "b"]),
        lambda s: s.update(nodes=["a", "a", "b"]),
        lambda s: s.update(nodes=["a", "", "b"]),
        lambda s: s.pop("nodes"),
        lambda s: s.pop("duration"),
        lambda s: s.pop("electionTimeouts"),
        lambda s: s.pop("heartbeatInterval"),
        lambda s: s.pop("messageDelay"),
        lambda s: s.update(unknown=1),
        lambda s: s.update(duration=-1),
        lambda s: s.update(duration=True),
        lambda s: s.update(duration="10"),
        lambda s: s.update(electionTimeouts=[10, 15]),
        lambda s: s.update(electionTimeouts=[10, 0, 20]),
        lambda s: s.update(heartbeatInterval=0),
        lambda s: s.update(messageDelay=-2),
    ],
)
def test_invalid_scenarios_rejected(patch):
    raw = scenario()
    patch(raw)
    with pytest.raises(SimulationError):
        parse_scenario(raw)


def test_not_an_object():
    with pytest.raises(SimulationError):
        parse_scenario([1, 2, 3])


def test_fault_validation():
    good_groups = {"faults": [{"time": 1, "action": "partition", "groups": [["n1"], ["n2", "n3"]]}]}
    assert run(scenario(**good_groups)) is not None

    bad = [
        {"time": 101, "action": "heal"},
        {"time": -1, "action": "heal"},
        {"time": 1, "action": "partition", "groups": [["n1"], ["n2", "x"]]},
        {"time": 1, "action": "partition", "groups": [["n1", "n2"], ["n2", "n3"]]},
        {"time": 1, "action": "partition", "groups": [["n1"], ["n2"]]},
        {"time": 1, "action": "heal", "groups": [["n1"], ["n2", "n3"]]},
        {"time": 1, "action": "explode"},
    ]
    for fault in bad:
        with pytest.raises(SimulationError):
            parse_scenario(scenario(faults=[fault]))


# -------------------------------------------------------------- happy paths

def test_basic_election_and_heartbeats():
    result = run(scenario())
    final = result["nodes"]

    assert final["n1"]["role"] == "leader"
    assert final["n1"]["term"] == 1
    assert final["n1"]["votedFor"] == "n1"
    assert final["n2"]["role"] == "follower"
    assert final["n2"]["votedFor"] == "n1"
    assert final["n2"]["knownLeader"] == "n1"

    assert result["electionSafety"]["violations"] == []
    assert result["electionSafety"]["terms"] == [
        {"term": 1, "leaders": ["n1"], "violation": False}
    ]

    # First timeout -> candidate, majority -> elected.
    assert states(result)[0] == (10, "n1", "candidate", 1, "election-timeout")
    assert (12, "n1", "leader", 1, "elected") in states(result)

    # Heartbeats keep flowing after the election.
    hb_sent = [e for e in result["timeline"] if e["type"] == "message-sent" and e["message"] == "AppendEntries"]
    assert len(hb_sent) > 2
    assert all(e["term"] == 1 for e in hb_sent)


def test_one_vote_per_term():
    # Two candidates share the first timeout; each already holds its own vote,
    # so neither can grant the other a vote in the same term.
    result = run(
        scenario(
            nodes=["a", "b", "c"],
            electionTimeouts=[10, 10, 100],
            heartbeatInterval=5,
            messageDelay=1,
            duration=60,
        )
    )
    denials = [
        e for e in result["timeline"]
        if e["type"] == "message" and e.get("reason") == "already-voted"
    ]
    assert denials
    assert result["electionSafety"]["violations"] == []


def test_split_vote_resolves_on_heal():
    result = run(
        scenario(
            nodes=["a", "b", "c", "d"],
            duration=400,
            electionTimeouts=[100, 100, 300, 300],
            heartbeatInterval=20,
            messageDelay=5,
            faults=[
                {"time": 90, "action": "partition", "groups": [["a", "b"], ["c", "d"]]},
                {"time": 260, "action": "heal"},
            ],
        )
    )
    # No leader while the 2-2 partition lasts.
    during_partition = [
        e for e in result["timeline"]
        if e["type"] == "state" and e["role"] == "leader" and 90 <= e["time"] < 260
    ]
    assert during_partition == []

    leaders = {name: state["role"] for name, state in result["nodes"].items()}
    assert sum(1 for role in leaders.values() if role == "leader") == 1
    assert result["electionSafety"]["violations"] == []


def test_partition_drops_cross_group_messages_and_heal_converges():
    result = run(
        scenario(
            nodes=["n1", "n2", "n3", "n4", "n5"],
            duration=300,
            electionTimeouts=[50, 60, 70, 80, 90],
            heartbeatInterval=10,
            messageDelay=2,
            faults=[
                {"time": 70, "action": "partition", "groups": [["n1"], ["n2", "n3", "n4", "n5"]]},
                {"time": 200, "action": "heal"},
            ],
        )
    )
    dropped = [e for e in result["timeline"] if e.get("result") == "dropped"]
    assert dropped
    assert all(e["reason"] == "partition" for e in dropped)

    # Minority leader n1 eventually sees term 2 and steps down.
    assert (result["nodes"]["n1"]["role"], result["nodes"]["n1"]["term"]) == ("follower", 2)
    assert result["nodes"]["n2"]["role"] == "leader"
    terms = {entry["term"]: entry["leaders"] for entry in result["electionSafety"]["terms"]}
    assert terms == {1: ["n1"], 2: ["n2"]}
    assert result["electionSafety"]["violations"] == []


def test_messages_after_duration_are_not_delivered():
    # Election at t=10, votes land at t=12 which equals duration: processed.
    result = run(scenario(duration=12))
    times = {e["time"] for e in result["timeline"]}
    assert max(times) == 12

    result_short = run(scenario(duration=11))
    assert max(e["time"] for e in result_short["timeline"]) == 10


def test_seq_is_contiguous_and_starts_at_one():
    result = run(scenario())
    seqs = [e["seq"] for e in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def test_deterministic_byte_identical_output():
    raw = scenario(
        faults=[{"time": 50, "action": "partition", "groups": [["n1"], ["n2", "n3"]]}]
    )
    first = json.dumps(run(raw), sort_keys=True, ensure_ascii=False)
    second = json.dumps(run(json.loads(json.dumps(raw))), sort_keys=True, ensure_ascii=False)
    assert first == second


def test_equal_time_fault_order_preserved():
    raw = scenario(
        duration=80,
        faults=[
            {"time": 30, "action": "partition", "groups": [["n1"], ["n2", "n3"]]},
            {"time": 30, "action": "heal"},
        ],
    )
    result = run(raw)
    fault_events = [(e["seq"], e["action"]) for e in result["timeline"] if e["type"] == "fault"]
    assert [action for _, action in fault_events] == ["partition", "heal"]


def test_timeline_record_shape():
    result = run(scenario(duration=12))
    for event in result["timeline"]:
        for key in ("seq", "time", "node", "term", "peer"):
            assert key in event
