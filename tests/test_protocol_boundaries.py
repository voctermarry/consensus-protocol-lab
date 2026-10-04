"""Regression tests for the protocol-kernel responsibility split.

The protocol event-processing kernel is decomposed into per-domain mixins
(elections+pre-vote, replication+snapshots, membership, reads, lifecycle,
liveness) composed over the single simulator state. These tests pin the
observable behavior of the collaboration boundaries between those domains —
through the public ``simulate`` / ``explore`` / ``replay`` entry points
only — so the observable results are proven identical to the pre-split
implementation:

- a higher-term step-down cancels the pre-vote round and the pending reads
  at the same instant (elections x reads);
- a committed configuration immediately governs election eligibility,
  joint majorities, replication commit and read confirmation
  (membership x elections x replication x reads);
- an installed snapshot restores the membership configuration and
  continues the global log indices (replication x membership);
- crash/restart keeps persisted state, clears volatile state and rebuilds
  the election timer from the restart moment (lifecycle);
- every state change, message, commit and liveness result lands on the one
  timeline with unchanged per-time ordering and global seq, so simulate is
  byte-identical across runs and replay matches.
"""

from __future__ import annotations

import json

from consensus_lab.cli import main
from consensus_lab.explore import run_explore
from consensus_lab.replay import run_replay
from consensus_lab.simulate import run_simulation


def _write(path_dir, name, value):
    path = path_dir / name
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


def _base(**overrides):
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 600,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 10,
    }
    scenario.update(overrides)
    return scenario


def _base4(**overrides):
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 1500,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 10,
        "initialMembers": ["a", "b", "c"],
    }
    scenario.update(overrides)
    return scenario


def _by_type(result, event_type):
    return [e for e in result["timeline"] if e["type"] == event_type]


# -- elections x reads: one step-down cancels the round and the reads -------


def test_stepdown_cancels_pending_reads_at_the_same_instant():
    # a leads term 1 and accepts read r1 while isolated by the partition;
    # when the heal lets b's term-2 appendEntries through, the very
    # transition that deposes a also fails the read it was vouching for.
    result = run_simulation(_base(
        preVote=True,
        duration=2000,
        clientCommands=[{"time": 200, "node": "a", "id": "w1", "command": "v"}],
        readQueries=[{"time": 350, "node": "a", "id": "r1"}],
        faults=[
            {"time": 300, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 900, "action": "heal"},
        ],
    ))

    timeline = result["timeline"]
    stepdown = next(
        e for e in timeline
        if e["type"] == "stateChange" and e["node"] == "a" and e["role"] == "follower"
        and e["time"] == 900
    )
    assert stepdown["term"] == 2
    # The read failure is the next timeline entry at the same timestamp:
    # one transition, both cancellations, one timeline.
    followup = timeline[stepdown["seq"]]  # seq is 1-based
    assert followup == {
        "seq": stepdown["seq"] + 1,
        "time": 900,
        "type": "readResult",
        "node": "a",
        "id": "r1",
        "result": "rejected",
        "reason": "leadershipLost",
        "knownLeader": None,
    }
    assert result["reads"] == [
        {"id": "r1", "node": "a", "outcome": "rejected",
         "reason": "leadershipLost", "knownLeader": None}
    ]


def test_leader_message_cancels_the_active_pre_vote_round():
    # c is isolated and keeps opening pre-vote rounds (its term stays 0:
    # pre-vote never raises it); the healed leader's heartbeat ends the
    # round without c ever campaigning in a real term.
    result = run_simulation(_base(
        preVote=True,
        duration=1200,
        faults=[
            {"time": 50, "action": "partition", "groups": [["c"], ["a", "b"]]},
            {"time": 700, "action": "heal"},
        ],
    ))

    c_states = [
        (e["time"], e["role"], e["reason"], e["term"])
        for e in _by_type(result, "stateChange")
        if e["node"] == "c"
    ]
    assert c_states == [
        (200, "preCandidate", "electionTimeout", 0),
        (400, "preCandidate", "electionTimeout", 0),
        (600, "preCandidate", "electionTimeout", 0),
        (700, "follower", "heartbeat", 1),
    ]
    # No pre-vote probe leaves c once the round is cancelled.
    assert not [
        e for e in _by_type(result, "messageSend")
        if e.get("message") == "preVote" and e["node"] == "c" and e["time"] > 700
    ]
    assert result["nodes"]["c"]["role"] == "follower"
    assert result["nodes"]["c"]["term"] == 1


# -- membership x reads: a committed config widens the read quorum ---------


def test_read_confirmation_follows_the_widened_quorum():
    # After the add of d commits, the leader's newest configuration covers
    # four voters: a read needs three acknowledgements (self + two peers),
    # not the two that sufficed for the original three-node cluster.
    result = run_simulation(_base4(
        clientCommands=[{"time": 200, "node": "a", "id": "w1", "command": "v"}],
        membershipChanges=[
            {"time": 300, "node": "a", "id": "m1", "action": "add", "member": "d"},
        ],
        readQueries=[
            {"time": 320, "node": "a", "id": "r1"},
            {"time": 900, "node": "a", "id": "r2"},
        ],
    ))

    timeline = result["timeline"]
    completion = next(
        e for e in timeline
        if e["type"] == "readResult" and e.get("id") == "r1" and e["result"] == "completed"
    )
    acknowledged = [
        e for e in timeline
        if e["type"] == "messageResult" and e.get("message") == "readReply"
        and e.get("id") == "r1" and e.get("detail") == "acknowledged"
        and e["seq"] < completion["seq"]
    ]
    # Two peer acks (self + two = 3 of 4) were required; a single peer ack
    # — a majority of the old three-node configuration — did not complete it.
    assert len(acknowledged) == 2
    assert {e["peer"] for e in acknowledged} == {"b", "c"}
    assert result["reads"][0]["outcome"] == "completed"
    assert result["membership"]["changes"] == [
        {"id": "m1", "node": "a", "action": "add", "member": "d",
         "outcome": "committed", "index": 3, "term": 1}
    ]
    assert result["membership"]["current"] == ["a", "b", "c", "d"]


def test_read_confirmation_under_the_original_stable_quorum():
    # Control: with the stable three-voter configuration, one peer ack
    # (self + one = 2 of 3) completes the read.
    result = run_simulation(_base(
        clientCommands=[{"time": 200, "node": "a", "id": "w1", "command": "v"}],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
    ))
    timeline = result["timeline"]
    completion = next(
        e for e in timeline
        if e["type"] == "readResult" and e.get("id") == "r1" and e["result"] == "completed"
    )
    acknowledged = [
        e for e in timeline
        if e["type"] == "messageResult" and e.get("message") == "readReply"
        and e.get("id") == "r1" and e.get("detail") == "acknowledged"
        and e["seq"] < completion["seq"]
    ]
    assert len(acknowledged) == 1
    assert completion["time"] == 320


# -- replication x membership: snapshots carry membership and indices ------


def test_snapshot_install_restores_membership_and_continues_global_indices():
    # a's removal of d is interrupted by a's crash; b finishes the joint
    # change (joint at index 7, stable at index 8), snapshots fold the
    # config entries, and c — down through it all — restores everything
    # from one installed snapshot: membership, change outcome and the
    # global log positions, with no renumbering.
    result = run_simulation(_base4(
        duration=4000,
        snapshotThreshold=2,
        initialMembers=["a", "b", "c", "d"],
        clientCommands=[
            {"time": 200 + 40 * i, "node": "a", "id": f"w{i}", "command": i}
            for i in range(6)
        ],
        membershipChanges=[
            {"time": 500, "node": "a", "id": "m1", "action": "remove", "member": "d"},
        ],
        nodeEvents=[
            {"time": 520, "node": "a", "action": "crash"},
            {"time": 1500, "node": "a", "action": "restart"},
            {"time": 300, "node": "c", "action": "crash"},
            {"time": 2500, "node": "c", "action": "restart"},
        ],
    ))

    installed = _by_type(result, "snapshotInstalled")
    assert [(e["node"], e["lastIncludedIndex"]) for e in installed] == [("c", 8)]

    # The interrupted change was resumed by the next leader: the joint
    # entry committed at index 7, its stable successor at index 8.
    config_applied = [
        (e["index"], e["entryType"]) for e in _by_type(result, "configurationApplied")
    ]
    assert (7, "joint") in config_applied
    assert (8, "stable") in config_applied
    assert result["membership"]["changes"] == [
        {"id": "m1", "node": "a", "action": "remove", "member": "d",
         "outcome": "committed", "index": 8, "term": 8}
    ]
    assert result["membership"]["current"] == ["a", "b", "c"]

    # c's state after the install: membership restored (voter in the
    # three-member stable config), applied history contiguous from 1, and
    # the log continues at global index lastIncludedIndex + 1 (here the log
    # is fully compacted, so the next entry would be index 9).
    c = result["nodes"]["c"]
    assert c["snapshot"] == {"lastIncludedIndex": 8, "lastIncludedTerm": 8}
    assert c["membershipRole"] == "voter"
    assert [entry["index"] for entry in c["applied"]] == list(range(1, 9))
    assert c["commitIndex"] == 8 and c["lastApplied"] == 8
    assert all(entry["index"] > 8 for entry in c["log"])


# -- lifecycle: crash/restart keeps persisted, clears volatile -------------


def test_crash_restart_preserves_persistent_state_and_rebuilds_timer():
    # a crashes as leader and restarts isolated: its persisted term and log
    # survive (the next campaign continues at term 2 with w1 still in the
    # log), no timer fires while it is down, and the rebuilt election timer
    # fires exactly at restart + electionTimeout and every timeout after.
    result = run_simulation(_base(
        duration=1200,
        clientCommands=[{"time": 200, "node": "a", "id": "w1", "command": "one"}],
        nodeEvents=[
            {"time": 250, "node": "a", "action": "crash"},
            {"time": 600, "node": "a", "action": "restart"},
        ],
        faults=[{"time": 550, "action": "partition", "groups": [["a"], ["b", "c"]]}],
    ))

    a_timeouts = [
        (e["time"], e["term"]) for e in _by_type(result, "timeout") if e["node"] == "a"
    ]
    # The boot timer at 100, then silence while down (250..600), then the
    # rebuilt timer from the restart moment: 700, 800, ... (600 + 100k).
    assert a_timeouts == [(100, 0)] + [(700 + 100 * k, 1 + k) for k in range(6)]

    a_states = [
        (e["time"], e["role"], e["reason"], e["term"])
        for e in _by_type(result, "stateChange")
        if e["node"] == "a"
    ]
    # Volatile state was cleared at restart: a campaigns again as a
    # candidate (a still-leader node would ignore the timeout), continuing
    # from the persisted term 1.
    assert a_states[0] == (100, "candidate", "electionTimeout", 1)
    assert a_states[1] == (120, "leader", "majority", 1)
    assert a_states[2:] == [
        (700 + 100 * k, "candidate", "electionTimeout", 2 + k) for k in range(6)
    ]

    a = result["nodes"]["a"]
    assert a["online"] is True and a["restartCount"] == 1
    assert a["log"] == [{"index": 1, "term": 1, "id": "w1", "command": "one"}]
    assert a["votedFor"] == "a"


def test_crash_abandons_pending_reads():
    # The read's confirmations are dropped, so r1 is still pending when
    # the leader crashes: the crash fails it at once.
    result = run_simulation(_base(
        clientCommands=[{"time": 200, "node": "a", "id": "w1", "command": "v"}],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
        messageFaults=[
            {"from": "b", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "drop"},
            {"from": "c", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "drop"},
        ],
        nodeEvents=[{"time": 350, "node": "a", "action": "crash"}],
    ))

    timeline = result["timeline"]
    crash = next(
        e for e in timeline
        if e["type"] == "nodeLifecycle" and e["action"] == "crash"
    )
    followup = timeline[crash["seq"]]
    assert followup == {
        "seq": crash["seq"] + 1,
        "time": 350,
        "type": "readResult",
        "node": "a",
        "id": "r1",
        "result": "rejected",
        "reason": "leadershipLost",
        "knownLeader": None,
    }
    assert result["reads"] == [
        {"id": "r1", "node": "a", "outcome": "rejected",
         "reason": "leadershipLost", "knownLeader": None}
    ]


# -- liveness: results across domains on the shared timeline ----------------


def test_liveness_results_track_the_drained_state():
    result = run_simulation(_base(
        duration=1200,
        clientCommands=[{"time": 200, "node": "a", "id": "w1", "command": 1}],
        nodeEvents=[{"time": 300, "node": "a", "action": "crash"}],
        livenessChecks=[
            {"id": "k1", "type": "leaderElected", "startTime": 0, "deadline": 250},
            {"id": "k2", "type": "clientCommitted", "startTime": 200,
             "deadline": 400, "target": "w1"},
            {"id": "k3", "type": "leaderElected", "startTime": 500, "deadline": 1100},
        ],
    ))
    assert result["liveness"] == {
        "checks": [
            {"id": "k1", "checkType": "leaderElected",
             "status": "satisfied", "time": 120},
            {"id": "k2", "checkType": "clientCommitted", "target": "w1",
             "status": "satisfied", "time": 220},
            {"id": "k3", "checkType": "leaderElected",
             "status": "satisfied", "time": 500},
        ],
        "violations": [],
    }


# -- one timeline: simulate/explore/replay agree byte for byte ---------------


def _kitchen_sink():
    return _base4(
        duration=4000,
        preVote=True,
        snapshotThreshold=3,
        clientCommands=[
            {"time": 300 + 50 * i, "node": "a", "id": f"w{i}", "command": i}
            for i in range(8)
        ],
        membershipChanges=[
            {"time": 800, "node": "a", "id": "m1", "action": "add", "member": "d"},
        ],
        readQueries=[
            {"time": 1500, "node": "a", "id": "r1"},
            {"time": 2500, "node": "a", "id": "r2"},
        ],
        faults=[
            {"time": 1000, "action": "partition", "groups": [["c"], ["a", "b", "d"]]},
            {"time": 1800, "action": "heal"},
        ],
        nodeEvents=[
            {"time": 2000, "node": "b", "action": "crash"},
            {"time": 2600, "node": "b", "action": "restart"},
        ],
        messageFaults=[
            {"from": "a", "to": "d", "message": "appendEntries", "occurrence": 1,
             "action": "delay", "delay": 120},
        ],
        livenessChecks=[
            {"id": "k1", "type": "membershipCommitted", "startTime": 800,
             "deadline": 3000, "target": "m1"},
            {"id": "k2", "type": "readCompleted", "startTime": 1500,
             "deadline": 2000, "target": "r1"},
            {"id": "k3", "type": "clientCommitted", "startTime": 300,
             "deadline": 3500, "target": "w7"},
        ],
    )


def test_all_domains_share_one_deterministic_timeline(tmp_path, capsys):
    scenario = _kitchen_sink()
    path = _write(tmp_path, "scenario.json", scenario)

    # Two runs of the public simulate entry point are byte-identical.
    assert main(["simulate", path]) == 0
    out_first, err = capsys.readouterr()
    assert err == ""
    assert main(["simulate", path]) == 0
    out_second, err = capsys.readouterr()
    assert err == ""
    assert out_first == out_second

    result = json.loads(out_first)
    # Global seq is a dense 1-based numbering of the single timeline.
    assert [e["seq"] for e in result["timeline"]] == list(
        range(1, len(result["timeline"]) + 1)
    )
    # Every domain left its marks on the one timeline.
    observed = {e["type"] for e in result["timeline"]}
    assert {
        "stateChange", "messageSend", "messageResult", "applied",
        "commitAdvance", "configurationApplied", "membershipResult",
        "readResult", "snapshotCreated", "nodeLifecycle", "livenessResult",
        "timeout", "fault", "messageFault",
    } <= observed

    # The cross-domain results are exactly the pre-split observable ones.
    assert result["liveness"]["checks"] == [
        {"id": "k1", "checkType": "membershipCommitted", "target": "m1",
         "status": "satisfied", "time": 840},
        {"id": "k2", "checkType": "readCompleted", "target": "r1",
         "status": "satisfied", "time": 1520},
        {"id": "k3", "checkType": "clientCommitted", "target": "w7",
         "status": "satisfied", "time": 670},
    ]
    assert result["membership"]["changes"] == [
        {"id": "m1", "node": "a", "action": "add", "member": "d",
         "outcome": "committed", "index": 10, "term": 1}
    ]
    assert [read["outcome"] for read in result["reads"]] == ["completed", "completed"]
    assert [entry["id"] for entry in result["reads"][0]["state"]] == [
        f"w{i}" for i in range(8)
    ]

    # Replay of the saved output matches, with the same exit code contract.
    result_path = _write(tmp_path, "result.json", result)
    assert main(["replay", path, result_path]) == 0
    out, err = capsys.readouterr()
    assert err == ""
    assert json.loads(out) == {"status": "matched"}


def test_explore_combination_order_and_minimal_references():
    # The same plan enumerates combinations in the established order and
    # every failing case minimizes to the same single-candidate counter
    # example — the explore surface is unchanged by the kernel split.
    plan = {
        "scenario": _base(
            duration=350,
            preVote=True,
            clientCommands=[{"time": 140, "node": "a", "id": "w1",
                             "command": "set x=1"}],
            nodeEvents=[{"time": 145, "node": "c", "action": "crash"}],
            livenessChecks=[
                {"id": "k1", "type": "clientCommitted", "startTime": 140,
                 "deadline": 175, "target": "w1"},
            ],
        ),
        "candidates": [
            {"from": "a", "to": "b", "message": "appendEntries",
             "occurrence": i, "action": "drop"}
            for i in range(1, 4)
        ],
        "maxFaults": 3,
        "maxCases": 100,
        "minimizeFailures": True,
    }
    first = run_explore(plan)
    second = run_explore(plan)
    assert json.dumps(first, ensure_ascii=False) == json.dumps(second, ensure_ascii=False)

    assert (first["totalCases"], first["passedCases"], first["failedCases"]) == (8, 4, 4)
    expected_selected = [[], [0], [1], [2], [0, 1], [0, 2], [1, 2], [0, 1, 2]]
    assert [case["selected"] for case in first["cases"]] == expected_selected
    assert [case["caseId"] for case in first["cases"]] == list(range(8))
    failed = [case for case in first["cases"] if case["status"] == "failed"]
    assert [case["selected"] for case in failed] == [[0], [0, 1], [0, 2], [0, 1, 2]]
    for case in failed:
        assert case["failureReports"] == ["liveness"]
        assert case["minimalSelected"] == [0]
        assert case["minimalCaseId"] == 1
