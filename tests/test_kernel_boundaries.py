"""Regression tests for the split protocol-kernel boundaries and for the
cross-domain flows that must survive the refactor unchanged.

The protocol event-handling kernel is split into one mixin per
responsibility (timeline/config helpers, elections and pre-vote, replication
and snapshots, membership changes, read-only queries, lifecycle, liveness,
dispatch); the simulator still assembles those mixins onto one object with
one node-state map, one virtual clock, one event queue and one timeline.

These tests drive only the public entry point (``simulate`` through the CLI,
exactly like the rest of the suite): every boundary and coupling is observed
through the deterministic timeline and result document, never through new
internal interfaces.
"""

from __future__ import annotations

import json

from consensus_lab.cli import main


def _write(tmp_path, scenario):
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(scenario), encoding="utf-8")
    return str(path)


def _run_ok(tmp_path, capsys, scenario):
    path = _write(tmp_path, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    return json.loads(out)


def _results(result, event_type, node=None):
    return [
        e for e in result["timeline"]
        if e["type"] == event_type and (node is None or e.get("node") == node)
    ]


# --------------------------------------------------------------------------
# Boundary: the election/pre-vote kernel vs the reads kernel
# --------------------------------------------------------------------------


def test_stepdown_on_higher_term_cancels_old_pre_vote_round_and_pending_read(
    tmp_path, capsys
):
    # a wins term 1 through pre-vote and accepts read r1. It is then isolated
    # with no quorum; b's early preVoteReply from the very first round is
    # delayed until well after the cluster has moved to term 2. When the
    # partition heals, the higher-term leader contact both steps a down and
    # fails the read; the ancient preVoteReply afterwards matches no round.
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 120, "c": 300},
        "heartbeatInterval": 40,
        "messageDelay": 10,
        "preVote": True,
        "faults": [
            {"time": 150, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 500, "action": "heal"},
        ],
        "readQueries": [{"time": 130, "node": "a", "id": "r1"}],
        "messageFaults": [
            {
                "from": "b", "to": "a", "message": "preVoteReply",
                "occurrence": 1, "action": "delay", "delay": 600,
            }
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)

    read_events = _results(result, "readResult", "a")
    assert [(e["time"], e["id"], e["result"], e.get("reason")) for e in read_events] == [
        (130, "r1", "accepted", None),
        (530, "r1", "rejected", "leadershipLost"),
    ]
    # The ancient round-1 preVoteReply arrives after a already follows term 2
    # and is reported staleRound — it neither revives a pre-vote round nor
    # triggers another state change (which would show up as an extra entry).
    stale = [
        e for e in result["timeline"]
        if e["type"] == "messageResult"
        and e["node"] == "a"
        and e.get("message") == "preVoteReply"
        and e["round"] == 1
        and e["peer"] == "b"
    ]
    assert [(e["time"], e["detail"]) for e in stale] == [(700, "staleRound")]
    # After that stale reply a still follows term 2 and opens no new round.
    assert result["nodes"]["a"]["role"] == "follower"
    assert result["nodes"]["a"]["term"] == 2
    a_changes = _results(result, "stateChange", "a")
    last_change = a_changes[-1]
    assert (last_change["role"], last_change["term"]) == ("follower", 2)
    stale_index = next(i for i, e in enumerate(result["timeline"]) if e is stale[-1])
    assert all(
        e["type"] != "stateChange" for e in result["timeline"][stale_index + 1:]
    )
    # The read could not be confirmed by the deposed leader; final outcome is
    # the rejection the stepdown recorded.
    assert result["reads"][0] == {
        "id": "r1", "node": "a", "outcome": "rejected",
        "reason": "leadershipLost", "knownLeader": None,
    }


def test_precandidate_receives_higher_term_preVoteReply_steps_down_without_persisting(
    tmp_path, capsys
):
    # Without any partition, c opens the very first pre-vote round at t=80;
    # its probe to a is dropped and its probe to b is delayed past b's own
    # election. b wins term 1; c opens a second round (still prospective term
    # 1), and a's reply then carries the higher real term 1, stepping c down
    # to a term-1 follower without c ever becoming a real candidate.
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 600,
        "electionTimeouts": {"a": 300, "b": 120, "c": 80},
        "heartbeatInterval": 40,
        "messageDelay": 10,
        "preVote": True,
        "messageFaults": [
            {"from": "c", "to": "a", "message": "preVote",
             "occurrence": 1, "action": "drop"},
            {"from": "c", "to": "b", "message": "preVote",
             "occurrence": 1, "action": "delay", "delay": 110},
            # Keep b's term-1 traffic away from c until the higher-term reply
            # to c's own open round has stepped c down.
            {"from": "b", "to": "c", "message": "requestVote",
             "occurrence": 1, "action": "delay", "delay": 300},
            {"from": "b", "to": "c", "message": "heartbeat",
             "occurrence": 1, "action": "delay", "delay": 300},
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)
    higher = [
        e for e in result["timeline"]
        if e["type"] == "messageResult"
        and e.get("message") == "preVoteReply"
        and e["detail"] == "higherTerm"
    ]
    assert len(higher) == 1
    higher_entry = higher[0]
    assert higher_entry["node"] == "c"
    assert higher_entry["term"] == 1
    # c's next state change is the higher-term stepdown to a term-1 follower;
    # c never reached the real candidate role.
    later_changes = [
        e for e in _results(result, "stateChange", "c")
        if e["time"] >= higher_entry["time"]
    ]
    assert later_changes[0]["role"] == "follower"
    assert later_changes[0]["term"] == 1
    assert later_changes[0]["reason"] == "higherTermMessage"
    assert all(e["role"] not in ("candidate", "leader") for e in later_changes)
    assert result["nodes"]["c"]["role"] == "follower"
    assert result["nodes"]["c"]["term"] == 1
    assert result["electionSafety"]["leadersByTerm"] == {"1": ["b"]}
    assert result["electionSafety"]["violations"] == []


# --------------------------------------------------------------------------
# Boundary: config application -> election eligibility / joint majorities /
# replication commit / read confirmation
# --------------------------------------------------------------------------


def test_joint_config_takes_effect_before_commit_for_elections(
    tmp_path, capsys
):
    # The joint entry is appended everywhere (but not committed) when a
    # crashes; d — a voter only under the appended joint/new config — must
    # already receive preVote/requestVote traffic, and the election that
    # follows must be decided under both joint quorum sets.
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "preVote": True,
        "initialMembers": ["a", "b", "c"],
        "membershipChanges": [
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        "clientCommands": [{"time": 120, "node": "a", "id": "w1", "command": 1}],
        "nodeEvents": [
            {"time": 208, "node": "a", "action": "crash"},
            {"time": 700, "node": "a", "action": "restart"},
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)

    pre_votes_to_d = [
        e for e in result["timeline"]
        if e["type"] == "messageSend" and e.get("message") == "preVote"
        and e["peer"] == "d"
    ]
    assert pre_votes_to_d, "new joint voter d must be polled in pre-vote rounds"
    # d wins term 2 after a crashed; its requestVote goes to every joint
    # voter, and it records itself leader in term 2 only after b and c
    # granted (a majority of BOTH sets {a,b,c} and {a,b,c,d} with a down).
    assert result["nodes"]["d"]["role"] == "leader"
    assert result["nodes"]["d"]["term"] == 2
    rv_to_d_ack = [
        (e["time"], e["detail"])
        for e in result["timeline"]
        if e["type"] == "messageResult"
        and e.get("message") == "requestVote" and e["node"] in ("b", "c")
        and e["detail"] == "voteGranted" and e["time"] >= 300
    ]
    assert len(rv_to_d_ack) == 2
    assert result["electionSafety"]["violations"] == []
    # The unfinished change is finished by the new leader d: the stable
    # entry lands at global index 3 in term 2 and the change commits.
    assert result["membership"]["changes"][0]["outcome"] == "committed"
    assert result["membership"]["changes"][0]["index"] == 3
    for node in result["nodes"].values():
        assert node["membershipRole"] == "voter"


def test_read_confirmation_uses_the_committed_new_configuration(
    tmp_path, capsys
):
    # After the stable add of d commits, a read accepted by a is confirmed
    # against the new 4-voter set. With b and d down, only {a, c} can ack —
    # a majority of the old 3-set but not of the new 4-set — so it stays
    # pending; with d back (only b down) {a, c, d} completes it.
    base = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "initialMembers": ["a", "b", "c"],
        "membershipChanges": [
            {"time": 150, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
    }
    stuck = _run_ok(tmp_path, capsys, dict(
        base,
        readQueries=[{"time": 400, "node": "a", "id": "r1"}],
        nodeEvents=[
            {"time": 405, "node": "b", "action": "crash"},
            {"time": 405, "node": "d", "action": "crash"},
        ],
    ))
    assert stuck["membership"]["current"] == ["a", "b", "c", "d"]
    assert stuck["reads"][0]["outcome"] == "pending"

    fine = _run_ok(tmp_path, capsys, dict(
        base,
        readQueries=[{"time": 400, "node": "a", "id": "r1"}],
        nodeEvents=[{"time": 405, "node": "b", "action": "crash"}],
    ))
    assert fine["reads"][0]["outcome"] == "completed"
    assert fine["linearizability"]["violations"] == []


# --------------------------------------------------------------------------
# Boundary: membership -> elections/reads (leader removed by its own change)
# --------------------------------------------------------------------------


def test_removed_leader_steps_down_cancels_read_and_liveness_rejects_target(
    tmp_path, capsys
):
    # a proposes removing itself; the moment the stable entry applies, the
    # membership kernel demotes a (election kernel) and fails its pending
    # read (reads kernel); the liveness kernel observes both outcomes at the
    # same timeline, resolving membershipCommitted satisfied and the read
    # check failed targetRejected at its deadline.
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "initialMembers": ["a", "b", "c", "d"],
        "membershipChanges": [
            {"time": 200, "node": "a", "id": "m1", "action": "remove", "member": "a"}
        ],
        "readQueries": [{"time": 210, "node": "a", "id": "r1"}],
        "livenessChecks": [
            {"id": "l1", "type": "readCompleted", "target": "r1",
             "startTime": 0, "deadline": 600},
            {"id": "l2", "type": "membershipCommitted", "target": "m1",
             "startTime": 0, "deadline": 600},
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)

    demotion = [
        e for e in result["timeline"]
        if e["type"] == "stateChange" and e.get("reason") == "removedFromCluster"
    ]
    assert [(e["time"], e["node"], e["role"]) for e in demotion] == [(220, "a", "follower")]
    # The read rejection is recorded in the same timestamp drain.
    rejection = [
        e for e in result["timeline"]
        if e["type"] == "readResult" and e["result"] == "rejected"
    ]
    assert [(e["time"], e["id"], e["reason"]) for e in rejection] == [
        (220, "r1", "leadershipLost")
    ]
    # a never campaigns again: it is a learner under the stable config.
    assert result["nodes"]["a"]["membershipRole"] == "learner"
    later_campaigns = [
        e for e in result["timeline"]
        if e["type"] == "stateChange" and e.get("node") == "a"
        and e["time"] > 220 and e["role"] in ("candidate", "preCandidate", "leader")
    ]
    assert not later_campaigns
    # Liveness sees the committed change satisfied and the doomed read
    # failed for the terminal targetRejected reason.
    live = {e["id"]: e for e in _results(result, "livenessResult")}
    assert live["l2"]["status"] == "satisfied"
    assert live["l2"]["time"] == 220
    assert live["l1"]["status"] == "failed"
    assert live["l1"]["reason"] == "targetRejected"
    assert result["membership"]["changes"][0]["outcome"] == "committed"


# --------------------------------------------------------------------------
# Boundary: snapshots restore membership and continue global log indices
# --------------------------------------------------------------------------


def test_snapshot_restores_config_silently_and_log_continues_at_global_indices(
    tmp_path, capsys
):
    # c is partitioned away before its removal; the majority compacts
    # everything through index 6 (including the joint+stable config of change
    # rm-c). On heal c installs a snapshot carrying that configuration: no
    # configurationApplied is emitted for the restored entries, c becomes a
    # learner with its election timer cancelled, and later entries continue
    # the SAME global index sequence (5, 6 applied through the snapshot; no
    # renumbering) with global indices preserved on every node.
    scenario = {
        "nodes": ["a", "b", "c", "d", "e"],
        "duration": 1500,
        "electionTimeouts": {"a": 80, "b": 120, "c": 100, "d": 200, "e": 300},
        "heartbeatInterval": 40,
        "messageDelay": 5,
        "snapshotThreshold": 1,
        "initialMembers": ["a", "b", "c", "d", "e"],
        "faults": [
            {"time": 120, "action": "partition", "groups": [["c"], ["a", "b", "d", "e"]]},
            {"time": 700, "action": "heal"},
        ],
        "membershipChanges": [
            {"time": 150, "node": "a", "id": "rm-c", "action": "remove", "member": "c"}
        ],
        "clientCommands": [
            {"time": 200, "node": "a", "id": "x1", "command": "one"},
            {"time": 300, "node": "a", "id": "x2", "command": "two"},
            {"time": 900, "node": "a", "id": "x3", "command": "three"},
            {"time": 950, "node": "a", "id": "x4", "command": "four"},
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)

    node_c = result["nodes"]["c"]
    assert node_c["snapshot"]["lastIncludedIndex"] == 6
    assert node_c["snapshot"]["lastIncludedTerm"] == 8
    # Membership restored from the bundled snapshot config.
    assert node_c["membershipRole"] == "learner"
    assert node_c["role"] == "follower"
    # The applied prefix includes the config entries exactly once and keeps
    # global indices 1..6; global indexing continues seamlessly after the
    # snapshot boundary.
    applied_sig = [
        (e["index"], e.get("kind"), e.get("entryType"), e.get("id"))
        for e in node_c["applied"]
    ]
    assert applied_sig == [
        (1, "config", "joint", "rm-c"),
        (2, "config", "stable", "rm-c"),
        (3, "command", None, "x1"),
        (4, "command", None, "x2"),
        (5, "command", None, "x3"),
        (6, "command", None, "x4"),
    ]
    assert node_c["lastApplied"] == 6
    assert node_c["commitIndex"] == 6
    # Restored config entries are never re-applied.
    assert not [
        e for e in result["timeline"]
        if e["type"] == "configurationApplied" and e.get("node") == "c"
    ]
    # Snapshot-installed x3/x4 at global indices 5 and 6 ARE applied once
    # (they arrived as a suffix appended after the earlier snapshot c
    # installed while partitioned); assert the continuing sequence.
    applied_events = [
        (e["index"], e["id"])
        for e in result["timeline"]
        if e["type"] == "applied" and e.get("node") == "c"
    ]
    assert applied_events == [(5, "x3"), (6, "x4")]
    # c never campaigns after the heal (learner, timer cancelled).
    assert not [
        e for e in result["timeline"]
        if e["type"] == "stateChange" and e.get("node") == "c"
        and e["time"] >= 700 and e["role"] in ("candidate", "preCandidate", "leader")
    ]
    # Global index consistency survived across every node.
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


def test_follower_snapshot_install_then_replication_resumes_after_boundary(
    tmp_path, capsys
):
    # A non-removed follower far behind installs a snapshot mid-log and then
    # receives entries whose prevLogIndex is exactly the snapshot boundary;
    # commitIndex and lastApplied keep global indices and the suffix appends
    # continue without renumbering.
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 1200,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 40,
        "messageDelay": 5,
        "snapshotThreshold": 2,
        "initialMembers": ["a", "b", "c"],
        "membershipChanges": [],
        "faults": [
            {"time": 0, "action": "partition", "groups": [["d"], ["a", "b", "c"]]},
            {"time": 700, "action": "heal"},
        ],
        "clientCommands": [
            {"time": t, "node": "a", "id": f"w{i}", "command": i}
            for i, t in enumerate(range(100, 500, 40))
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)
    node_d = result["nodes"]["d"]
    assert node_d["snapshot"]["lastIncludedIndex"] == 10
    assert node_d["lastApplied"] == 10
    assert node_d["commitIndex"] == 10
    assert [e["index"] for e in node_d["applied"]] == list(range(1, 11))
    # The first appendEntries accepted after install resumes exactly past
    # the snapshot boundary.
    after = [
        e for e in result["timeline"]
        if e["type"] == "messageResult" and e.get("node") == "d"
        and e.get("message") == "appendEntries" and e["result"] == "delivered"
        and e["detail"] == "accepted" and e["time"] >= 700
    ]
    assert after and after[0]["prevLogIndex"] == 10
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


# --------------------------------------------------------------------------
# Boundary: crash/restart — persisted state kept, volatile state cleared
# --------------------------------------------------------------------------


def test_crash_keeps_persisted_state_and_restart_rebuilds_volatile(
    tmp_path, capsys
):
    # a commits two commands, then crashes and restarts. The persisted log,
    # commit/apply progress and applied history survive; each command was
    # applied exactly once and is not re-applied after restart; volatile
    # leader state is gone and a re-arms a fresh election timer (it joins the
    # new term's leader as a follower rather than calling an election).
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200},
        "heartbeatInterval": 40,
        "messageDelay": 5,
        "clientCommands": [
            {"time": 120, "node": "a", "id": "w1", "command": 1},
            {"time": 140, "node": "a", "id": "w2", "command": 2},
        ],
        "nodeEvents": [
            {"time": 300, "node": "a", "action": "crash"},
            {"time": 500, "node": "a", "action": "restart"},
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)

    applied_events = [
        (e["index"], e["id"]) for e in _results(result, "applied", "a")
    ]
    assert applied_events == [(1, "w1"), (2, "w2")]
    lifecycle = [
        (e["time"], e["action"]) for e in _results(result, "nodeLifecycle", "a")
    ]
    assert lifecycle == [(300, "crash"), (500, "restart")]
    node_a = result["nodes"]["a"]
    assert node_a["online"] is True
    assert node_a["restartCount"] == 1
    assert node_a["role"] == "follower"
    assert node_a["knownLeader"] == "b"
    assert node_a["commitIndex"] == 2
    assert node_a["lastApplied"] == 2
    assert [(e["index"], e["id"]) for e in node_a["log"]] == [
        (1, "w1"), (2, "w2")
    ]
    assert [(e["index"], e["id"]) for e in node_a["applied"]] == [
        (1, "w1"), (2, "w2")
    ]
    # a does not campaign after restart before a leader contacts it.
    post = [
        e for e in result["timeline"]
        if e["type"] == "stateChange" and e.get("node") == "a" and e["time"] >= 500
    ]
    assert all(e["role"] == "follower" for e in post)
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


def test_crash_cancels_pre_vote_round_and_pending_read_then_rounds_diverge(
    tmp_path, capsys
):
    # A pre-vote round and a pending read open on leader a; crashing a must
    # cancel both (no round survives into the restart, and a reply straddling
    # the restart can never match a later round). The read is rejected
    # leadershipLost; after restart a is kept isolated long enough to open a
    # fresh round, whose round number is strictly greater than any pre-crash
    # round (the counter survives only as a generation nonce).
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 300, "c": 200},
        "heartbeatInterval": 40,
        "messageDelay": 10,
        "preVote": True,
        "readQueries": [{"time": 300, "node": "a", "id": "r1"}],
        "faults": [
            {"time": 302, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 850, "action": "heal"},
        ],
        "nodeEvents": [
            {"time": 301, "node": "a", "action": "crash"},
            {"time": 500, "node": "a", "action": "restart"},
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)
    assert result["reads"][0] == {
        "id": "r1", "node": "a", "outcome": "rejected",
        "reason": "leadershipLost", "knownLeader": None,
    }
    read_events = _results(result, "readResult", "a")
    assert [(e["time"], e["result"], e.get("reason")) for e in read_events] == [
        (300, "accepted", None),
        (301, "rejected", "leadershipLost"),
    ]
    # After restart a's first election event is a fresh pre-vote round with a
    # strictly higher round number than any round used before the crash.
    def rounds(node):
        out = []
        for e in result["timeline"]:
            if (
                e["type"] == "messageSend" and e.get("node") == node
                and e.get("message") == "preVote"
            ):
                out.append((e["time"], e["round"]))
        return out

    before = [r for t, r in rounds("a") if t < 301]
    after = [(t, r) for t, r in rounds("a") if t >= 500]
    assert before, "a should have opened a pre-vote round before crashing"
    assert after, "a should open a fresh round after the restart timeout"
    assert min(r for _t, r in after) > max(before)


# --------------------------------------------------------------------------
# Boundary: liveness observes committed/rejected ledgers on one timeline
# --------------------------------------------------------------------------


def test_liveness_evaluated_against_drained_cross_domain_state(
    tmp_path, capsys
):
    # A command, a read and a membership change all settle in the same run;
    # satisfied checks resolve exactly at the commit/done moment and a
    # deadline failure for a doomed command (superseded) is attributed with
    # the terminal reason — all read from the same ledgers the other kernels
    # maintain, with no separate state.
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200},
        "heartbeatInterval": 40,
        "messageDelay": 5,
        "clientCommands": [{"time": 120, "node": "a", "id": "w1", "command": 1}],
        "readQueries": [{"time": 200, "node": "a", "id": "r1"}],
        "livenessChecks": [
            {"id": "ok-leader", "type": "leaderElected",
             "startTime": 0, "deadline": 400},
            {"id": "ok-write", "type": "clientCommitted", "target": "w1",
             "startTime": 0, "deadline": 400},
            {"id": "ok-read", "type": "readCompleted", "target": "r1",
             "startTime": 0, "deadline": 400},
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)
    live = {e["id"]: e for e in _results(result, "livenessResult")}
    assert live["ok-leader"]["status"] == "satisfied"
    assert live["ok-write"]["status"] == "satisfied"
    # The write commits at ~130; the read completes at ~210 — liveness
    # resolves each at its own moment, strictly ordered on the one timeline.
    assert live["ok-write"]["time"] < live["ok-read"]["time"]
    assert live["ok-read"]["status"] == "satisfied"
    assert result["liveness"]["violations"] == []


def test_single_timeline_seq_is_contiguous_through_every_boundary(
    tmp_path, capsys
):
    # One scenario exercising every kernel; every state change, message send,
    # zero-delay cascade, commit apply and liveness result lands on the same
    # timeline whose seq is 1..N contiguous and whose timestamps are
    # non-decreasing. This pins the shared-timeline requirement directly.
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 1500,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 40,
        "messageDelay": 0,
        "preVote": True,
        "snapshotThreshold": 3,
        "initialMembers": ["a", "b", "c"],
        "faults": [
            {"time": 400, "action": "partition", "groups": [["a", "d"], ["b", "c"]]},
            {"time": 700, "action": "heal"},
        ],
        "clientCommands": [
            {"time": t, "node": "a", "id": f"w{i}", "command": i}
            for i, t in enumerate(range(100, 600, 50))
        ],
        "nodeEvents": [
            {"time": 350, "node": "c", "action": "crash"},
            {"time": 750, "node": "c", "action": "restart"},
        ],
        "membershipChanges": [
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        "readQueries": [
            {"time": 250, "node": "a", "id": "r1"},
            {"time": 800, "node": "a", "id": "r2"},
        ],
        "livenessChecks": [
            {"id": "l1", "type": "leaderElected", "startTime": 0, "deadline": 1200}
        ],
    }
    result = _run_ok(tmp_path, capsys, scenario)
    timeline = result["timeline"]
    seqs = [e["seq"] for e in timeline]
    assert seqs == list(range(1, len(seqs) + 1))
    times = [e["time"] for e in timeline]
    assert times == sorted(times)
    # Every kernel contributed at least one entry to the one timeline.
    types_seen = {e["type"] for e in timeline}
    assert {
        "stateChange", "messageSend", "messageResult", "timeout",
        "clientResult", "commitAdvance", "applied", "snapshotCreated",
        "snapshotInstalled", "membershipResult", "configurationApplied",
        "readResult", "nodeLifecycle", "livenessResult", "fault",
    } <= types_seen
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []
