"""Combined regression tests: membership configuration across snapshots and
crash recovery, exercised end-to-end through the public ``simulate`` /
``replay`` / ``explore`` / ``version`` entry points.

Each scenario starts from three initial voters plus one learner, commits
client commands, runs a joint-consensus membership change, lets nodes compact
the configuration entries into snapshots, crashes and restarts at least one
node after the configuration is inside its snapshot, and then uses a
partition (or downed nodes) to make the old and new configuration majorities
distinguishable.  The assertions are always made against the complete JSON
stdout of ``simulate``; nothing here touches the protocol implementation.
"""

from __future__ import annotations

import json

import pytest

from consensus_lab import __version__
from consensus_lab.cli import main


# -- scenarios ---------------------------------------------------------------


def _add_recovery_scenario():
    """add learner d; snapshots; b crashes with the config inside its
    snapshot and restarts; a mid-run partition separates the old majority
    ({b, c} would suffice under the old 3-voter config) from the new one
    (3 of 4 required)."""
    return {
        "nodes": ["a", "b", "c", "d"],
        "duration": 1800,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "snapshotThreshold": 2,
        "initialMembers": ["a", "b", "c"],
        "clientCommands": [
            {"time": 120, "node": "a", "id": "x1", "command": "one"},
            {"time": 140, "node": "a", "id": "x2", "command": "two"},
            {"time": 320, "node": "a", "id": "x3", "command": "three"},
            {"time": 340, "node": "a", "id": "x4", "command": "four"},
            {"time": 900, "node": "a", "id": "x5", "command": "five"},
            {"time": 1150, "node": "a", "id": "x6", "command": "six"},
            {"time": 1600, "node": "b", "id": "x7", "command": "seven"},
        ],
        "membershipChanges": [
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        "nodeEvents": [
            {"time": 420, "node": "b", "action": "crash"},
            {"time": 520, "node": "b", "action": "restart"},
            {"time": 1250, "node": "a", "action": "crash"},
        ],
        "faults": [
            {"time": 60, "action": "partition", "groups": [["d"], ["a", "b", "c"]]},
            {"time": 190, "action": "heal"},
            {"time": 700, "action": "partition", "groups": [["a", "d"], ["b", "c"]]},
            {"time": 1000, "action": "heal"},
        ],
    }


def _joint_reads_scenario():
    """Read queries while the joint configuration is in effect: r1 sees both
    majorities and completes; c and d then crash so the new-set majority is
    unreachable and r2 stays pending with the joint phase left open."""
    return {
        "nodes": ["a", "b", "c", "d"],
        "duration": 700,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "initialMembers": ["a", "b", "c"],
        "clientCommands": [
            {"time": 120, "node": "a", "id": "w1", "command": "one"},
            {"time": 140, "node": "a", "id": "w2", "command": "two"},
        ],
        "membershipChanges": [
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        "nodeEvents": [
            {"time": 211, "node": "c", "action": "crash"},
            {"time": 211, "node": "d", "action": "crash"},
        ],
        "readQueries": [
            {"time": 205, "node": "a", "id": "r1"},
            {"time": 212, "node": "a", "id": "r2"},
        ],
    }


def _remove_recovery_scenario():
    """remove voter d; snapshots; voter c crashes with the removal inside its
    snapshot and restarts; the next election must be won from the current
    3-voter configuration alone."""
    return {
        "nodes": ["a", "b", "c", "d"],
        "duration": 1200,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "snapshotThreshold": 2,
        "initialMembers": ["a", "b", "c", "d"],
        "clientCommands": [
            {"time": 120, "node": "a", "id": "y1", "command": "one"},
            {"time": 140, "node": "a", "id": "y2", "command": "two"},
            {"time": 300, "node": "a", "id": "y3", "command": "three"},
            {"time": 320, "node": "a", "id": "y4", "command": "four"},
            {"time": 900, "node": "b", "id": "y5", "command": "five"},
        ],
        "membershipChanges": [
            {"time": 200, "node": "a", "id": "rm-d", "action": "remove", "member": "d"}
        ],
        "nodeEvents": [
            {"time": 420, "node": "c", "action": "crash"},
            {"time": 520, "node": "c", "action": "restart"},
            {"time": 600, "node": "a", "action": "crash"},
        ],
    }


_ALL_SCENARIOS = [
    _add_recovery_scenario,
    _joint_reads_scenario,
    _remove_recovery_scenario,
]


# -- helpers -----------------------------------------------------------------


def _write(tmp_path, name, value):
    path = tmp_path / name
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


def _run(tmp_path, capsys, scenario, name="scenario.json"):
    path = _write(tmp_path, name, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _run_ok(tmp_path, capsys, scenario, name="scenario.json"):
    code, out, err = _run(tmp_path, capsys, scenario, name)
    assert code == 0
    assert err == ""
    return json.loads(out)


def _by_type(result, event_type):
    return [e for e in result["timeline"] if e["type"] == event_type]


def _assert_clean_reports(result):
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []
    if "linearizability" in result:
        assert result["linearizability"]["violations"] == []


def _assert_seq_is_dense(result):
    seqs = [e["seq"] for e in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def _vote_messages(result, kind):
    return [e for e in _by_type(result, "messageSend") if e["message"] == kind]


def _rename(value, mapping):
    """Recursively rename node names (dict keys and string values)."""
    if isinstance(value, dict):
        return {_rename(k, mapping): _rename(v, mapping) for k, v in value.items()}
    if isinstance(value, list):
        return [_rename(item, mapping) for item in value]
    if isinstance(value, str):
        return mapping.get(value, value)
    return value


# -- scenario 1: add + snapshots + crash/restart + partition ------------------


def test_add_flow_membership_summary_and_config_application(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _add_recovery_scenario())
    _assert_seq_is_dense(result)

    assert result["membership"] == {
        "initial": ["a", "b", "c"],
        "current": ["a", "b", "c", "d"],
        "joint": None,
        "changes": [
            {"id": "m1", "node": "a", "action": "add", "member": "d",
             "outcome": "committed", "index": 4, "term": 1}
        ],
    }
    # The add was accepted while the learner was still catching up.
    assert [
        (e["time"], e["id"], e["result"], e.get("phase"))
        for e in _by_type(result, "membershipResult")
    ] == [(200, "m1", "accepted", "catchingUp")]

    # Every node applies the joint entry (index 3) before the stable one
    # (index 4); the leader applies first, the followers one hop later.
    applied = [
        (e["time"], e["node"], e["entryType"], e["index"])
        for e in _by_type(result, "configurationApplied")
    ]
    assert applied == [
        (210, "a", "joint", 3),
        (215, "b", "joint", 3),
        (215, "c", "joint", 3),
        (215, "d", "joint", 3),
        (220, "a", "stable", 4),
        (245, "b", "stable", 4),
        (245, "c", "stable", 4),
        (245, "d", "stable", 4),
    ]
    for node in result["nodes"].values():
        assert node["membershipRole"] == "voter"
    _assert_clean_reports(result)


def test_learner_catches_up_through_installed_snapshot(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _add_recovery_scenario())

    # The leader compacted x1/x2 into its snapshot at t=150 while d was
    # partitioned away, so d's catch-up after the heal is an installSnapshot.
    leader_snapshots = [
        (e["time"], e["lastIncludedIndex"], e["lastIncludedTerm"])
        for e in _by_type(result, "snapshotCreated")
        if e["node"] == "a"
    ]
    assert leader_snapshots == [(150, 2, 1), (220, 4, 1), (350, 6, 1), (1160, 8, 4)]

    installed = _by_type(result, "snapshotInstalled")
    assert [(e["time"], e["node"], e["peer"], e["lastIncludedIndex"],
             e["lastIncludedTerm"]) for e in installed] == [(195, "d", "a", 2, 1)]

    # Ordering: heal (190) -> snapshot installed on d (195) -> membership
    # change accepted (200) -> joint configuration applied (210).
    seq_of = {}
    for event in result["timeline"]:
        if event["type"] == "fault" and event["action"] == "heal" and event["time"] == 190:
            seq_of["heal"] = event["seq"]
        if event["type"] == "snapshotInstalled":
            seq_of["installed"] = event["seq"]
        if event["type"] == "membershipResult":
            seq_of["accepted"] = event["seq"]
        if event["type"] == "configurationApplied" and event["node"] == "a":
            seq_of.setdefault("jointApplied", event["seq"])
    assert seq_of["heal"] < seq_of["installed"] < seq_of["accepted"] < seq_of["jointApplied"]
    _assert_clean_reports(result)


def test_restarted_node_recovers_stable_config_from_snapshot(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _add_recovery_scenario())

    # b's last snapshot before the crash already contains both configuration
    # entries (indices 3 and 4 are compacted away at t=395).
    b_snapshots = [
        (e["time"], e["lastIncludedIndex"])
        for e in _by_type(result, "snapshotCreated")
        if e["node"] == "b"
    ]
    assert b_snapshots == [(195, 2), (245, 4), (395, 6), (1195, 8)]

    lifecycle = [
        (e["time"], e["node"], e["action"])
        for e in _by_type(result, "nodeLifecycle")
    ]
    assert lifecycle == [(420, "b", "crash"), (520, "b", "restart"), (1250, "a", "crash")]
    # The snapshot at index 6 precedes the crash; the restart follows it.
    assert b_snapshots[2][0] < 420 < 520

    # After the restart, b applies only the entries that were not yet
    # compacted: nothing at or below the snapshot position is re-applied,
    # and no configuration entry is applied twice.
    b_applies = [
        (e["time"], e["index"])
        for e in _by_type(result, "applied")
        if e["node"] == "b"
    ]
    assert b_applies == [
        (145, 1), (195, 2), (345, 5), (395, 6), (1195, 7), (1195, 8), (1610, 9)
    ]
    assert [e for e in b_applies if e[0] > 520] == [(1195, 7), (1195, 8), (1610, 9)]
    b_config = [
        (e["time"], e["entryType"])
        for e in _by_type(result, "configurationApplied")
        if e["node"] == "b"
    ]
    assert b_config == [(215, "joint"), (245, "stable")]

    # b recovered as a voter of the new stable configuration and went on to
    # become leader itself.
    node_b = result["nodes"]["b"]
    assert node_b["membershipRole"] == "voter"
    assert node_b["online"] is True
    assert node_b["restartCount"] == 1
    assert node_b["snapshot"] == {"lastIncludedIndex": 8, "lastIncludedTerm": 4}
    assert node_b["role"] == "leader"
    assert node_b["term"] == 5
    # The recovered applied prefix holds both config entries exactly once.
    config_entries = [e for e in node_b["applied"] if e.get("kind") == "config"]
    assert [(e["index"], e["entryType"]) for e in config_entries] == [(3, "joint"), (4, "stable")]
    _assert_clean_reports(result)


def test_partition_distinguishes_old_and_new_majorities(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _add_recovery_scenario())

    # During the [a,d]/[b,c] partition the {b,c} side would hold a majority
    # under the *old* three-voter configuration; under the committed
    # four-voter configuration it does not, so b's candidacies in terms 2
    # and 3 never produce a leader.
    assert result["electionSafety"]["leadersByTerm"] == {
        "1": ["a"], "4": ["a"], "5": ["b"]
    }
    candidacies = [
        (e["time"], e["term"])
        for e in _by_type(result, "stateChange")
        if e["node"] == "b" and e["role"] == "candidate"
    ]
    assert (845, 2) in candidacies and (995, 3) in candidacies

    # x5 was accepted by the leader inside its partition at t=900 but no
    # commit advance happened while the majority was unreachable; the commit
    # index jumps 6 -> 8 only after the heal, once x6 (current term)
    # replicates to a majority of the current configuration.
    commits = [
        (e["time"], e["node"], e["commitIndex"])
        for e in _by_type(result, "commitAdvance")
    ]
    assert commits == [
        (130, "a", 1), (150, "a", 2), (210, "a", 3), (220, "a", 4),
        (330, "a", 5), (350, "a", 6), (1160, "a", 8), (1610, "b", 9),
    ]
    assert all(time < 700 or time > 1000 for time, _, _ in commits)

    committed = {c["id"]: c for c in result["clients"]["committed"]}
    assert committed["x5"]["index"] == 7
    assert committed["x5"]["term"] == 1
    assert [c["id"] for c in result["clients"]["committed"]] == [
        "x1", "x2", "x3", "x4", "x5", "x6", "x7"
    ]
    assert result["clients"]["pending"] == []
    assert result["clients"]["superseded"] == []
    assert result["clients"]["rejected"] == []
    _assert_clean_reports(result)


def test_elections_count_only_current_voters(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _add_recovery_scenario())

    sends = {}
    for e in _vote_messages(result, "requestVote"):
        sends.setdefault((e["node"], e["term"]), []).append(e["peer"])
    # Before the add, the learner is never asked for a vote.
    assert sends[("a", 1)] == ["b", "c"]
    # After the add commits, d is a full voter and is canvassed.
    assert sends[("b", 5)] == ["a", "c", "d"]

    replies = {}
    for e in _vote_messages(result, "voteReply"):
        replies.setdefault(e["term"], set()).add(e["node"])
    # d contributes no vote in term 1 (not yet joined) ...
    assert replies[1] == {"b", "c"}
    # ... but its vote is counted in term 5, when the restarted b wins the
    # election with exactly the current configuration's majority {b, c, d}.
    assert replies[5] == {"c", "d"}
    _assert_clean_reports(result)


# -- scenario 2: read queries during the joint phase --------------------------


def test_joint_phase_read_completes_with_command_prefix_state(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _joint_reads_scenario())

    reads = {read["id"]: read for read in result["reads"]}
    # r1 was accepted while the joint configuration was in effect and
    # completed once both the old and the new set confirmed; the state is
    # the full client-command prefix at readIndex and contains no
    # configuration entries.
    assert reads["r1"] == {
        "id": "r1",
        "node": "a",
        "outcome": "completed",
        "term": 1,
        "readIndex": 2,
        "state": [
            {"index": 1, "term": 1, "id": "w1", "command": "one"},
            {"index": 2, "term": 1, "id": "w2", "command": "two"},
        ],
    }
    # The learner's readReply is explicitly ignored for the majority count.
    learner_replies = [
        e for e in _by_type(result, "messageResult")
        if e.get("message") == "readReply" and e.get("peer") == "d"
    ]
    assert [(e["id"], e["detail"]) for e in learner_replies] == [("r1", "ignored")]

    # Ordering in the timeline: the read is accepted after the joint entry
    # is appended (the membershipResult) and completes after the joint
    # configuration is applied on the leader.
    read_events = [
        (e["seq"], e["time"], e["id"], e["result"])
        for e in _by_type(result, "readResult")
    ]
    joint_applied = next(
        e for e in _by_type(result, "configurationApplied")
        if e["node"] == "a" and e["entryType"] == "joint"
    )
    accepted_r1 = next(e for e in read_events if e[2] == "r1" and e[3] == "accepted")
    completed_r1 = next(e for e in read_events if e[2] == "r1" and e[3] == "completed")
    membership = _by_type(result, "membershipResult")
    assert len(membership) == 1 and membership[0]["result"] == "accepted"
    assert membership[0]["seq"] < accepted_r1[0] < joint_applied["seq"] < completed_r1[0]
    _assert_clean_reports(result)


def test_joint_phase_read_stays_pending_without_new_set_majority(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _joint_reads_scenario())

    reads = {read["id"]: read for read in result["reads"]}
    # c and d crashed after the joint entry committed but before the stable
    # one could: the old-set majority {a, b} is reachable, the new-set
    # majority (3 of 4) is not, so r2 never completes.
    assert reads["r2"] == {
        "id": "r2", "node": "a", "outcome": "pending", "term": 1, "readIndex": 3
    }
    drops = [
        e for e in _by_type(result, "messageResult")
        if e.get("message") == "readProbe" and e.get("id") == "r2"
        and e.get("result") == "dropped"
    ]
    assert {(e["node"], e["reason"]) for e in drops} == {("c", "nodeDown"), ("d", "nodeDown")}

    # The joint phase is still open when the simulation ends.
    membership = result["membership"]
    assert membership["current"] == ["a", "b", "c"]
    assert membership["joint"] == {
        "id": "m1", "old": ["a", "b", "c"], "new": ["a", "b", "c", "d"]
    }
    (change,) = membership["changes"]
    assert change["outcome"] == "pending"
    assert change["phase"] == "joint"
    assert change["joint"] == {"old": ["a", "b", "c"], "new": ["a", "b", "c", "d"]}

    # Only the joint entry was ever applied; no stable entry exists.
    applied = [
        (e["node"], e["entryType"]) for e in _by_type(result, "configurationApplied")
    ]
    assert applied == [("a", "joint"), ("b", "joint")]
    assert result["electionSafety"]["leadersByTerm"] == {"1": ["a"]}
    _assert_clean_reports(result)


# -- scenario 3: remove + snapshots + crash/restart ---------------------------


def test_remove_flow_recovers_from_snapshot_and_reelects(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _remove_recovery_scenario())
    _assert_seq_is_dense(result)

    assert result["membership"] == {
        "initial": ["a", "b", "c", "d"],
        "current": ["a", "b", "c"],
        "joint": None,
        "changes": [
            {"id": "rm-d", "node": "a", "action": "remove", "member": "d",
             "outcome": "committed", "index": 4, "term": 1}
        ],
    }

    # c crashed after its snapshot covered the removal configuration and
    # restarted from it: nothing at or below index 6 is applied again.
    c_applies = [
        (e["time"], e["index"])
        for e in _by_type(result, "applied")
        if e["node"] == "c"
    ]
    assert c_applies == [(145, 1), (195, 2), (325, 5), (345, 6), (960, 7)]
    c_config = [
        (e["time"], e["entryType"])
        for e in _by_type(result, "configurationApplied")
        if e["node"] == "c"
    ]
    assert c_config == [(215, "joint"), (245, "stable")]
    node_c = result["nodes"]["c"]
    assert node_c["membershipRole"] == "voter"
    assert node_c["snapshot"] == {"lastIncludedIndex": 6, "lastIncludedTerm": 1}
    assert node_c["online"] is True
    assert node_c["restartCount"] == 1

    # The removed node stays a learner, follows the new leader, and never
    # campaigns again.
    node_d = result["nodes"]["d"]
    assert node_d["membershipRole"] == "learner"
    assert node_d["role"] == "follower"
    stable_applied_on_d = next(
        e["time"] for e in _by_type(result, "configurationApplied")
        if e["node"] == "d" and e["entryType"] == "stable"
    )
    assert not [
        e for e in _by_type(result, "stateChange")
        if e["node"] == "d" and e["role"] == "candidate"
        and e["time"] >= stable_applied_on_d
    ]

    # The post-recovery election is won from the current three-voter
    # configuration: the removed d is neither asked nor counted.
    sends = {}
    for e in _vote_messages(result, "requestVote"):
        sends.setdefault((e["node"], e["term"]), []).append(e["peer"])
    assert sends[("a", 1)] == ["b", "c", "d"]  # d still a voter in term 1
    assert sends[("b", 2)] == ["a", "c"]       # d removed: not canvassed
    replies = {}
    for e in _vote_messages(result, "voteReply"):
        replies.setdefault(e["term"], set()).add(e["node"])
    assert replies[1] == {"b", "c", "d"}
    assert replies[2] == {"c"}
    assert result["electionSafety"]["leadersByTerm"] == {"1": ["a"], "2": ["b"]}

    # The new leader commits a fresh write with the current majority {b, c}.
    committed = {c["id"]: c for c in result["clients"]["committed"]}
    assert committed["y5"] == {"id": "y5", "node": "b", "index": 7, "term": 2}
    assert result["clients"]["pending"] == []
    _assert_clean_reports(result)


# -- cross-cutting: determinism, replay, renaming -----------------------------


@pytest.mark.parametrize("build", _ALL_SCENARIOS)
def test_repeated_simulate_is_byte_identical_and_replays(tmp_path, capsys, build):
    scenario = build()
    scenario_path = _write(tmp_path, "scenario.json", scenario)

    code1 = main(["simulate", scenario_path])
    out1, err1 = capsys.readouterr()
    code2 = main(["simulate", scenario_path])
    out2, err2 = capsys.readouterr()
    assert (code1, err1) == (0, "")
    assert (code2, err2) == (0, "")
    assert out1 == out2

    result_path = tmp_path / "result.json"
    result_path.write_text(out1, encoding="utf-8")
    code = main(["replay", scenario_path, str(result_path)])
    out, err = capsys.readouterr()
    assert code == 0
    assert err == ""
    assert out == '{"status":"matched"}\n'


@pytest.mark.parametrize("build", _ALL_SCENARIOS)
def test_renaming_nodes_preserves_events_and_outcomes(tmp_path, capsys, build):
    scenario = build()
    mapping = {"a": "n1", "b": "n2", "c": "n3", "d": "n4"}
    renamed = _rename(scenario, mapping)
    # Only the names changed: node order and topology are preserved.
    assert renamed["nodes"] == ["n1", "n2", "n3", "n4"]

    original = _run_ok(tmp_path, capsys, scenario, name="original.json")
    renamed_result = _run_ok(tmp_path, capsys, renamed, name="renamed.json")
    # Apart from the name mapping, every event time, sequence number and
    # final outcome is identical.
    assert _rename(original, mapping) == renamed_result


# -- public surface stays stable ----------------------------------------------


def test_version_and_explore_behavior_unchanged(tmp_path, capsys):
    code = main(["version"])
    out, err = capsys.readouterr()
    assert code == 0
    assert err == ""
    assert out == f"{__version__}\n"

    plan = {
        "scenario": _joint_reads_scenario(),
        "candidates": [
            {"from": "a", "to": "b", "message": "readProbe", "occurrence": 1,
             "action": "drop"},
            {"from": "b", "to": "a", "message": "readReply", "occurrence": 1,
             "action": "drop"},
        ],
        "maxFaults": 1,
        "maxCases": 10,
    }
    plan_path = _write(tmp_path, "plan.json", plan)
    code1 = main(["explore", plan_path])
    out1, err1 = capsys.readouterr()
    code2 = main(["explore", plan_path])
    out2, err2 = capsys.readouterr()
    assert (code1, err1) == (0, "")
    assert (code2, err2) == (0, "")
    assert out1 == out2
    report = json.loads(out1)
    assert report["totalCases"] == 3
    assert report["passedCases"] == 3
    assert report["failedCases"] == 0
    assert [case["selected"] for case in report["cases"]] == [[], [0], [1]]
    assert [case["status"] for case in report["cases"]] == ["passed"] * 3


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(initialMembers=["a", "b"]),  # below the 3-voter minimum
    lambda s: s.pop("membershipChanges"),
    lambda s: s.update(snapshotThreshold=0),
    lambda s: s.update(nodeEvents=[
        {"time": 100, "node": "b", "action": "crash"},
        {"time": 200, "node": "b", "action": "crash"},
    ]),
    lambda s: s.update(membershipChanges=[
        {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "z"}
    ]),
    lambda s: s.update(readQueries=[{"time": 120, "node": "a", "id": "x1"}]),
])
def test_invalid_scenarios_fail_with_single_error_line(tmp_path, capsys, mutate):
    scenario = _add_recovery_scenario()
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1
