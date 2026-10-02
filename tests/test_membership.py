"""Tests for joint-consensus membership changes in the deterministic sim."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main


def _write_scenario(tmp_path, scenario):
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(scenario), encoding="utf-8")
    return str(path)


def _run(tmp_path, capsys, scenario):
    path = _write_scenario(tmp_path, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


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


def _by_type(result, event_type):
    return [e for e in result["timeline"] if e["type"] == event_type]


def _entry_sig(entry):
    return (entry["index"], entry.get("kind", "command"), entry.get("entryType"), entry.get("id"))


# -- learners before any change -------------------------------------------


def test_initial_learners_never_vote_or_campaign(tmp_path, capsys):
    code, out, err = _run(tmp_path, capsys, _membership_base())
    assert code == 0
    assert err == ""
    result = json.loads(out)

    assert result["nodes"]["a"]["role"] == "leader"
    assert result["nodes"]["d"]["membershipRole"] == "learner"
    for name in ("a", "b", "c"):
        assert result["nodes"][name]["membershipRole"] == "voter"

    candidates = [e["node"] for e in _by_type(result, "stateChange") if e["role"] == "candidate"]
    assert "d" not in candidates
    # The learner is never asked for a vote.
    rv_sends = [e for e in _by_type(result, "messageSend") if e["message"] == "requestVote"]
    assert all(e["peer"] != "d" for e in rv_sends)
    # With no changes there are no membership events or change rows.
    assert not _by_type(result, "membershipResult")
    assert not _by_type(result, "configurationApplied")
    assert result["membership"]["changes"] == []
    assert result["membership"] == {
        "initial": ["a", "b", "c"],
        "current": ["a", "b", "c"],
        "joint": None,
        "changes": [],
    }


# -- add member ------------------------------------------------------------


def test_add_member_joint_consensus_full_flow(tmp_path, capsys):
    scenario = _membership_base(
        membershipChanges=[
            {"time": 300, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ]
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert err == ""
    result = json.loads(out)

    # All nodes converge on joint then stable configuration entries.
    for name, node in result["nodes"].items():
        sigs = [_entry_sig(e) for e in node["log"]]
        assert (1, "config", "joint", "m1") in sigs
        assert (2, "config", "stable", "m1") in sigs
        assert node["commitIndex"] == 2
        assert node["membershipRole"] == "voter"

    assert result["membership"] == {
        "initial": ["a", "b", "c"],
        "current": ["a", "b", "c", "d"],
        "joint": None,
        "changes": [
            {"id": "m1", "node": "a", "action": "add", "member": "d",
             "outcome": "committed", "index": 2, "term": 1}
        ],
    }

    results = _by_type(result, "membershipResult")
    assert [(e["id"], e["result"], e.get("phase")) for e in results] == [
        ("m1", "accepted", "catchingUp")
    ]
    applied = _by_type(result, "configurationApplied")
    # Joint applied before stable, on every node, in index order per node.
    by_node = {}
    for e in applied:
        by_node.setdefault(e["node"], []).append(e["entryType"])
    assert set(by_node) == {"a", "b", "c", "d"}
    for sequence in by_node.values():
        assert sequence == ["joint", "stable"]

    # Config replication is marked distinctly on appendEntries results.
    markers = [
        e
        for e in _by_type(result, "messageResult")
        if e["message"] == "appendEntries" and e.get("configEntries")
    ]
    assert markers
    assert all(e["result"] == "delivered" for e in markers)
    kinds = {ce["entryType"] for e in markers for ce in e["configEntries"]}
    assert kinds == {"joint", "stable"}

    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


def test_command_and_config_entries_are_distinguishable(tmp_path, capsys):
    scenario = _membership_base(
        duration=700,
        clientCommands=[{"time": 400, "node": "a", "id": "x1", "command": "v"}],
        membershipChanges=[
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    for node in result["nodes"].values():
        kinds = {e.get("kind") for e in node["log"]} | {
            e.get("kind") for e in node["applied"]
        }
        assert kinds == {"command", "config"}
        commands = [e for e in node["applied"] if e.get("kind") == "command"]
        configs = [e for e in node["applied"] if e.get("kind") == "config"]
        assert [e["id"] for e in commands] == ["x1"]
        assert [e["entryType"] for e in configs] == ["joint", "stable"]
    assert [c["id"] for c in result["clients"]["committed"]] == ["x1"]


def test_add_member_is_deterministic(tmp_path, capsys):
    scenario = _membership_base(
        membershipChanges=[
            {"time": 300, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ]
    )
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2
    parsed = json.loads(out1)
    seqs = [e["seq"] for e in parsed["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


# -- remove member ---------------------------------------------------------


def _five_node_base(**overrides):
    scenario = {
        "nodes": ["a", "b", "c", "d", "e"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250, "e": 300},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "initialMembers": ["a", "b", "c", "d"],
        "membershipChanges": [],
    }
    scenario.update(overrides)
    return scenario


def test_remove_leader_steps_down_and_never_campaigns(tmp_path, capsys):
    scenario = _five_node_base(
        membershipChanges=[
            {"time": 300, "node": "a", "id": "rm-a", "action": "remove", "member": "a"}
        ]
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)

    assert result["membership"]["current"] == ["b", "c", "d"]
    change = result["membership"]["changes"][0]
    assert change["outcome"] == "committed"
    assert change["action"] == "remove"
    assert change["member"] == "a"

    node_a = result["nodes"]["a"]
    assert node_a["membershipRole"] == "learner"
    assert node_a["role"] == "follower"
    # After the stable configuration commits, the removed former leader never
    # becomes candidate again.
    stable_apply = next(
        e["time"]
        for e in _by_type(result, "configurationApplied")
        if e["node"] == "a" and e["entryType"] == "stable"
    )
    later_candidacies = [
        e for e in _by_type(result, "stateChange")
        if e["node"] == "a" and e["role"] == "candidate" and e["time"] >= stable_apply
    ]
    assert not later_candidacies
    stepdown = [
        e for e in _by_type(result, "stateChange")
        if e["node"] == "a" and e["reason"] == "removedFromCluster"
    ]
    assert len(stepdown) == 1
    # The cluster still has exactly one leader per term.
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


def test_rejection_precedence(tmp_path, capsys):
    scenario = _five_node_base(
        nodeEvents=[
            {"time": 250, "node": "a", "action": "crash"},
            {"time": 650, "node": "a", "action": "restart"},
        ],
        membershipChanges=[
            # Receiver down takes precedence over everything.
            {"time": 300, "node": "a", "id": "down", "action": "add", "member": "e"},
            # A follower is not the leader.
            {"time": 400, "node": "b", "id": "notleader", "action": "add", "member": "e"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    reasons = {c["id"]: c for c in result["membership"]["changes"]}
    assert reasons["down"]["outcome"] == "rejected"
    assert reasons["down"]["reason"] == "nodeDown"
    assert reasons["notleader"]["reason"] == "notLeader"
    results = {e["id"]: e for e in _by_type(result, "membershipResult")}
    assert results["down"]["reason"] == "nodeDown"
    assert results["notleader"]["reason"] == "notLeader"


def test_change_in_progress_already_not_member_minimum(tmp_path, capsys):
    # 4 voters (a,b,c,d) and learner e. rm-a keeps the change busy long enough
    # for a same-time batch of requests to observe changeInProgress first.
    scenario = _five_node_base(
        duration=360,
        membershipChanges=[
            {"time": 300, "node": "a", "id": "rm-a", "action": "remove", "member": "a"},
            {"time": 300, "node": "a", "id": "busy", "action": "remove", "member": "b"},
            {"time": 300, "node": "a", "id": "add-present", "action": "add", "member": "b"},
            {"time": 300, "node": "a", "id": "remove-absent", "action": "remove", "member": "e"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    rejected = {c["id"]: c["reason"] for c in result["membership"]["changes"]
                if c["outcome"] == "rejected"}
    # Every later request at the same timestamp is blocked by the in-flight
    # joint entry.
    assert rejected == {
        "busy": "changeInProgress",
        "add-present": "changeInProgress",
        "remove-absent": "changeInProgress",
    }

    # After the cluster settles at three voters (b,c,d), a further removal is
    # refused with minimumClusterSize, and an unknown member with notMember /
    # alreadyMember.
    scenario2 = _five_node_base(
        membershipChanges=[
            {"time": 200, "node": "a", "id": "rm-d", "action": "remove", "member": "d"},
            {"time": 500, "node": "a", "id": "too-small", "action": "remove", "member": "c"},
            {"time": 500, "node": "a", "id": "already", "action": "add", "member": "b"},
            {"time": 500, "node": "a", "id": "absent", "action": "remove", "member": "e"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario2)
    assert code == 0
    result2 = json.loads(out)
    rejected2 = {c["id"]: c["reason"] for c in result2["membership"]["changes"]
                 if c["outcome"] == "rejected"}
    assert rejected2 == {
        "too-small": "minimumClusterSize",
        "already": "alreadyMember",
        "absent": "notMember",
    }


def test_summary_reports_joint_phase_while_unfinished(tmp_path, capsys):
    # Ends after the joint entry commits but before the stable entry can.
    scenario = _membership_base(
        duration=316,
        membershipChanges=[
            {"time": 300, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    membership = result["membership"]
    assert membership["current"] == ["a", "b", "c"]
    assert membership["joint"] == {
        "id": "m1",
        "old": ["a", "b", "c"],
        "new": ["a", "b", "c", "d"],
    }
    (change,) = membership["changes"]
    assert change["outcome"] == "pending"
    assert change["phase"] == "joint"
    assert change["joint"] == {"old": ["a", "b", "c"], "new": ["a", "b", "c", "d"]}


def test_readd_member_restores_voter(tmp_path, capsys):
    scenario = _membership_base(
        duration=1100,
        initialMembers=["a", "b", "c", "d"],
        membershipChanges=[
            {"time": 200, "node": "a", "id": "rm-d", "action": "remove", "member": "d"},
            {"time": 600, "node": "a", "id": "add-d", "action": "add", "member": "d"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert result["membership"]["current"] == ["a", "b", "c", "d"]
    assert result["nodes"]["d"]["membershipRole"] == "voter"
    outcomes = {c["id"]: c["outcome"] for c in result["membership"]["changes"]}
    assert outcomes == {"rm-d": "committed", "add-d": "committed"}
    assert result["electionSafety"]["violations"] == []


def test_add_stays_pending_catching_up_when_learner_isolated(tmp_path, capsys):
    # The learner has real entries to catch but is permanently partitioned,
    # so the joint entry is never appended and the change stays pending.
    scenario = _membership_base(
        duration=700,
        faults=[
            {"time": 100, "action": "partition", "groups": [["d"], ["a", "b", "c"]]}
        ],
        clientCommands=[
            {"time": 120, "node": "a", "id": "x1", "command": 1},
            {"time": 130, "node": "a", "id": "x2", "command": 2},
        ],
        membershipChanges=[
            {"time": 300, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    membership = result["membership"]
    assert membership["current"] == ["a", "b", "c"]
    assert membership["joint"] is None
    (change,) = membership["changes"]
    assert change["outcome"] == "pending"
    assert change["phase"] == "catchingUp"
    assert result["nodes"]["d"]["membershipRole"] == "learner"
    assert result["nodes"]["d"]["log"] == []


# -- snapshots and crash/restart -------------------------------------------


def test_snapshot_persists_configuration(tmp_path, capsys):
    scenario = _membership_base(
        duration=700,
        snapshotThreshold=2,
        membershipChanges=[
            {"time": 150, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        clientCommands=[
            {"time": 250, "node": "a", "id": "x1", "command": "one"},
            {"time": 300, "node": "a", "id": "x2", "command": "two"},
        ],
    )
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    for node in result["nodes"].values():
        # Config entries are compacted into the snapshot but membership
        # survives and the change is still reported committed.
        assert node["snapshot"]["lastIncludedIndex"] == 4
        kinds = [e.get("kind") for e in node["applied"]]
        assert kinds == ["config", "config", "command", "command"]
        assert node["membershipRole"] == "voter"
    assert result["membership"]["current"] == ["a", "b", "c", "d"]
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


def test_removed_member_demoted_through_installed_snapshot(tmp_path, capsys):
    scenario = {
        "nodes": ["a", "b", "c", "d", "e"],
        "duration": 1200,
        "electionTimeouts": {"a": 80, "b": 150, "c": 100, "d": 200, "e": 300},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "snapshotThreshold": 1,
        "initialMembers": ["a", "b", "c", "d", "e"],
        "faults": [
            {"time": 120, "action": "partition", "groups": [["c"], ["a", "b", "d", "e"]]},
            {"time": 500, "action": "heal"},
        ],
        "membershipChanges": [
            {"time": 150, "node": "a", "id": "rm-c", "action": "remove", "member": "c"}
        ],
        "clientCommands": [{"time": 250, "node": "a", "id": "x1", "command": "one"}],
    }
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    node_c = result["nodes"]["c"]
    assert node_c["membershipRole"] == "learner"
    assert node_c["role"] == "follower"
    installed = _by_type(result, "snapshotInstalled")
    assert any(e["node"] == "c" for e in installed)
    # Configuration restored from a snapshot is never re-applied.
    assert not [
        e for e in _by_type(result, "configurationApplied") if e["node"] == "c"
    ]
    after_heal = [
        e for e in _by_type(result, "stateChange")
        if e["node"] == "c" and e["role"] == "candidate" and e["time"] >= 500
    ]
    assert not after_heal
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


def test_added_learner_can_catch_up_via_snapshot(tmp_path, capsys):
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 900,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 300},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "snapshotThreshold": 1,
        "initialMembers": ["a", "b", "c"],
        "faults": [
            {"time": 60, "action": "partition", "groups": [["d"], ["a", "b", "c"]]},
            {"time": 400, "action": "heal"},
        ],
        "clientCommands": [
            {"time": 100, "node": "a", "id": "x1", "command": "one"},
            {"time": 110, "node": "a", "id": "x2", "command": "two"},
        ],
        "membershipChanges": [
            {"time": 300, "node": "a", "id": "add-d", "action": "add", "member": "d"}
        ],
    }
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert any(e["node"] == "d" for e in _by_type(result, "snapshotInstalled"))
    assert result["nodes"]["d"]["membershipRole"] == "voter"
    assert result["membership"]["current"] == ["a", "b", "c", "d"]
    assert result["membership"]["changes"][0]["outcome"] == "committed"


def test_removed_member_stays_demoted_after_crash_restart(tmp_path, capsys):
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 1100,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 90},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "initialMembers": ["a", "b", "c", "d"],
        "membershipChanges": [
            {"time": 200, "node": "a", "id": "rm-d", "action": "remove", "member": "d"}
        ],
        "nodeEvents": [
            {"time": 400, "node": "d", "action": "crash"},
            {"time": 600, "node": "d", "action": "restart"},
        ],
    }
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    node_d = result["nodes"]["d"]
    assert node_d["membershipRole"] == "learner"
    assert node_d["role"] == "follower"
    assert not [
        e for e in _by_type(result, "stateChange")
        if e["node"] == "d" and e["role"] == "candidate"
    ]
    # No configuration is applied twice after restart.
    applied = [e for e in _by_type(result, "configurationApplied") if e["node"] == "d"]
    assert [e["entryType"] for e in applied] == ["joint", "stable"]


# -- leader change mid-membership change -----------------------------------


def test_new_leader_finishes_change_after_joint_committed(tmp_path, capsys):
    # The old leader appends and commits the joint entry, then is partitioned
    # and crashes before appending the stable entry. The newly elected leader
    # holds the joint entry and appends the stable one; the term-2 stable
    # entry commits and carries the term-1 joint entry with it.
    scenario = {
        "nodes": ["a", "b", "c", "d"],
        "duration": 1200,
        "electionTimeouts": {"a": 80, "b": 90, "c": 200, "d": 300},
        "heartbeatInterval": 40,
        "messageDelay": 5,
        "initialMembers": ["a", "b", "c"],
        "faults": [
            {"time": 211, "action": "partition", "groups": [["a"], ["b", "c", "d"]]},
            {"time": 700, "action": "heal"},
        ],
        "nodeEvents": [
            {"time": 212, "node": "a", "action": "crash"},
            {"time": 800, "node": "a", "action": "restart"},
        ],
        "membershipChanges": [
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
    }
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)

    change = result["membership"]["changes"][0]
    assert change["outcome"] == "committed"
    assert change["index"] == 2
    assert change["term"] == 2
    assert result["membership"]["joint"] is None
    assert result["membership"]["current"] == ["a", "b", "c", "d"]

    # Exactly one joint and one stable entry per node, shared by all.
    for node in result["nodes"].values():
        sigs = [(e["index"], e["term"], e.get("entryType")) for e in node["log"]]
        assert sigs == [(1, 1, "joint"), (2, 2, "stable")]
        assert node["membershipRole"] == "voter"

    assert result["electionSafety"]["leadersByTerm"] == {"1": ["a"], "2": ["b"]}
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []


# -- backward-compatible shapes --------------------------------------------

def test_no_membership_fields_keeps_legacy_shape(tmp_path, capsys):
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 400,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 10,
        "clientCommands": [{"time": 200, "node": "a", "id": "x1", "command": 1}],
    }
    code, out, _ = _run(tmp_path, capsys, scenario)
    assert code == 0
    result = json.loads(out)
    assert "membership" not in result
    for node in result["nodes"].values():
        assert "membershipRole" not in node
        # Untyped command entries.
        assert "kind" not in node["applied"][0]
    assert not _by_type(result, "membershipResult")
    assert not _by_type(result, "configurationApplied")
    assert not [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "appendEntries" and "configEntries" in e
    ]


# -- validation -------------------------------------------------------------


@pytest.mark.parametrize("mutate", [
    lambda s: s.pop("membershipChanges"),
    lambda s: s.pop("initialMembers"),
    lambda s: s.update(initialMembers=["a", "b"]),
    lambda s: s.update(initialMembers="abc"),
    lambda s: s.update(initialMembers=["a", "b", "a"]),
    lambda s: s.update(initialMembers=["a", "b", "z"]),
    lambda s: s.update(initialMembers=["a", "b", 1]),
    lambda s: s.update(membershipChanges=[{"time": 100, "node": "a", "id": "m", "action": "add"}]),
    lambda s: s.update(membershipChanges=[{"time": 100, "node": "a", "id": "m", "action": "add", "member": "d", "x": 1}]),
    lambda s: s.update(membershipChanges=[{"time": 100, "node": "a", "id": "m", "action": "wat", "member": "d"}]),
    lambda s: s.update(membershipChanges=[{"time": "x", "node": "a", "id": "m", "action": "add", "member": "d"}]),
    lambda s: s.update(membershipChanges=[{"time": -1, "node": "a", "id": "m", "action": "add", "member": "d"}]),
    lambda s: s.update(membershipChanges=[{"time": 601, "node": "a", "id": "m", "action": "add", "member": "d"}]),
    lambda s: s.update(membershipChanges=[{"time": 100, "node": "z", "id": "m", "action": "add", "member": "d"}]),
    lambda s: s.update(membershipChanges=[{"time": 100, "node": "a", "id": "m", "action": "add", "member": "z"}]),
    lambda s: s.update(membershipChanges=[{"time": 100, "node": "a", "id": "", "action": "add", "member": "d"}]),
    lambda s: s.update(membershipChanges="nope"),
    lambda s: s.update(
        membershipChanges=[
            {"time": 100, "node": "a", "id": "m", "action": "add", "member": "d"},
            {"time": 200, "node": "a", "id": "m", "action": "add", "member": "d"},
        ]
    ),
    lambda s: s.update(
        membershipChanges=[{"time": 100, "node": "a", "id": "dup", "action": "add", "member": "d"}],
        clientCommands=[{"time": 90, "node": "a", "id": "dup", "command": 1}],
    ),
    lambda s: s.update(extraField=1),
])
def test_invalid_membership_scenarios(tmp_path, capsys, mutate):
    scenario = _membership_base(duration=600)
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1
