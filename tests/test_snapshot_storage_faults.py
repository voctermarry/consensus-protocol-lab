"""End-to-end regression tests for a storage fault injected exactly at the
persistence barrier of an installSnapshot delivery: the failed snapshot
save (snapshot, commitIndex, lastApplied, applied and any log change) must
be rolled back as one atomic unit, every dependent effect must stay
invisible, and the node must then recover — or stay offline — according to
the storageFaults rule."""

from __future__ import annotations

import json

from consensus_lab.cli import main


def _write(tmp_path, name, payload):
    path = tmp_path / name
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _scenario(**overrides):
    # Deterministic three-node layout: a leads term 1, b follows, and c's
    # election timeout sits far beyond the duration so it never campaigns —
    # once partitioned it can only catch up through the leader's
    # installSnapshot. c keeps one uncommitted entry (x1); a commits x1..x4
    # with b and compacts through index 3 while c is cut off.
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 900,
        "electionTimeouts": {"a": 100, "b": 150, "c": 5000},
        "heartbeatInterval": 50,
        "messageDelay": 10,
        "snapshotThreshold": 3,
        "faults": [
            {"time": 220, "action": "partition", "groups": [["a", "b"], ["c"]]},
            {"time": 500, "action": "heal"},
        ],
        "clientCommands": [
            {"time": 200, "node": "a", "id": "x1", "command": "one"},
            {"time": 210, "node": "a", "id": "x2", "command": "two"},
            {"time": 220, "node": "a", "id": "x3", "command": "three"},
            {"time": 230, "node": "a", "id": "x4", "command": "four"},
        ],
    }
    scenario.update(overrides)
    return scenario


def _run(tmp_path, capsys, scenario):
    path = _write(tmp_path, "scenario.json", scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _events(result, *types):
    return [entry for entry in result["timeline"] if entry["type"] in types]


def _assert_global_invariants(result):
    seqs = [entry["seq"] for entry in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))
    # Every client command committed exactly once; no other outcome.
    assert result["clients"] == {
        "committed": [
            {"id": "x1", "node": "a", "index": 1, "term": 1},
            {"id": "x2", "node": "a", "index": 2, "term": 1},
            {"id": "x3", "node": "a", "index": 3, "term": 1},
            {"id": "x4", "node": "a", "index": 4, "term": 1},
        ],
        "superseded": [],
        "pending": [],
        "rejected": [],
    }
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}
    assert result["logMatching"] == {"violations": []}
    assert result["stateMachineSafety"] == {"violations": []}


def test_partitioned_follower_catches_up_only_via_install_snapshot(tmp_path, capsys):
    # Baseline without storageFaults: the heal lets a's installSnapshot
    # through exactly once, and the legacy output shape is untouched.
    code, out, err = _run(tmp_path, capsys, _scenario())
    assert code == 0 and err == ""
    result = json.loads(out)

    assert _events(result, "storageFault", "nodeLifecycle") == []
    for node in result["nodes"].values():
        assert "online" not in node
        assert "restartCount" not in node

    # No appendEntries reaches c between the partition and the install:
    # the snapshot is the only way back (the suffix x4 follows normally).
    accepted_appends = [
        entry
        for entry in _events(result, "messageResult")
        if entry.get("node") == "c"
        and entry.get("message") == "appendEntries"
        and entry.get("result") == "delivered"
        and 220 < entry["time"] < 530
    ]
    assert accepted_appends == []
    (installed,) = _events(result, "snapshotInstalled")
    assert (installed["time"], installed["node"], installed["peer"]) == (530, "c", "a")
    assert (installed["lastIncludedIndex"], installed["lastIncludedTerm"]) == (3, 1)

    node_c = result["nodes"]["c"]
    assert node_c["snapshot"] == {"lastIncludedIndex": 3, "lastIncludedTerm": 1}
    assert [entry["id"] for entry in node_c["applied"]] == ["x1", "x2", "x3", "x4"]
    _assert_global_invariants(result)


def test_failed_snapshot_save_rolls_back_atomically_and_recovers(tmp_path, capsys):
    # c's barriers: 1 = vote for a (t=110), 2 = appending x1 (t=210),
    # 3 = saving the installSnapshot delivered at t=530. The fault kills
    # that save; c restarts 50ms later from its last saved state and the
    # leader's next snapshot carries it to the committed frontier.
    scenario = _scenario(
        messageFaults=[
            {
                "from": "c",
                "to": "a",
                "message": "installSnapshotReply",
                "occurrence": 1,
                "action": "drop",
            }
        ],
        storageFaults=[{"node": "c", "occurrence": 3, "restartDelay": 50}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)

    # The failed barrier is recorded with exactly the fields it was about
    # to save, in the fixed persistence order.
    (fault,) = _events(result, "storageFault")
    assert fault == {
        "seq": 97,
        "time": 530,
        "type": "storageFault",
        "node": "c",
        "occurrence": 3,
        "fields": ["log", "snapshot", "commitIndex", "lastApplied", "applied"],
    }
    lifecycle = _events(result, "nodeLifecycle")
    assert [(e["time"], e["node"], e["action"]) for e in lifecycle] == [
        (530, "c", "crash"),
        (580, "c", "restart"),
    ]

    # Nothing depending on the failed save is visible: the delivery at
    # t=530 left no messageResult, no snapshotInstalled, no reply send and
    # no stateChange — the only c events at that instant are the fault and
    # the crash.
    assert [
        entry
        for entry in result["timeline"]
        if entry["time"] == 530 and entry.get("node") == "c"
    ] == [fault, lifecycle[0]]
    installs = [
        entry
        for entry in _events(result, "messageResult")
        if entry.get("message") == "installSnapshot" and entry.get("result") == "delivered"
    ]
    assert [(e["time"], e["detail"]) for e in installs] == [
        (580, "installed"),
        (630, "ignored"),
    ]
    (installed,) = _events(result, "snapshotInstalled")
    assert (installed["time"], installed["node"], installed["peer"]) == (580, "c", "a")
    reply_sends = [
        entry
        for entry in _events(result, "messageSend")
        if entry["message"] == "installSnapshotReply"
    ]
    assert [entry["time"] for entry in reply_sends] == [580, 630]

    # The suppressed reply at t=530 did not consume the first
    # (c -> a, installSnapshotReply) send: the drop rule hits the reply c
    # sends after its restart.
    (message_fault,) = _events(result, "messageFault")
    assert (message_fault["time"], message_fault["occurrence"]) == (580, 1)
    assert message_fault["action"] == "drop"

    # The snapshot contents are applied exactly once (silently, inside the
    # snapshot); only the suffix entry x4 produces an applied event.
    applied_c = [e for e in _events(result, "applied") if e["node"] == "c"]
    assert [(e["index"], e["id"]) for e in applied_c] == [(4, "x4")]

    # c restarted from its last saved state and caught up to the frontier.
    node_c = result["nodes"]["c"]
    assert node_c["online"] is True
    assert node_c["restartCount"] == 1
    assert node_c["snapshot"] == {"lastIncludedIndex": 3, "lastIncludedTerm": 1}
    assert node_c["log"] == [{"index": 4, "term": 1, "id": "x4", "command": "four"}]
    assert node_c["commitIndex"] == 4
    assert node_c["lastApplied"] == 4
    assert [entry["id"] for entry in node_c["applied"]] == ["x1", "x2", "x3", "x4"]
    for name in ("a", "b"):
        assert result["nodes"][name]["online"] is True
        assert result["nodes"][name]["restartCount"] == 0
        assert result["nodes"][name]["commitIndex"] == 4

    _assert_global_invariants(result)


def test_failed_snapshot_save_without_restart_stays_offline(tmp_path, capsys):
    # Same barrier, no restartDelay: c crashes at the failed save and never
    # comes back; later deliveries are recorded nodeDown and the final
    # state keeps exactly what was persisted before the fault.
    scenario = _scenario(storageFaults=[{"node": "c", "occurrence": 3}])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)

    (fault,) = _events(result, "storageFault")
    assert (fault["time"], fault["node"], fault["occurrence"]) == (530, "c", 3)
    assert fault["fields"] == ["log", "snapshot", "commitIndex", "lastApplied", "applied"]
    assert [(e["action"]) for e in _events(result, "nodeLifecycle")] == ["crash"]

    # The failed install produced nothing: no snapshotInstalled, no reply.
    assert _events(result, "snapshotInstalled") == []
    assert not [
        entry
        for entry in _events(result, "messageSend")
        if entry["message"] == "installSnapshotReply"
    ]

    # Every delivery to c after the crash is an explicit nodeDown drop.
    post_crash = [
        entry
        for entry in _events(result, "messageResult")
        if entry.get("node") == "c" and entry["time"] > 530
    ]
    assert post_crash
    assert all(
        entry["result"] == "dropped" and entry["reason"] == "nodeDown"
        for entry in post_crash
    )

    # The persisted image is the last successful save: term 1, voted for
    # a, only the uncommitted x1 in the log, nothing applied, no snapshot.
    node_c = result["nodes"]["c"]
    assert node_c["online"] is False
    assert node_c["restartCount"] == 0
    assert node_c["term"] == 1
    assert node_c["votedFor"] == "a"
    assert node_c["log"] == [{"index": 1, "term": 1, "id": "x1", "command": "one"}]
    assert node_c["snapshot"] is None
    assert node_c["commitIndex"] == 0
    assert node_c["lastApplied"] == 0
    assert node_c["applied"] == []

    _assert_global_invariants(result)


def test_snapshot_storage_fault_run_is_deterministic_and_replayable(tmp_path, capsys):
    scenario = _scenario(
        messageFaults=[
            {
                "from": "c",
                "to": "a",
                "message": "installSnapshotReply",
                "occurrence": 1,
                "action": "drop",
            }
        ],
        storageFaults=[{"node": "c", "occurrence": 3, "restartDelay": 50}],
    )
    scenario_path = _write(tmp_path, "scenario.json", scenario)
    code, first, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    code, second, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    assert first == second

    result_path = _write(tmp_path, "result.json", json.loads(first))
    code = main(["replay", scenario_path, result_path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    assert json.loads(out) == {"status": "matched"}
