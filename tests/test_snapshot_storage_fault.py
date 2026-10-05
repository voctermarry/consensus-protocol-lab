"""End-to-end regression tests for a persistence-barrier failure while a
lagging follower *receives* an installSnapshot.

The cross-functional path exercised here chains log replication, snapshot
compaction, a partition that leaves one follower far behind, network heal,
installSnapshot catch-up, a storage fault injected exactly at the follower's
"snapshot save" persistence barrier and (optionally) the automatic restart
that lets the leader retry the snapshot.

The scenario is deterministic and uses only existing simulate inputs and
output fields:

* a wins term 1 (its timeout is the shortest; c's is made long enough that
  the isolated follower never campaigns and disturbs the timeline);
* x1 replicates and applies on c *before* the partition closes at t=240,
  while x2..x4 commit on the a/b majority while c is isolated; a and b
  compact x1..x3 into a snapshot (snapshotThreshold 3);
* after the heal at t=500 the leader can only reconcile c with an
  installSnapshot: c's log (x1..x3) predates a's compacted prefix;
* a storageFaults rule targets exactly that receive-snapshot barrier on c
  (its 5th persistence barrier, which net-changes log + snapshot +
  commitIndex + lastApplied + applied in one atomic save).

The tests assert only on the public JSON report and the exit code: fault
recorded in the fixed field order, immediate crash, the failed snapshot
write's whole bundle rolled back as a unit, no installed result / reply /
state change / later send depending on it, no messageFaults occurrence
consumed by the suppressed reply, nodeDown deliveries while offline,
single application of snapshot commands, convergence, summaries, safety,
global seq continuity, byte-identical reruns and a ``matched`` replay.
"""

from __future__ import annotations

import json

from consensus_lab.cli import main


# Fixed virtual-time anchors of the scenario; derived from the deterministic
# event ordering, not tuned by retries (there is no randomness anywhere).
_T_HEAL = 500
_T_FAULT_AND_CRASH = 530
_T_RESTART = 630  # crash at 530 + restartDelay 100
_T_SEND = 520      # first post-heal installSnapshot sent by a
_T_OFFLINE_DROP = 580  # arrival after the crash, recorded nodeDown

_SNAPSHOT_FIELDS = ["log", "snapshot", "commitIndex", "lastApplied", "applied"]


def _write(tmp_path, name, payload):
    path = tmp_path / name
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _base_scenario(**overrides):
    """Three nodes: leader a compacts x1..x3 while follower c is isolated;
    c already holds/applied x1, so the received snapshot discards part of
    its log — the failed barrier therefore moves every persisted field."""
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 900,
        "electionTimeouts": {"a": 100, "b": 150, "c": 2000},
        "heartbeatInterval": 50,
        "messageDelay": 10,
        "snapshotThreshold": 3,
        "faults": [
            {"time": 240, "action": "partition", "groups": [["a", "b"], ["c"]]},
            {"time": _T_HEAL, "action": "heal"},
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


def _by_type(result, *types):
    return [entry for entry in result["timeline"] if entry["type"] in types]


def _results_for(result, message):
    return [
        entry
        for entry in result["timeline"]
        if entry["type"] == "messageResult" and entry["message"] == message
    ]


def _assert_client_summary_has_no_duplicates(result):
    """The four commands each land in exactly one client bucket, once."""
    summary = result["clients"]
    assert summary["superseded"] == []
    assert summary["pending"] == []
    assert summary["rejected"] == []
    committed = summary["committed"]
    assert [entry["id"] for entry in committed] == ["x1", "x2", "x3", "x4"]
    ids = [entry["id"] for entry in committed]
    assert len(ids) == len(set(ids))
    assert all(entry["node"] == "a" for entry in committed)
    assert [(entry["index"], entry["term"]) for entry in committed] == [
        (1, 1),
        (2, 1),
        (3, 1),
        (4, 1),
    ]


def _assert_seq_continuous(result):
    seqs = [entry["seq"] for entry in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def _assert_no_safety_violations(result):
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []
    # One leader for term 1, throughout — the fault never lets c campaign.
    assert result["electionSafety"]["leadersByTerm"] == {"1": ["a"]}


def _assert_clean_convergence(result):
    _assert_client_summary_has_no_duplicates(result)
    _assert_seq_continuous(result)
    _assert_no_safety_violations(result)
    node_c = result["nodes"]["c"]
    assert node_c["role"] == "follower"
    assert node_c["term"] == 1
    assert node_c["votedFor"] == "a"
    assert node_c["snapshot"] == {"lastIncludedIndex": 3, "lastIncludedTerm": 1}
    assert node_c["commitIndex"] == 4
    assert node_c["lastApplied"] == 4
    assert [entry["id"] for entry in node_c["log"]] == ["x4"]
    assert [entry["id"] for entry in node_c["applied"]] == ["x1", "x2", "x3", "x4"]


def test_setup_requires_install_snapshot_after_heal(tmp_path, capsys):
    """Sanity scaffold: without a storage fault the same scenario catches c
    up exactly once via installSnapshot — the fault tests rely on this."""
    code, out, err = _run(tmp_path, capsys, _base_scenario())
    assert code == 0
    assert err == ""
    result = json.loads(out)

    installs = _results_for(result, "installSnapshot")
    # Five partition drops, then exactly one delivered/installed at t=530.
    assert [(entry["time"], entry["result"], entry.get("reason") or entry["detail"]) for entry in installs] == [
        (280, "dropped", "partition"),
        (330, "dropped", "partition"),
        (380, "dropped", "partition"),
        (430, "dropped", "partition"),
        (480, "dropped", "partition"),
        (_T_FAULT_AND_CRASH, "delivered", "installed"),
    ]
    installed = _by_type(result, "snapshotInstalled")
    assert [(entry["time"], entry["node"], entry["peer"]) for entry in installed] == [
        (_T_FAULT_AND_CRASH, "c", "a")
    ]
    node_c = result["nodes"]["c"]
    assert node_c["snapshot"] == {"lastIncludedIndex": 3, "lastIncludedTerm": 1}
    assert [entry["id"] for entry in node_c["applied"]] == ["x1", "x2", "x3", "x4"]
    # Snapshot commands restore silently; only the suffix x4 is applied as an
    # applied event on c.
    applied_c = [entry for entry in _by_type(result, "applied") if entry["node"] == "c"]
    assert [(entry["time"], entry["index"], entry["id"]) for entry in applied_c] == [
        (230, 1, "x1"),
        (550, 4, "x4"),
    ]
    _assert_clean_convergence(result)


def test_failed_snapshot_barrier_is_atomic_and_retries_after_restart(tmp_path, capsys):
    scenario = _base_scenario(
        storageFaults=[{"node": "c", "occurrence": 5, "restartDelay": 100}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert err == ""
    result = json.loads(out)
    timeline = result["timeline"]

    # -- the fault record, in fixed field order, immediately followed by crash
    faults = _by_type(result, "storageFault")
    assert len(faults) == 1
    assert faults[0] == {
        "seq": 104,
        "time": _T_FAULT_AND_CRASH,
        "type": "storageFault",
        "node": "c",
        "occurrence": 5,
        "fields": _SNAPSHOT_FIELDS,
    }
    lifecycle = _by_type(result, "nodeLifecycle")
    assert [(entry["time"], entry["action"]) for entry in lifecycle] == [
        (_T_FAULT_AND_CRASH, "crash"),
        (_T_RESTART, "restart"),
    ]
    assert faults[0]["seq"] < lifecycle[0]["seq"] < lifecycle[1]["seq"]

    # -- every effect depending on the failed save is absent from the timeline
    # Full installSnapshot delivery history: five partition drops while
    # isolated, one nodeDown after the crash, then the single installed retry
    # after the restart. The failed t=530 attempt leaves no result at all.
    installs = _results_for(result, "installSnapshot")
    assert [
        (
            entry["time"],
            entry["result"],
            entry.get("reason") or entry.get("detail"),
        )
        for entry in installs
    ] == [
        (280, "dropped", "partition"),
        (330, "dropped", "partition"),
        (380, "dropped", "partition"),
        (430, "dropped", "partition"),
        (480, "dropped", "partition"),
        (_T_OFFLINE_DROP, "dropped", "nodeDown"),
        (_T_RESTART, "delivered", "installed"),
    ]
    # The only snapshotInstalled belongs to the successful retry, not the
    # failed attempt; the only reply traffic likewise.
    installed = _by_type(result, "snapshotInstalled")
    assert [(entry["time"], entry["node"], entry["peer"]) for entry in installed] == [
        (_T_RESTART, "c", "a")
    ]
    reply_sends = [
        entry for entry in timeline
        if entry["type"] == "messageSend" and entry["message"] == "installSnapshotReply"
    ]
    assert [entry["time"] for entry in reply_sends] == [_T_RESTART]
    reply_results = _results_for(result, "installSnapshotReply")
    assert [(entry["time"], entry["result"], entry["detail"]) for entry in reply_results] == [
        (_T_RESTART + 10, "delivered", "installed"),
    ]
    # No applied event straddles the crash; nothing was applied during the
    # failed attempt (snapshot contents restore silently anyway).
    applied_c = [entry for entry in _by_type(result, "applied") if entry["node"] == "c"]
    assert all(entry["time"] <= 240 or entry["time"] >= _T_RESTART for entry in applied_c)

    # The in-flight snapshot sent at 520 is the one recorded nodeDown at 580;
    # no installSnapshot delivery at all is recorded at the fault instant.
    assert all(
        not (
            entry["time"] == _T_FAULT_AND_CRASH
            and entry["message"] == "installSnapshot"
            and entry["result"] == "delivered"
        )
        for entry in timeline
        if entry["type"] == "messageResult"
    )
    # The leader keeps its 50ms resend cadence across the crash (the failed
    # barrier suppressed nothing on the sender side): five partition-era
    # sends, the 520 send whose receive failed, a 570 send dropped while c
    # was offline, and the 620 send that the restarted node installs. After
    # c is caught up no further installSnapshot is sent.
    assert [
        entry["time"]
        for entry in timeline
        if entry["type"] == "messageSend" and entry["message"] == "installSnapshot"
    ] == [270, 320, 370, 420, 470, _T_SEND, 570, 620]

    # -- after restart the snapshot is installed once and c applies x4 once
    node_c = result["nodes"]["c"]
    assert node_c["online"] is True
    assert node_c["restartCount"] == 1
    assert node_c["snapshot"] == {"lastIncludedIndex": 3, "lastIncludedTerm": 1}
    assert node_c["commitIndex"] == 4
    assert node_c["lastApplied"] == 4
    assert [entry["id"] for entry in node_c["log"]] == ["x4"]
    assert [entry["id"] for entry in node_c["applied"]] == ["x1", "x2", "x3", "x4"]
    # x1 was applied once before isolation and is never re-applied; x4 is
    # applied exactly once, after the restart.
    assert [(entry["index"], entry["id"]) for entry in applied_c] == [
        (1, "x1"),
        (4, "x4"),
    ]
    assert all(entry["time"] >= _T_RESTART for entry in applied_c if entry["id"] == "x4")

    _assert_clean_convergence(result)


def test_omitted_restart_delay_keeps_node_offline_on_prefault_state(tmp_path, capsys):
    scenario = _base_scenario(
        storageFaults=[{"node": "c", "occurrence": 5}],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert err == ""
    result = json.loads(out)
    timeline = result["timeline"]

    (fault,) = _by_type(result, "storageFault")
    assert fault["fields"] == _SNAPSHOT_FIELDS
    assert fault["time"] == _T_FAULT_AND_CRASH
    lifecycle = _by_type(result, "nodeLifecycle")
    assert [(entry["time"], entry["action"]) for entry in lifecycle] == [
        (_T_FAULT_AND_CRASH, "crash")
    ]

    # No installed result, no snapshotInstalled and no reply ever appear.
    assert _by_type(result, "snapshotInstalled") == []
    assert _results_for(result, "installSnapshotReply") == []
    assert [
        entry for entry in timeline
        if entry["type"] == "messageSend" and entry["message"] == "installSnapshotReply"
    ] == []
    assert [
        entry for entry in _results_for(result, "installSnapshot")
        if entry["result"] == "delivered"
    ] == []

    # The leader keeps resending every 50ms; every arrival from the crash
    # instant on is explicitly nodeDown, through the end of the run.
    assert [
        entry["time"]
        for entry in timeline
        if entry["type"] == "messageSend" and entry["message"] == "installSnapshot"
    ] == [270, 320, 370, 420, 470, _T_SEND, 570, 620, 670, 720, 770, 820, 870]
    installs = _results_for(result, "installSnapshot")
    assert [
        (
            entry["time"],
            entry["result"],
            entry.get("reason") or entry.get("detail"),
        )
        for entry in installs
    ] == [
        (280, "dropped", "partition"),
        (330, "dropped", "partition"),
        (380, "dropped", "partition"),
        (430, "dropped", "partition"),
        (480, "dropped", "partition"),
        (580, "dropped", "nodeDown"),
        (630, "dropped", "nodeDown"),
        (680, "dropped", "nodeDown"),
        (730, "dropped", "nodeDown"),
        (780, "dropped", "nodeDown"),
        (830, "dropped", "nodeDown"),
        (880, "dropped", "nodeDown"),
    ]
    post_fault = [entry for entry in installs if entry["time"] >= _T_FAULT_AND_CRASH]
    assert post_fault and all(
        entry["result"] == "dropped" and entry["reason"] == "nodeDown"
        for entry in post_fault
    )

    # Final state is exactly what c had last saved before the failed barrier:
    # term 1 / voted a, commitIndex/lastApplied 1, only x1 applied, the
    # pre-fault log x1..x3 intact, and no snapshot.
    node_c = result["nodes"]["c"]
    assert node_c["online"] is False
    assert node_c["restartCount"] == 0
    assert node_c["term"] == 1
    assert node_c["votedFor"] == "a"
    assert node_c["snapshot"] is None
    assert node_c["commitIndex"] == 1
    assert node_c["lastApplied"] == 1
    assert [entry["id"] for entry in node_c["log"]] == ["x1", "x2", "x3"]
    assert [entry["id"] for entry in node_c["applied"]] == ["x1"]

    # x2..x4 stay committed cluster-wide (a/b majority) with no duplicates.
    _assert_client_summary_has_no_duplicates(result)
    _assert_seq_continuous(result)
    _assert_no_safety_violations(result)


def test_suppressed_snapshot_reply_does_not_consume_message_fault_occurrence(tmp_path, capsys):
    scenario = _base_scenario(
        storageFaults=[{"node": "c", "occurrence": 5, "restartDelay": 100}],
        messageFaults=[
            {
                "from": "c",
                "to": "a",
                "message": "installSnapshotReply",
                "occurrence": 1,
                "action": "drop",
            }
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0
    assert err == ""
    result = json.loads(out)

    # Exactly one messageFault fires, and it fires at the restart-time
    # successful install — the suppressed reply from the failed attempt did
    # not consume occurrence 1.
    (message_fault,) = _by_type(result, "messageFault")
    assert message_fault["occurrence"] == 1
    assert message_fault["time"] == _T_RESTART
    assert message_fault["from"] == "c"
    assert message_fault["to"] == "a"
    assert message_fault["message"] == "installSnapshotReply"

    # The dropped first real reply makes a reconcile again: the next snapshot
    # is redundant ("ignored"), its reply is delivered, and appendEntries
    # then streams x4. c still converges and applies x4 exactly once.
    delivered_installs = [
        entry for entry in _results_for(result, "installSnapshot")
        if entry["result"] == "delivered"
    ]
    assert [(entry["time"], entry["detail"]) for entry in delivered_installs] == [
        (_T_RESTART, "installed"),
        (_T_RESTART + 50, "ignored"),
    ]
    reply_results = _results_for(result, "installSnapshotReply")
    assert [
        (
            entry["time"],
            entry["result"],
            entry.get("detail") or entry.get("reason"),
        )
        for entry in reply_results
    ] == [
        (_T_RESTART + 10, "dropped", "messageFault"),
        (_T_RESTART + 60, "delivered", "ignored"),
    ]
    applied_c = [entry for entry in _by_type(result, "applied") if entry["node"] == "c"]
    assert [(entry["index"], entry["id"]) for entry in applied_c] == [
        (1, "x1"),
        (4, "x4"),
    ]
    assert all(entry["time"] >= _T_RESTART for entry in applied_c if entry["id"] == "x4")

    _assert_clean_convergence(result)


def test_same_scenario_is_byte_identical_and_replay_matches(tmp_path, capsys):
    scenario = _base_scenario(
        storageFaults=[{"node": "c", "occurrence": 5, "restartDelay": 100}],
        messageFaults=[
            {
                "from": "c",
                "to": "a",
                "message": "installSnapshotReply",
                "occurrence": 1,
                "action": "drop",
            }
        ],
    )
    scenario_path = _write(tmp_path, "scenario.json", scenario)

    code = main(["simulate", scenario_path])
    first, err = capsys.readouterr()
    assert code == 0 and err == ""
    code = main(["simulate", scenario_path])
    second, err = capsys.readouterr()
    assert code == 0 and err == ""
    assert first == second  # byte for byte, including key order and spacing

    # Save the public output and hand it to replay.
    result_path = _write(tmp_path, "result.json", json.loads(first))
    code = main(["replay", scenario_path, result_path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    assert json.loads(out) == {"status": "matched"}


def test_normal_snapshot_install_and_no_storage_faults_path_unchanged(tmp_path, capsys):
    """The regression scenario with storageFaults omitted keeps the legacy
    shape and results: no lifecycle/fault fields on nodes, one install."""
    scenario = _base_scenario()
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    assert _by_type(result, "storageFault", "nodeLifecycle", "messageFault") == []
    # Lifecycle fields only exist while the lifecycle machinery is active.
    node_c = result["nodes"]["c"]
    assert "online" not in node_c
    assert "restartCount" not in node_c
    assert node_c["commitIndex"] == 4
    _assert_seq_continuous(result)
    _assert_no_safety_violations(result)

    # An explicitly empty storageFaults list is byte-identical to omitted.
    code, with_empty, err = _run(tmp_path, capsys, dict(scenario, storageFaults=[]))
    assert code == 0 and err == ""
    assert with_empty == out
