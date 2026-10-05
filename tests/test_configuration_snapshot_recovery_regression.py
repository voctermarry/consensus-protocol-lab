"""End-to-end combination regressions over the public simulate / explore /
replay / version entry points.

Where the feature tests pin one semantic at a time, these scenarios drive the
whole lifecycle in one run: a learner is promoted (joint then stable config),
client commands and both configuration entries are compacted into snapshots,
nodes crash *after* the configuration entries entered their snapshots and
recover from the persisted snapshot, and a later partition makes the old and
new voter majorities observably different for elections, commits and
read-only queries.

The scenarios are fixed virtual-time inputs (no wall clock, no randomness),
so every run is byte identical; replay must reproduce the saved result field
by field, and renaming the nodes while keeping their order and topology must
produce the same timeline up to the name mapping.
"""

from __future__ import annotations

import copy
import json

from consensus_lab.cli import main


# -- fixtures ---------------------------------------------------------------


def _combined_scenario() -> dict:
    # Four initial voters (a..d) and two learners (e, f). a wins term 1 and
    # stays leader until the partitions:
    #   110/111  x1, x2 accepted -> indices 1, 2
    #   150      add-e; the joint entry is index 3, stable index 4; the
    #            snapshot threshold (3) folds x1, x2 and the joint entry
    #            into a snapshot at index 3 the moment it is applied
    #   250/251  x3 (5), x4 (6) -> snapshot at 6
    #   350      remove d; joint index 7, stable index 8
    #   450/451  x5 (9) -> snapshot at 9 (460 on a, 495 on the followers);
    #            x6 is index 10
    #   400      b crashes before index 9 exists; it restarts at 700 and can
    #            only catch up through installSnapshot (745, -> index 9)
    #   600/650  d crashes after *both* configuration entries and the index-9
    #            snapshot are persisted, then restarts: the latest stable
    #            config ({a,b,c,e}) must come back from the snapshot and d
    #            must neither campaign nor vote again
    #   900      partition [c,d,e,f] | [a,b]: c (timeout 200) campaigns at
    #            1095/1295/1495 but can reach only e of the current voters,
    #            so it never wins term 2/3/4
    #   1150     partition [a,b,d,f] | [c,e]: x7 (index 11) is accepted by a
    #            at 1200 and replicated to b plus the removed d and the
    #            never-admitted f, but not to c or e: an old-config majority
    #            ({a,b,d}) holds it while the current config {a,b,c,e} has
    #            only {a,b}, so x7 must stay pending
    # f stays a learner for the whole run and must never receive a vote
    # request or contribute a vote.
    return {
        "nodes": ["a", "b", "c", "d", "e", "f"],
        "duration": 1500,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250, "e": 5000, "f": 5000},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "snapshotThreshold": 3,
        "initialMembers": ["a", "b", "c", "d"],
        "membershipChanges": [
            {"time": 150, "node": "a", "id": "add-e", "action": "add", "member": "e"},
            {"time": 350, "node": "a", "id": "rm-d", "action": "remove", "member": "d"},
        ],
        "clientCommands": [
            {"time": 110, "node": "a", "id": "x1", "command": "one"},
            {"time": 111, "node": "a", "id": "x2", "command": "two"},
            {"time": 250, "node": "a", "id": "x3", "command": "three"},
            {"time": 251, "node": "a", "id": "x4", "command": "four"},
            {"time": 450, "node": "a", "id": "x5", "command": "five"},
            {"time": 451, "node": "a", "id": "x6", "command": "six"},
            {"time": 1200, "node": "a", "id": "x7", "command": "seven"},
        ],
        "nodeEvents": [
            {"time": 400, "node": "b", "action": "crash"},
            {"time": 600, "node": "d", "action": "crash"},
            {"time": 650, "node": "d", "action": "restart"},
            {"time": 700, "node": "b", "action": "restart"},
        ],
        "faults": [
            {"time": 900, "action": "partition",
             "groups": [["c", "d", "e", "f"], ["a", "b"]]},
            {"time": 1150, "action": "partition",
             "groups": [["a", "b", "d", "f"], ["c", "e"]]},
        ],
    }


def _joint_reads_scenario() -> dict:
    # Three voters (a,b,c) and two learners (d,e). add-d appends the joint
    # entry at index 3 while snapshotThreshold is 3, so on a the joint entry
    # is compacted at t=160 immediately after applying.
    #   r-bad (162): probes to c and d are dropped; acks are only a, b and
    #     the learner e -> the old set {a,b,c} has a majority but the new set
    #     {a,b,c,d} does not -> stays pending until the end
    #   r-good (163): c and d answer; a,b,c give both joint sets their
    #     majority -> completed at 173; its state is the full client-command
    #     prefix through readIndex 3, i.e. exactly x1, x2 (the joint config
    #     entry at index 3 is excluded)
    #   r-late (300): after the stable entry (index 4), x3/x4 (indices 5,6)
    #     and the index-6 snapshot; completion returns the complete prefix
    #     including the compacted x1..x4 and excluding both config entries
    return {
        "nodes": ["a", "b", "c", "d", "e"],
        "duration": 600,
        "electionTimeouts": {"a": 80, "b": 150, "c": 200, "d": 250, "e": 300},
        "heartbeatInterval": 50,
        "messageDelay": 5,
        "snapshotThreshold": 3,
        "initialMembers": ["a", "b", "c"],
        "membershipChanges": [
            {"time": 150, "node": "a", "id": "add-d", "action": "add", "member": "d"},
        ],
        "clientCommands": [
            {"time": 110, "node": "a", "id": "x1", "command": "one"},
            {"time": 111, "node": "a", "id": "x2", "command": "two"},
            {"time": 250, "node": "a", "id": "x3", "command": "three"},
            {"time": 251, "node": "a", "id": "x4", "command": "four"},
        ],
        "messageFaults": [
            {"from": "a", "to": "c", "message": "readProbe",
             "occurrence": 1, "action": "drop"},
            {"from": "a", "to": "d", "message": "readProbe",
             "occurrence": 1, "action": "drop"},
        ],
        "readQueries": [
            {"time": 162, "node": "a", "id": "r-bad"},
            {"time": 163, "node": "a", "id": "r-good"},
            {"time": 300, "node": "a", "id": "r-late"},
        ],
    }


_RENAME = {
    "a": "alpha", "b": "bravo", "c": "charlie", "d": "delta",
    "e": "echo", "f": "foxtrot",
}


def _rename_tree(value):
    """Rename every node-name *value and dictionary key* while leaving every
    other string (command payloads, ids such as "add-e" or "x1") untouched."""
    if isinstance(value, str):
        return _RENAME.get(value, value)
    if isinstance(value, list):
        return [_rename_tree(item) for item in value]
    if isinstance(value, dict):
        return {
            _RENAME.get(key, key): _rename_tree(item)
            for key, item in value.items()
        }
    return value


def _write(tmp_path, name, payload) -> str:
    path = tmp_path / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _simulate(tmp_path, capsys, scenario, *, name="scenario.json"):
    path = _write(tmp_path, name, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _simulate_ok(tmp_path, capsys, scenario, *, name="scenario.json"):
    code, out, err = _simulate(tmp_path, capsys, scenario, name=name)
    assert code == 0
    assert err == ""
    return json.loads(out), out


def _events(result, *event_types):
    return [e for e in result["timeline"] if e["type"] in event_types]


def _assert_core_invariants_clean(result, *, reads_enabled):
    assert result["electionSafety"]["violations"] == []
    assert result["logMatching"]["violations"] == []
    assert result["stateMachineSafety"]["violations"] == []
    if reads_enabled:
        assert result["linearizability"] == {"violations": []}
    else:
        assert "linearizability" not in result
    seqs = [e["seq"] for e in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


# -- main combined scenario: configuration across snapshots and recovery ----


def test_configuration_survives_snapshot_and_crash_recovery(tmp_path, capsys):
    result, _ = _simulate_ok(tmp_path, capsys, _combined_scenario())

    # The full ordered timeline of configuration application: each of the two
    # changes applies joint then stable, on every node in node order, and the
    # leader always applies first. No entry is ever applied twice.
    assert [
        (e["time"], e["node"], e["index"], e["entryType"], e["id"])
        for e in _events(result, "configurationApplied")
    ] == [
        (160, "a", 3, "joint", "add-e"),
        (165, "b", 3, "joint", "add-e"),
        (165, "c", 3, "joint", "add-e"),
        (165, "d", 3, "joint", "add-e"),
        (165, "e", 3, "joint", "add-e"),
        (165, "f", 3, "joint", "add-e"),
        (170, "a", 4, "stable", "add-e"),
        (195, "b", 4, "stable", "add-e"),
        (195, "c", 4, "stable", "add-e"),
        (195, "d", 4, "stable", "add-e"),
        (195, "e", 4, "stable", "add-e"),
        (195, "f", 4, "stable", "add-e"),
        (360, "a", 7, "joint", "rm-d"),
        (365, "b", 7, "joint", "rm-d"),
        (365, "c", 7, "joint", "rm-d"),
        (365, "d", 7, "joint", "rm-d"),
        (365, "e", 7, "joint", "rm-d"),
        (365, "f", 7, "joint", "rm-d"),
        (370, "a", 8, "stable", "rm-d"),
        (395, "b", 8, "stable", "rm-d"),
        (395, "c", 8, "stable", "rm-d"),
        (395, "d", 8, "stable", "rm-d"),
        (395, "e", 8, "stable", "rm-d"),
        (395, "f", 8, "stable", "rm-d"),
    ]

    # Snapshot creation order, including the threshold boundaries at 3, 6, 9.
    # b is down from 400 to 700, so it never creates the index-9 snapshot and
    # receives it via installSnapshot instead; d creates it at 495 and keeps
    # it across its 600/650 crash.
    assert [
        (e["time"], e["node"], e["lastIncludedIndex"], e["lastIncludedTerm"])
        for e in _events(result, "snapshotCreated")
    ] == [
        (160, "a", 3, 1), (165, "b", 3, 1), (165, "c", 3, 1),
        (165, "d", 3, 1), (165, "e", 3, 1), (165, "f", 3, 1),
        (261, "a", 6, 1), (295, "b", 6, 1), (295, "c", 6, 1),
        (295, "d", 6, 1), (295, "e", 6, 1), (295, "f", 6, 1),
        (460, "a", 9, 1), (495, "c", 9, 1), (495, "d", 9, 1),
        (495, "e", 9, 1), (495, "f", 9, 1),
    ]
    assert [
        (e["time"], e["node"], e["peer"], e["lastIncludedIndex"])
        for e in _events(result, "snapshotInstalled")
    ] == [(745, "b", "a", 9)]

    # Node lifecycle ordering, including d crashing after the config entries
    # entered the index-9 snapshot and b crashing before index 9 exists.
    assert [
        (e["time"], e["node"], e["action"])
        for e in _events(result, "nodeLifecycle")
    ] == [
        (400, "b", "crash"), (600, "d", "crash"),
        (650, "d", "restart"), (700, "b", "restart"),
    ]

    # At t=160 on a, committing the joint config applies it, then the
    # snapshot compacts it; at t=745 b installs the snapshot (which restores
    # the configurations silently) instead of re-applying anything.
    by_seq = {e["seq"]: e for e in result["timeline"]}

    def seq_of(time, node, event_type, **fields):
        matches = [
            e for e in result["timeline"]
            if e["time"] == time and e["node"] == node and e["type"] == event_type
            and all(e.get(k) == v for k, v in fields.items())
        ]
        assert len(matches) == 1, (time, node, event_type, fields)
        return matches[0]["seq"]

    joint_a = seq_of(160, "a", "configurationApplied", index=3)
    snap_a = seq_of(160, "a", "snapshotCreated", lastIncludedIndex=3)
    assert joint_a < snap_a
    install_b = seq_of(745, "b", "snapshotInstalled", lastIncludedIndex=9)
    assert snap_a < install_b

    # After b's restart the only applied client command is x6 (index 10,
    # arriving after the installed snapshot); d applies nothing at all after
    # its restart, and neither node re-applies a configuration or creates
    # another snapshot (both configuration entries already sit compacted in
    # the recovered snapshot).
    assert [
        (e["time"], e["index"], e["id"])
        for e in _events(result, "applied")
        if e["node"] == "b" and e["time"] >= 700
    ] == [(755, 10, "x6")]
    for event_type in ("applied", "configurationApplied", "snapshotCreated"):
        assert not [
            e for e in _events(result, event_type)
            if e["node"] == "d" and e["time"] >= 650
        ]
    # Every node applied each of the four configuration entries exactly once.
    for node in result["nodes"]:
        config_events = [
            e for e in _events(result, "configurationApplied") if e["node"] == node
        ]
        assert [(e["index"], e["entryType"]) for e in config_events] == [
            (3, "joint"), (4, "stable"), (7, "joint"), (8, "stable")
        ]

    # d restarted from its persisted snapshot holding the latest stable
    # configuration {a,b,c,e}; b installed that same snapshot. Both compacted
    # prefixes cover indices 1..9 once each, with both config changes inside.
    for name, st in result["nodes"].items():
        assert st["snapshot"] == {"lastIncludedIndex": 9, "lastIncludedTerm": 1}
        applied_sig = [(e["index"], e.get("kind", "command"),
                        e.get("entryType"), e["id"]) for e in st["applied"]]
        assert applied_sig == [
            (1, "command", None, "x1"), (2, "command", None, "x2"),
            (3, "config", "joint", "add-e"), (4, "config", "stable", "add-e"),
            (5, "command", None, "x3"), (6, "command", None, "x4"),
            (7, "config", "joint", "rm-d"), (8, "config", "stable", "rm-d"),
            (9, "command", None, "x5"), (10, "command", None, "x6"),
        ]
        assert st["lastApplied"] == 10
    assert result["nodes"]["b"]["restartCount"] == 1
    assert result["nodes"]["d"]["restartCount"] == 1
    assert {n: st["membershipRole"] for n, st in result["nodes"].items()} == {
        "a": "voter", "b": "voter", "c": "voter", "e": "voter",
        "d": "learner", "f": "learner",
    }

    # Membership summary: the removal and the addition are both committed and
    # the cluster is no longer in a joint phase.
    assert result["membership"]["initial"] == ["a", "b", "c", "d"]
    assert result["membership"]["current"] == ["a", "b", "c", "e"]
    assert result["membership"]["joint"] is None
    assert {c["id"]: (c["outcome"], c["index"], c["term"])
            for c in result["membership"]["changes"]} == {
        "add-e": ("committed", 4, 1),
        "rm-d": ("committed", 8, 1),
    }


def test_elections_after_recovery_count_only_current_voters(tmp_path, capsys):
    result, _ = _simulate_ok(tmp_path, capsys, _combined_scenario())

    # c campaigns after the first partition (its timeout is 200ms and its
    # last leader contact was ~895) and retries twice, never winning.
    assert [
        (e["time"], e["node"], e["term"], e["role"])
        for e in _events(result, "stateChange") if e["role"] == "candidate"
    ] == [
        (80, "a", 1, "candidate"),
        (1095, "c", 2, "candidate"),
        (1295, "c", 3, "candidate"),
        (1495, "c", 4, "candidate"),
    ]

    # Every post-recovery RequestVote addresses only the current stable
    # voters {a,b,c,e}: the removed d and the never-admitted f are neither
    # solicited nor able to contribute a vote.
    rv_sends = [
        e for e in _events(result, "messageSend")
        if e["message"] == "requestVote"
    ]
    assert {e["peer"] for e in rv_sends if e["time"] >= 1095} <= {"a", "b", "e"}
    # f is a learner for the whole run and is never asked for a vote; d is
    # asked only during the initial election while still a voter and never
    # again after the removal's stable configuration applied at 395.
    assert not [e for e in rv_sends if e["peer"] == "f"]
    assert not [e for e in rv_sends if e["peer"] == "d" and e["time"] > 395]

    # a and b are across the partition, so only e's vote reaches c; one vote
    # is short of the strict majority (2 of 3 excluding c itself) and c never
    # becomes leader in terms 2..4.
    counted = [
        e for e in _events(result, "messageResult")
        if e.get("message") == "voteReply" and e["time"] >= 1095
    ]
    assert counted
    assert all(e["peer"] == "e" and e["detail"] == "voteCounted" for e in counted)
    assert result["electionSafety"]["leadersByTerm"] == {"1": ["a"]}
    assert result["nodes"]["a"]["role"] == "leader"
    assert result["nodes"]["a"]["term"] == 1
    assert result["nodes"]["c"]["role"] == "candidate"
    assert result["nodes"]["c"]["term"] == 4

    # The removed d and the never-admitted learner f never become candidates
    # or start an election timer, even after d's crash/restart. The only
    # timeout events in the run belong to voters before losing eligibility
    # (d's last timeout is while it is still a voter).
    assert not [
        e for e in _events(result, "stateChange")
        if e["node"] in ("d", "f") and e["role"] in ("candidate", "preCandidate")
    ]
    assert not [
        e for e in _events(result, "timeout")
        if e["node"] in ("d", "f") and e["time"] > 395
    ]


def test_commits_need_the_current_configuration_majority(tmp_path, capsys):
    result, _ = _simulate_ok(tmp_path, capsys, _combined_scenario())

    # x7 is accepted at index 11 after the second partition and lands on b,
    # the removed voter d and the never-admitted learner f — an old-config
    # majority ({a,b,d} of {a,b,c,d}) but only two of the current voters
    # {a,b,c,e}.
    accepted = [e for e in _events(result, "clientResult") if e["id"] == "x7"]
    assert [(e["time"], e["result"], e["index"]) for e in accepted] == [
        (1200, "accepted", 11)
    ]
    appends = [
        e for e in _events(result, "messageResult")
        if e.get("message") == "appendEntries" and e["time"] >= 1200
    ]
    assert {
        (e["node"], e["result"], e.get("reason")) for e in appends
    } == {
        ("b", "delivered", None),
        ("d", "delivered", None),
        ("f", "delivered", None),
        ("c", "dropped", "partition"),
        ("e", "dropped", "partition"),
    }

    # a's commit pointer never passes 10 and no commitAdvance follows x7;
    # x7 stays pending and is neither committed nor superseded.
    assert not [
        e for e in _events(result, "commitAdvance")
        if e["node"] == "a" and e["commitIndex"] > 10
    ]
    assert result["nodes"]["a"]["commitIndex"] == 10
    assert result["nodes"]["b"]["commitIndex"] == 10
    clients = result["clients"]
    assert [(c["id"], c["index"], c["term"]) for c in clients["committed"]] == [
        ("x1", 1, 1), ("x2", 2, 1), ("x3", 5, 1), ("x4", 6, 1),
        ("x5", 9, 1), ("x6", 10, 1),
    ]
    assert clients["pending"] == [
        {"id": "x7", "node": "a", "index": 11, "term": 1}
    ]
    assert clients["superseded"] == []
    assert clients["rejected"] == []

    _assert_core_invariants_clean(result, reads_enabled=False)


# -- joint-phase read queries across the snapshot boundary ------------------


def test_joint_phase_reads_require_both_majorities_and_exclude_configs(
    tmp_path, capsys
):
    result, _ = _simulate_ok(tmp_path, capsys, _joint_reads_scenario())

    # Configuration application and snapshot ordering: on a the joint entry
    # is applied and immediately compacted at t=160, before any read result.
    assert [
        (e["time"], e["node"], e["index"], e["entryType"])
        for e in _events(result, "configurationApplied")
    ] == [
        (160, "a", 3, "joint"),
        (165, "b", 3, "joint"), (165, "c", 3, "joint"),
        (165, "d", 3, "joint"), (165, "e", 3, "joint"),
        (170, "a", 4, "stable"),
        (195, "b", 4, "stable"), (195, "c", 4, "stable"),
        (195, "d", 4, "stable"), (195, "e", 4, "stable"),
    ]
    assert [
        (e["time"], e["node"], e["lastIncludedIndex"])
        for e in _events(result, "snapshotCreated")
    ] == [
        (160, "a", 3), (165, "b", 3), (165, "c", 3), (165, "d", 3), (165, "e", 3),
        (261, "a", 6), (295, "b", 6), (295, "c", 6), (295, "d", 6), (295, "e", 6),
    ]
    assert _events(result, "snapshotInstalled", "nodeLifecycle") == []

    # Read results in order: r-bad is accepted and never completes; r-good
    # completes while the joint configuration is in force; r-late completes
    # after x3/x4 and the index-6 snapshot.
    assert [
        (e["time"], e["id"], e["result"])
        for e in _events(result, "readResult")
    ] == [
        (162, "r-bad", "accepted"),
        (163, "r-good", "accepted"),
        (173, "r-good", "completed"),
        (300, "r-late", "accepted"),
        (310, "r-late", "completed"),
    ]

    # The reads straddle the configuration timeline explicitly: both are
    # accepted after the joint entry applied (and was snapshotted) on a and
    # before its stable entry applies. r-good completes at 173, *after* a has
    # already applied the stable entry at 170: a read pins the configuration
    # recorded at acceptance, so it is still decided under the joint
    # configuration and must collect majorities of both sets. r-bad has only the
    # old-set majority and stays pending even though the cluster moved on, which
    # is what makes the joint-quorum requirement observable.
    def read_seq(query_id, result_kind):
        matches = [
            e["seq"] for e in result["timeline"]
            if e["type"] == "readResult" and e["id"] == query_id
            and e["result"] == result_kind
        ]
        assert len(matches) == 1
        return matches[0]

    joint_a = next(
        e["seq"] for e in result["timeline"]
        if e["type"] == "configurationApplied" and e["node"] == "a"
        and e["entryType"] == "joint"
    )
    stable_a = next(
        e["seq"] for e in result["timeline"]
        if e["type"] == "configurationApplied" and e["node"] == "a"
        and e["entryType"] == "stable"
    )
    assert joint_a < read_seq("r-bad", "accepted") < stable_a
    assert joint_a < read_seq("r-good", "accepted") < stable_a
    assert stable_a < read_seq("r-good", "completed")

    # The two first-occurrence probe drops belong to r-bad only; r-good's
    # probes to c and d are delivered and carry it over both majorities.
    assert [
        (e["time"], e["from"], e["to"], e["message"], e["occurrence"])
        for e in _events(result, "messageFault")
    ] == [
        (162, "a", "c", "readProbe", 1),
        (162, "a", "d", "readProbe", 1),
    ]
    drops = [
        e for e in _events(result, "messageResult")
        if e.get("reason") == "messageFault"
    ]
    assert [(e["time"], e["node"], e["id"]) for e in drops] == [
        (167, "c", "r-bad"), (167, "d", "r-bad"),
    ]

    # Final read outcomes: the joint read missing new-set quorum stays
    # pending; both completed reads return exactly the client-command prefix
    # of their readIndex — compacted commands included, config entries never.
    reads = {read["id"]: read for read in result["reads"]}
    assert reads["r-bad"] == {
        "id": "r-bad", "node": "a", "outcome": "pending",
        "term": 1, "readIndex": 3,
    }
    assert reads["r-good"] == {
        "id": "r-good", "node": "a", "outcome": "completed",
        "term": 1, "readIndex": 3,
        "state": [
            {"index": 1, "term": 1, "id": "x1", "command": "one"},
            {"index": 2, "term": 1, "id": "x2", "command": "two"},
        ],
    }
    assert reads["r-late"] == {
        "id": "r-late", "node": "a", "outcome": "completed",
        "term": 1, "readIndex": 6,
        "state": [
            {"index": 1, "term": 1, "id": "x1", "command": "one"},
            {"index": 2, "term": 1, "id": "x2", "command": "two"},
            {"index": 5, "term": 1, "id": "x3", "command": "three"},
            {"index": 6, "term": 1, "id": "x4", "command": "four"},
        ],
    }

    # Probes always fan out to every other node; acks from learner e are
    # recorded but never count towards either quorum.
    assert {
        (e["id"], e["peer"])
        for e in _events(result, "messageSend") if e["message"] == "readProbe"
    } == {
        ("r-bad", "b"), ("r-bad", "c"), ("r-bad", "d"), ("r-bad", "e"),
        ("r-good", "b"), ("r-good", "c"), ("r-good", "d"), ("r-good", "e"),
        ("r-late", "b"), ("r-late", "c"), ("r-late", "d"), ("r-late", "e"),
    }
    assert result["membership"]["current"] == ["a", "b", "c", "d"]
    _assert_core_invariants_clean(result, reads_enabled=True)


# -- determinism, replay and name-mapping invariance ------------------------


def test_combined_runs_are_byte_identical_and_replay_matched(tmp_path, capsys):
    for factory in (_combined_scenario, _joint_reads_scenario):
        scenario = factory()
        _, out1 = _simulate_ok(tmp_path, capsys, scenario, name="first.json")
        _, out2 = _simulate_ok(tmp_path, capsys, scenario, name="second.json")
        assert out2 == out1

        scenario_path = _write(tmp_path, "replay_scenario.json", scenario)
        result_path = _write(
            tmp_path, "replay_result.json", json.loads(out1)
        )
        code = main(["replay", scenario_path, result_path])
        out, err = capsys.readouterr()
        assert code == 0
        assert err == ""
        assert out == '{"status":"matched"}\n'


def test_renaming_nodes_preserves_times_seq_and_outcome(tmp_path, capsys):
    for factory in (_combined_scenario, _joint_reads_scenario):
        original, _ = _simulate_ok(
            tmp_path, capsys, factory(), name="original.json"
        )
        renamed_input = _rename_tree(factory())
        renamed, _ = _simulate_ok(
            tmp_path, capsys, renamed_input, name="renamed.json"
        )

        # Event times, seq numbers and types are untouched by the renaming.
        assert [
            (e["time"], e["seq"], e["type"]) for e in renamed["timeline"]
        ] == [
            (e["time"], e["seq"], e["type"]) for e in original["timeline"]
        ]
        # Mapping the original names onto the new ones reproduces the whole
        # result JSON, including the node-keyed final states and reports.
        assert _rename_tree(copy.deepcopy(original)) == renamed


# -- public behavior of the other entry points stays intact -----------------


def test_version_still_prints(capsys):
    code = main(["version"])
    out, err = capsys.readouterr()
    assert code == 0
    assert err == ""
    assert out.count("\n") == 1
    assert out.strip()  # some non-empty version string


def test_explore_over_combined_base_is_stable(tmp_path, capsys):
    # Enumerate one benign message fault (dropping a heartbeat to the
    # learner that is never promoted): both combinations must stay free of
    # invariant violations and the explore output must be deterministic.
    scenario = _joint_reads_scenario()
    scenario.pop("messageFaults")
    plan = {
        "scenario": scenario,
        "candidates": [
            {"from": "a", "to": "e", "message": "heartbeat",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 1,
        "maxCases": 10,
    }
    path = _write(tmp_path, "plan.json", plan)
    code = main(["explore", path])
    out1, err = capsys.readouterr()
    assert code == 0
    assert err == ""
    summary1 = json.loads(out1)
    assert summary1["totalCases"] == 2
    assert summary1["passedCases"] == 2
    assert summary1["failedCases"] == 0
    for case in summary1["cases"]:
        case_result = case["result"]
        assert case["status"] == "passed"
        assert case_result["electionSafety"]["violations"] == []
        assert case_result["logMatching"]["violations"] == []
        assert case_result["stateMachineSafety"]["violations"] == []
        assert case_result["linearizability"]["violations"] == []

    code = main(["explore", path])
    out2, err = capsys.readouterr()
    assert code == 0
    assert err == ""
    assert out2 == out1


# -- invalid scenarios still fail with one stderr line and exit code 2 ------


def test_invalid_combined_scenarios_fail_cleanly(tmp_path, capsys):
    def partition_missing_node(scenario):
        scenario["faults"][1]["groups"] = [["a", "b", "d"], ["c", "e"]]

    def too_few_initial_voters(scenario):
        scenario["initialMembers"] = ["a", "b"]

    def node_event_not_starting_with_crash(scenario):
        scenario["nodeEvents"].append(
            {"time": 100, "node": "f", "action": "restart"}
        )

    def storage_and_node_events_together(scenario):
        scenario["storageFaults"] = [
            {"node": "a", "occurrence": 1}
        ]

    for mutate in (
        partition_missing_node,
        too_few_initial_voters,
        node_event_not_starting_with_crash,
        storage_and_node_events_together,
    ):
        scenario = _combined_scenario()
        mutate(scenario)
        code, out, err = _simulate(tmp_path, capsys, scenario)
        assert code == 2
        assert out == ""
        assert err.startswith("error: ")
        assert err.count("\n") == 1
