"""Negative tests for the safety-invariant discriminators in the report layer.

The public protocol flow can never itself produce a state that violates
election safety, log matching or state-machine safety, so these tests do not
drive the simulator through a scenario. Instead they hand the *existing*
:func:`consensus_lab.report.build_report` a minimal, structurally legal
completed state: a simulator built from a legal parsed scenario (duration 0,
so running it would process no event) whose per-node final state is seeded
with exactly the shapes the production protocol writes (see
``_command_entry``, ``_append_config_entry``/``_applied_config_view`` and
``_create_snapshot``/``installSnapshot``). Nothing in the production
protocol, scenario validation or report generation is altered or bypassed;
these tests only seed terminal state and read the report projection back.
"""

from __future__ import annotations

import json

from consensus_lab.node import (
    CONFIG_JOINT,
    CONFIG_STABLE,
    KIND_COMMAND,
    KIND_CONFIG,
    ROLE_LEADER,
    ROLE_PRECANDIDATE,
    _Node,
)
from consensus_lab.report import build_report
from consensus_lab.scenario import parse_scenario
from consensus_lab.simulate import _Simulator


# -- construction helpers -----------------------------------------------------


def _scenario(*, membership: bool = False) -> dict:
    raw = {
        "nodes": ["a", "b", "c"],
        "duration": 0,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 10,
    }
    if membership:
        raw["initialMembers"] = ["a", "b", "c"]
        raw["membershipChanges"] = []
    return parse_scenario(raw)


def _sim(membership: bool = False) -> _Simulator:
    # Built from a legal scenario; the zero duration means run() would process
    # no event. We never call run() — these tests inspect the read-only report
    # projection over seeded final state.
    return _Simulator(_scenario(membership=membership))


def _cmd(index: int, term: int, command_id: str, command: object) -> dict:
    """A client-command log/applied entry in the typed shape the protocol
    writes in membership mode (``_command_entry`` adds ``kind: "command"``)."""
    return {
        "index": index,
        "term": term,
        "kind": KIND_COMMAND,
        "id": command_id,
        "command": command,
    }


def _cfg(
    index: int,
    term: int,
    change_id: str,
    entry_type: str,
    old: list[str],
    new: list[str],
    *,
    action: str = "add",
    member: str = "d",
) -> dict:
    """A configuration entry in the exact shape ``_append_config_entry``
    writes / ``_applied_config_view`` reflects, with the config normalized the
    way ``_store_config`` normalizes it."""
    return {
        "index": index,
        "term": term,
        "kind": KIND_CONFIG,
        "id": change_id,
        "entryType": entry_type,
        "config": {"type": entry_type, "old": list(old), "new": list(new)},
        "action": action,
        "member": member,
    }


def _commit(st: _Node, entries: list[dict]) -> None:
    """Install committed entries: held in the uncompacted log and present in
    the applied history, with commit/apply advanced to the end — the normal
    terminal shape of a node that has not compacted."""
    st.log = [dict(e) for e in entries]
    st.applied = [dict(e) for e in entries]
    st.commit_index = len(entries)
    st.last_applied = len(entries)


def _snapshot(sim: _Simulator, name: str, prefix: list[dict], suffix: list[dict],
              *, snapshot_term: int, snapshot_config=None) -> None:
    """Put a node in the terminal state of having created/installed a
    snapshot covering ``prefix`` (global indices 1..len(prefix)) and holding
    ``suffix`` uncompacted in its log. A real run keeps the full 1..N applied
    history, so it is reconstructed here as well."""
    st = sim.state[name]
    n_prefix = len(prefix)
    st.snapshot_index = n_prefix
    st.snapshot_term = snapshot_term
    st.snapshot_config = snapshot_config
    st.log = [dict(e) for e in suffix]
    st.applied = [dict(e) for e in prefix] + [dict(e) for e in suffix]
    st.last_applied = len(st.applied)
    st.commit_index = len(st.applied)


def _assert_rebuild_stable(sim: _Simulator, select) -> None:
    """Building the report twice over the same terminal state yields the exact
    same JSON-observable structure (the report is a pure, deterministic
    projection)."""
    first = json.dumps(select(build_report(sim)), sort_keys=True)
    second = json.dumps(select(build_report(sim)), sort_keys=True)
    assert first == second


def _safety_slice(report: dict) -> dict:
    return {
        "electionSafety": report["electionSafety"],
        "logMatching": report["logMatching"],
        "stateMachineSafety": report["stateMachineSafety"],
    }


# -- election safety ----------------------------------------------------------


def test_election_safety_two_formal_leaders_same_term_is_one_violation():
    sim = _sim()
    # Two nodes both believe they lead term 2 (a split-brain terminal state the
    # correct protocol never reaches); the discriminator must report exactly
    # one violation for that term.
    sim.leaders_by_term = {2: ["a", "b"]}
    for name in ("a", "b"):
        st = sim.state[name]
        st.role = ROLE_LEADER
        st.term = 2

    es = build_report(sim)["electionSafety"]
    assert es["leadersByTerm"] == {"2": ["a", "b"]}
    assert es["violations"] == [{"term": 2, "leaders": ["a", "b"]}]
    assert len(es["violations"]) == 1

    _assert_rebuild_stable(sim, lambda r: r["electionSafety"])


def test_election_safety_deterministic_term_and_node_order():
    sim = _sim()
    # Seed terms out of order and leaders in non-sorted order; the report must
    # present terms ascending and keep each term's leader list in the
    # deterministic recorded order (no resorting of node names).
    sim.leaders_by_term = {3: ["c", "a"], 1: ["b"], 2: ["a", "c", "b"]}

    es = build_report(sim)["electionSafety"]
    assert list(es["leadersByTerm"]) == ["1", "2", "3"]
    assert es["leadersByTerm"] == {
        "1": ["b"],
        "2": ["a", "c", "b"],
        "3": ["c", "a"],
    }
    assert es["violations"] == [
        {"term": 2, "leaders": ["a", "c", "b"]},
        {"term": 3, "leaders": ["c", "a"]},
    ]
    _assert_rebuild_stable(sim, lambda r: r["electionSafety"])


def test_election_safety_one_leader_per_term_is_clean():
    sim = _sim()
    # A different legitimate leader in each successive term is legal.
    sim.leaders_by_term = {1: ["a"], 2: ["b"], 3: ["c"]}

    es = build_report(sim)["electionSafety"]
    assert es["leadersByTerm"] == {"1": ["a"], "2": ["b"], "3": ["c"]}
    assert es["violations"] == []
    _assert_rebuild_stable(sim, lambda r: r["electionSafety"])


def test_election_safety_precandidate_is_not_counted_as_leader():
    sim = _sim()
    # b is only campaigning in the pre-vote phase; it must not appear as a
    # term-3 leader even though a is the sole formal leader of term 2.
    sim.leaders_by_term = {1: ["a"], 2: ["a"]}
    sim.state["b"].role = ROLE_PRECANDIDATE
    sim.state["b"].term = 2
    sim.state["b"].pre_term = 3

    report = build_report(sim)
    es = report["electionSafety"]
    assert es["leadersByTerm"] == {"1": ["a"], "2": ["a"]}
    assert es["violations"] == []
    # The preCandidate role is observable in the node projection but never
    # enters leadersByTerm.
    assert report["nodes"]["b"]["role"] == ROLE_PRECANDIDATE
    _assert_rebuild_stable(sim, lambda r: r["electionSafety"])


# -- log matching -------------------------------------------------------------


def test_log_matching_same_content_is_clean():
    sim = _sim(membership=True)
    entries = [
        _cmd(1, 1, "x1", {"k": "v"}),
        _cfg(2, 1, "m1", CONFIG_STABLE, ["a", "b", "c"], ["a", "b", "c"]),
    ]
    for name in ("a", "b", "c"):
        sim.state[name].log = [dict(e) for e in entries]

    assert build_report(sim)["logMatching"]["violations"] == []
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_missing_index_on_a_node_is_not_a_violation():
    sim = _sim(membership=True)
    # a and b hold index 2; c has simply not received that entry yet.
    sim.state["a"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")]
    sim.state["b"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")]
    sim.state["c"].log = [_cmd(1, 1, "x1", "one")]

    assert build_report(sim)["logMatching"]["violations"] == []
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_different_term_at_same_index_is_not_a_violation():
    sim = _sim(membership=True)
    # Divergent entries at global index 2 but carried by *different* terms are
    # not a same-index/same-term log-match break.
    sim.state["a"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "left", "L")]
    sim.state["b"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 2, "right", "R")]
    sim.state["c"].log = [_cmd(1, 1, "x1", "one")]

    assert build_report(sim)["logMatching"]["violations"] == []
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_command_id_differs_same_index_term():
    sim = _sim(membership=True)
    sim.state["a"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "alpha", "v")]
    sim.state["b"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "beta", "v")]
    sim.state["c"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "alpha", "v")]

    v = build_report(sim)["logMatching"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 2
    assert v[0]["term"] == 1
    # Variants are ordered by their deterministic content marker; each node
    # list keeps the declared node iteration order (a, c) then (b).
    assert v[0]["variants"] == [
        {"id": "alpha", "command": "v", "nodes": ["a", "c"]},
        {"id": "beta", "command": "v", "nodes": ["b"]},
    ]
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_command_payload_differs_same_index_term():
    sim = _sim(membership=True)
    sim.state["a"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "same", {"v": 1})]
    sim.state["b"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "same", {"v": 2})]
    sim.state["c"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "same", {"v": 1})]

    v = build_report(sim)["logMatching"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 2
    assert v[0]["term"] == 1
    assert v[0]["variants"] == [
        {"id": "same", "command": {"v": 1}, "nodes": ["a", "c"]},
        {"id": "same", "command": {"v": 2}, "nodes": ["b"]},
    ]
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_joint_config_payload_differs():
    sim = _sim(membership=True)
    joint_wide = _cfg(2, 1, "m1", CONFIG_JOINT, ["a", "b", "c"], ["a", "b", "c", "d"])
    joint_narrow = _cfg(2, 1, "m1", CONFIG_JOINT, ["a", "b", "c"], ["a", "b"])
    sim.state["a"].log = [_cmd(1, 1, "x1", "one"), joint_wide]
    sim.state["b"].log = [_cmd(1, 1, "x1", "one"), joint_narrow]
    sim.state["c"].log = [_cmd(1, 1, "x1", "one"), joint_wide]

    v = build_report(sim)["logMatching"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 2
    assert v[0]["term"] == 1
    assert v[0]["variants"] == [
        {
            "id": "m1",
            "kind": KIND_CONFIG,
            "entryType": CONFIG_JOINT,
            "config": {"type": CONFIG_JOINT, "old": ["a", "b", "c"],
                       "new": ["a", "b", "c", "d"]},
            "nodes": ["a", "c"],
        },
        {
            "id": "m1",
            "kind": KIND_CONFIG,
            "entryType": CONFIG_JOINT,
            "config": {"type": CONFIG_JOINT, "old": ["a", "b", "c"], "new": ["a", "b"]},
            "nodes": ["b"],
        },
    ]
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_stable_config_payload_differs():
    sim = _sim(membership=True)
    stable_d = _cfg(3, 2, "m1", CONFIG_STABLE, ["a", "b", "c"], ["a", "b", "c", "d"])
    stable_e = _cfg(3, 2, "m1", CONFIG_STABLE, ["a", "b", "c"], ["a", "b", "c", "e"])
    sim.state["a"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two"), stable_d]
    sim.state["b"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two"), stable_e]
    sim.state["c"].log = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two"), stable_d]

    v = build_report(sim)["logMatching"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 3
    assert v[0]["term"] == 2
    assert all(variant["kind"] == KIND_CONFIG for variant in v[0]["variants"])
    assert v[0]["variants"] == [
        {
            "id": "m1",
            "kind": KIND_CONFIG,
            "entryType": CONFIG_STABLE,
            "config": {"type": CONFIG_STABLE, "old": ["a", "b", "c"],
                       "new": ["a", "b", "c", "d"]},
            "nodes": ["a", "c"],
        },
        {
            "id": "m1",
            "kind": KIND_CONFIG,
            "entryType": CONFIG_STABLE,
            "config": {"type": CONFIG_STABLE, "old": ["a", "b", "c"],
                       "new": ["a", "b", "c", "e"]},
            "nodes": ["b"],
        },
    ]
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_multiple_indices_reported_in_index_order():
    sim = _sim(membership=True)
    # Conflicts at indices 2 and 4 surface in ascending index order and keep
    # their own (index, term) identity.
    sim.state["a"].log = [
        _cmd(1, 1, "x1", "one"), _cmd(2, 1, "p", 1),
        _cmd(3, 1, "x3", "three"), _cmd(4, 2, "q", 1),
    ]
    sim.state["b"].log = [
        _cmd(1, 1, "x1", "one"), _cmd(2, 1, "r", 2),
        _cmd(3, 1, "x3", "three"), _cmd(4, 2, "s", 2),
    ]
    sim.state["c"].log = [
        _cmd(1, 1, "x1", "one"), _cmd(2, 1, "p", 1),
        _cmd(3, 1, "x3", "three"), _cmd(4, 2, "q", 1),
    ]

    v = build_report(sim)["logMatching"]["violations"]
    assert [(item["index"], item["term"]) for item in v] == [(2, 1), (4, 2)]
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_across_snapshot_boundary_matches():
    sim = _sim(membership=True)
    # a folded index 2 into its snapshot (held in applied history); b/c still
    # hold it in the uncompacted log. Same (term, id, command) must not fire.
    _snapshot(
        sim, "a",
        prefix=[_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")],
        suffix=[_cmd(3, 1, "x3", "three")],
        snapshot_term=1,
    )
    for name in ("b", "c"):
        sim.state[name].log = [
            _cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two"), _cmd(3, 1, "x3", "three"),
        ]

    assert build_report(sim)["logMatching"]["violations"] == []
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_across_snapshot_boundary_detects_conflict():
    sim = _sim(membership=True)
    # a's compacted/applied index 2 differs from b/c's uncompacted log entry at
    # the same index and term: still exactly one violation.
    _snapshot(
        sim, "a",
        prefix=[_cmd(1, 1, "x1", "one"), _cmd(2, 1, "applied-side", "L")],
        suffix=[_cmd(3, 1, "x3", "three")],
        snapshot_term=1,
    )
    sim.state["b"].log = [
        _cmd(1, 1, "x1", "one"), _cmd(2, 1, "log-side", "R"), _cmd(3, 1, "x3", "three"),
    ]
    sim.state["c"].log = [
        _cmd(1, 1, "x1", "one"), _cmd(2, 1, "log-side", "R"), _cmd(3, 1, "x3", "three"),
    ]

    v = build_report(sim)["logMatching"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 2
    assert v[0]["term"] == 1
    assert v[0]["variants"] == [
        {"id": "applied-side", "command": "L", "nodes": ["a"]},
        {"id": "log-side", "command": "R", "nodes": ["b", "c"]},
    ]
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


def test_log_matching_across_snapshot_boundary_config_conflict():
    sim = _sim(membership=True)
    joint_wide = _cfg(2, 1, "m1", CONFIG_JOINT, ["a", "b", "c"], ["a", "b", "c", "d"])
    joint_narrow = _cfg(2, 1, "m1", CONFIG_JOINT, ["a", "b", "c"], ["a", "b"])
    # a folded the joint entry into its snapshot/applied history; b/c hold it
    # (divergently for b) in the uncompacted log.
    _snapshot(
        sim, "a",
        prefix=[_cmd(1, 1, "x1", "one"), joint_wide],
        suffix=[_cmd(3, 1, "x3", "three")],
        snapshot_term=1,
        snapshot_config=joint_wide["config"],
    )
    sim.state["b"].log = [_cmd(1, 1, "x1", "one"), joint_narrow, _cmd(3, 1, "x3", "three")]
    sim.state["c"].log = [_cmd(1, 1, "x1", "one"), joint_wide, _cmd(3, 1, "x3", "three")]

    v = build_report(sim)["logMatching"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 2
    assert v[0]["term"] == 1
    assert v[0]["variants"] == [
        {
            "id": "m1",
            "kind": KIND_CONFIG,
            "entryType": CONFIG_JOINT,
            "config": {"type": CONFIG_JOINT, "old": ["a", "b", "c"],
                       "new": ["a", "b", "c", "d"]},
            "nodes": ["a", "c"],
        },
        {
            "id": "m1",
            "kind": KIND_CONFIG,
            "entryType": CONFIG_JOINT,
            "config": {"type": CONFIG_JOINT, "old": ["a", "b", "c"], "new": ["a", "b"]},
            "nodes": ["b"],
        },
    ]
    _assert_rebuild_stable(sim, lambda r: r["logMatching"])


# -- state machine safety -----------------------------------------------------


def test_state_machine_safety_same_content_is_clean():
    sim = _sim(membership=True)
    history = [
        _cmd(1, 1, "x1", "one"),
        _cfg(2, 1, "m1", CONFIG_STABLE, ["a", "b", "c"], ["a", "b", "c"]),
        _cmd(3, 1, "x3", "three"),
    ]
    for name in ("a", "b", "c"):
        _commit(sim.state[name], history)

    assert build_report(sim)["stateMachineSafety"]["violations"] == []
    _assert_rebuild_stable(sim, lambda r: r["stateMachineSafety"])


def test_state_machine_safety_node_not_yet_applied_is_clean():
    sim = _sim(membership=True)
    full = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")]
    _commit(sim.state["a"], full)
    _commit(sim.state["b"], full)
    # c has applied (and holds) only index 1: lag, not divergence.
    _commit(sim.state["c"], [_cmd(1, 1, "x1", "one")])

    assert build_report(sim)["stateMachineSafety"]["violations"] == []
    _assert_rebuild_stable(sim, lambda r: r["stateMachineSafety"])


def test_state_machine_safety_different_commands_same_index():
    sim = _sim(membership=True)
    _commit(sim.state["a"], [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "alpha", {"v": 1})])
    _commit(sim.state["b"], [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "beta", {"v": 9})])
    _commit(sim.state["c"], [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "alpha", {"v": 1})])

    v = build_report(sim)["stateMachineSafety"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 2
    assert v[0]["variants"] == [
        {"term": 1, "id": "alpha", "command": {"v": 1}, "nodes": ["a", "c"]},
        {"term": 1, "id": "beta", "command": {"v": 9}, "nodes": ["b"]},
    ]
    _assert_rebuild_stable(sim, lambda r: r["stateMachineSafety"])


def test_state_machine_safety_command_vs_config_same_index():
    sim = _sim(membership=True)
    cfg = _cfg(2, 1, "m1", CONFIG_STABLE, ["a", "b", "c"], ["a", "b", "c", "d"])
    _commit(sim.state["a"], [_cmd(1, 1, "x1", "one"), cfg])
    _commit(sim.state["b"], [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")])
    _commit(sim.state["c"], [_cmd(1, 1, "x1", "one"), cfg])

    v = build_report(sim)["stateMachineSafety"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 2
    # Both buckets are present and complete; the content marker orders the
    # config variant ("config") before the command variant ("two").
    assert v[0]["variants"] == [
        {
            "term": 1,
            "id": "m1",
            "kind": KIND_CONFIG,
            "entryType": CONFIG_STABLE,
            "config": {"type": CONFIG_STABLE, "old": ["a", "b", "c"],
                       "new": ["a", "b", "c", "d"]},
            "nodes": ["a", "c"],
        },
        {"term": 1, "id": "x2", "command": "two", "nodes": ["b"]},
    ]
    _assert_rebuild_stable(sim, lambda r: r["stateMachineSafety"])


def test_state_machine_safety_conflict_in_compacted_history():
    sim = _sim(membership=True)
    # a folded the divergent index 1 into its snapshot (kept in applied
    # history); b/c applied a different command there, and all three have
    # since advanced well past it.
    _snapshot(
        sim, "a",
        prefix=[_cmd(1, 1, "old-cmd", "L"), _cmd(2, 1, "x2", "two")],
        suffix=[_cmd(3, 1, "x3", "three"), _cmd(4, 1, "x4", "four")],
        snapshot_term=1,
    )
    tail = [
        _cmd(1, 1, "new-cmd", "R"), _cmd(2, 1, "x2", "two"),
        _cmd(3, 1, "x3", "three"), _cmd(4, 1, "x4", "four"),
    ]
    _commit(sim.state["b"], tail)
    _commit(sim.state["c"], tail)

    v = build_report(sim)["stateMachineSafety"]["violations"]
    assert len(v) == 1
    assert v[0]["index"] == 1
    assert v[0]["variants"] == [
        {"term": 1, "id": "new-cmd", "command": "R", "nodes": ["b", "c"]},
        {"term": 1, "id": "old-cmd", "command": "L", "nodes": ["a"]},
    ]
    _assert_rebuild_stable(sim, lambda r: r["stateMachineSafety"])


def test_state_machine_safety_multiple_divergent_indices_in_order():
    sim = _sim(membership=True)
    _commit(sim.state["a"], [_cmd(1, 1, "a1", 1), _cmd(2, 1, "x2", 2), _cmd(3, 1, "a3", 3)])
    _commit(sim.state["b"], [_cmd(1, 1, "b1", 1), _cmd(2, 1, "x2", 2), _cmd(3, 1, "b3", 3)])
    _commit(sim.state["c"], [_cmd(1, 1, "a1", 1), _cmd(2, 1, "x2", 2), _cmd(3, 1, "a3", 3)])

    v = build_report(sim)["stateMachineSafety"]["violations"]
    assert [item["index"] for item in v] == [1, 3]
    assert v[0]["variants"][0]["nodes"] == ["a", "c"]
    assert v[1]["variants"][0]["nodes"] == ["a", "c"]
    _assert_rebuild_stable(sim, lambda r: r["stateMachineSafety"])


# -- determinism over all three discriminators at once ------------------------


def test_all_three_reports_rebuild_identically():
    sim = _sim(membership=True)
    # An election-safety split AND a log/state-machine divergence at once.
    sim.leaders_by_term = {1: ["a"], 2: ["a", "b"]}
    sim.state["a"].role = ROLE_LEADER
    sim.state["a"].term = 2
    sim.state["b"].role = ROLE_LEADER
    sim.state["b"].term = 2
    _commit(sim.state["a"], [_cmd(1, 1, "x1", "one"), _cmd(2, 2, "left", "L")])
    _commit(sim.state["b"], [_cmd(1, 1, "x1", "one"), _cmd(2, 2, "right", "R")])
    _commit(sim.state["c"], [_cmd(1, 1, "x1", "one")])

    first = json.dumps(_safety_slice(build_report(sim)), sort_keys=True)
    second = json.dumps(_safety_slice(build_report(sim)), sort_keys=True)
    assert first == second

    report = _safety_slice(build_report(sim))
    assert len(report["electionSafety"]["violations"]) == 1
    assert len(report["logMatching"]["violations"]) == 1
    assert len(report["stateMachineSafety"]["violations"]) == 1
