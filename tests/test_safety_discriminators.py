"""Counterexample tests for the safety-invariant discriminators.

The three safety reports (electionSafety, logMatching, stateMachineSafety)
are read-only projections over a finished simulation; the correct public
protocol flow never reaches a violating end state, so the existing tests
only ever pin the empty-violations path. These tests instead construct
minimal, structurally legal *terminal states* — using the exact entry
shapes ``_command_entry`` / ``_append_config_entry`` /
``_applied_config_view`` produce during a real run, installed on a
simulator built from a validated, legal scenario — and hand them to the
unchanged ``build_report``. No production protocol, scenario validation or
report code is modified or bypassed: every expected value is computed by
the report layer itself.

Each test also rebuilds the report twice and asserts the observable JSON
structure is byte-identical.
"""

from __future__ import annotations

import json

from consensus_lab.node import (
    CONFIG_JOINT,
    CONFIG_STABLE,
    KIND_COMMAND,
    KIND_CONFIG,
    ROLE_FOLLOWER,
    ROLE_LEADER,
    ROLE_PRECANDIDATE,
)
from consensus_lab.report import build_report
from consensus_lab.scenario import parse_scenario
from consensus_lab.simulate import _Simulator


# -- fixtures ---------------------------------------------------------------


def _legal_scenario(**overrides):
    """A minimal legal scenario: nothing scheduled at or before time 0, so
    running it leaves every node in its initial state. Membership-enabled
    variants are what typed command/config entries are produced under."""
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 0,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 10,
    }
    scenario.update(overrides)
    return scenario


def _finished_sim():
    # Membership is enabled so entries carry the same typed shape the
    # production append/apply paths emit (kind on commands, full config
    # payloads on configuration entries).
    return _Simulator(
        parse_scenario(
            _legal_scenario(initialMembers=["a", "b", "c"], membershipChanges=[])
        )
    )


def _cmd(index, term, command_id, command):
    """The client-command entry shape _command_entry stores in logs and
    applied histories when membership changes are enabled."""
    return {
        "index": index,
        "term": term,
        "kind": KIND_COMMAND,
        "id": command_id,
        "command": command,
    }


def _cfg(index, term, change_id, entry_type, config, *, action="add", member="d"):
    """The configuration log-entry shape _append_config_entry appends and
    _applied_config_view mirrors into the applied history."""
    return {
        "index": index,
        "term": term,
        "kind": KIND_CONFIG,
        "id": change_id,
        "entryType": entry_type,
        "config": config,
        "action": action,
        "member": member,
    }


def _joint(old, new):
    return {"type": CONFIG_JOINT, "old": list(old), "new": list(new)}


def _stable(voters):
    ordered = list(voters)
    return {"type": CONFIG_STABLE, "old": ordered, "new": list(ordered)}


def _set_log(sim, name, entries):
    st = sim.state[name]
    st.log = [dict(entry) for entry in entries]
    # Global indices start at 1 in every constructed entry.
    st.snapshot_index = 0
    st.snapshot_term = 0


def _set_compacted(sim, name, applied, suffix):
    """A node that snapshotted its applied prefix: ``applied`` is the full
    history and ``suffix`` the surviving uncompacted log. The snapshot
    folds exactly the entries the suffix no longer holds."""
    st = sim.state[name]
    st.applied = [dict(entry) for entry in applied]
    st.last_applied = len(applied)
    st.snapshot_index = len(applied)
    st.snapshot_term = applied[-1]["term"] if applied else 0
    st.log = [dict(entry) for entry in suffix]


def _set_applied(sim, name, applied):
    st = sim.state[name]
    st.applied = [dict(entry) for entry in applied]
    st.last_applied = len(applied)


def _report(sim):
    """Build the report repeatedly: repeated projection of the same terminal
    state must yield the identical observable JSON structure every time."""
    first = build_report(sim)
    second = build_report(sim)
    assert second == first
    assert json.dumps(first, ensure_ascii=False, sort_keys=True) == json.dumps(
        second, ensure_ascii=False, sort_keys=True
    )
    return first


def test_clean_terminal_state_reports_empty_violations():
    # Baseline: the legal fixture, run normally, never trips any discriminator.
    # run() returns a freshly built report; rebuilding over the same finished
    # simulation must yield the identical JSON structure.
    sim = _Simulator(parse_scenario(_legal_scenario()))
    result = sim.run()
    assert result["electionSafety"] == {"leadersByTerm": {}, "violations": []}
    assert result["logMatching"] == {"violations": []}
    assert result["stateMachineSafety"] == {"violations": []}
    assert build_report(sim) == result


# -- election safety --------------------------------------------------------


def test_two_formal_leaders_same_term_is_one_violation():
    sim = _finished_sim()
    # Populated out of term order and with node names out of alphabetic
    # order; only the report layer may impose a deterministic order.
    sim.leaders_by_term = {3: ["c", "a"], 1: ["b"], 5: ["a"]}

    report = _report(sim)
    election = report["electionSafety"]
    # Terms appear in ascending order regardless of insertion order.
    assert list(election["leadersByTerm"]) == ["1", "3", "5"]
    assert election["leadersByTerm"] == {
        "1": ["b"],
        "3": ["c", "a"],
        "5": ["a"],
    }
    # Exactly one violation, for term 3; node order is the leaders' recorded
    # order, not re-sorted.
    assert election["violations"] == [{"term": 3, "leaders": ["c", "a"]}]


def test_one_leader_each_in_different_terms_is_not_a_violation():
    sim = _finished_sim()
    sim.leaders_by_term = {2: ["b"], 1: ["a"], 3: ["c"]}

    report = _report(sim)
    election = report["electionSafety"]
    assert list(election["leadersByTerm"]) == ["1", "2", "3"]
    assert election["leadersByTerm"] == {"1": ["a"], "2": ["b"], "3": ["c"]}
    assert election["violations"] == []


def test_pre_candidate_is_not_counted_as_leader():
    sim = _finished_sim()
    # a is the single formal leader of term 1; b is probing as a
    # preCandidate (pre-vote never bumps its term and never records it in
    # leaders_by_term), c follows.
    sim.state["a"].role = ROLE_LEADER
    sim.state["a"].term = 1
    sim.state["b"].role = ROLE_PRECANDIDATE
    sim.state["b"].term = 1
    sim.state["c"].role = ROLE_FOLLOWER
    sim.state["c"].term = 1
    sim.leaders_by_term = {1: ["a"]}

    report = _report(sim)
    election = report["electionSafety"]
    assert election["leadersByTerm"] == {"1": ["a"]}
    assert election["violations"] == []


# -- log matching -----------------------------------------------------------


def test_same_index_term_different_command_id_violates():
    sim = _finished_sim()
    _set_log(sim, "a", [_cmd(1, 1, "x1", {"k": 1})])
    _set_log(sim, "b", [_cmd(1, 1, "y1", {"k": 1})])
    _set_log(sim, "c", [_cmd(1, 1, "x1", {"k": 1})])

    violations = _report(sim)["logMatching"]["violations"]
    assert violations == [
        {
            "index": 1,
            "term": 1,
            "variants": [
                {"id": "x1", "command": {"k": 1}, "nodes": ["a", "c"]},
                {"id": "y1", "command": {"k": 1}, "nodes": ["b"]},
            ],
        }
    ]


def test_same_id_different_command_payload_violates():
    sim = _finished_sim()
    _set_log(sim, "a", [_cmd(1, 1, "x2", "one")])
    _set_log(sim, "b", [_cmd(1, 1, "x2", "two")])
    _set_log(sim, "c", [_cmd(1, 1, "x2", "one")])

    violations = _report(sim)["logMatching"]["violations"]
    assert violations == [
        {
            "index": 1,
            "term": 1,
            "variants": [
                {"id": "x2", "command": "one", "nodes": ["a", "c"]},
                {"id": "x2", "command": "two", "nodes": ["b"]},
            ],
        }
    ]


def test_different_joint_config_payloads_violate():
    sim = _finished_sim()
    config_a = _joint(["a", "b", "c"], ["a", "b", "c", "d"])
    config_b = _joint(["a", "b", "c"], ["a", "b", "c", "e"])
    _set_log(sim, "a", [_cfg(1, 2, "m1", CONFIG_JOINT, config_a)])
    _set_log(sim, "b", [_cfg(1, 2, "m1", CONFIG_JOINT, config_a)])
    _set_log(sim, "c", [_cfg(1, 2, "m1", CONFIG_JOINT, config_b)])

    violations = _report(sim)["logMatching"]["violations"]
    assert violations == [
        {
            "index": 1,
            "term": 2,
            "variants": [
                {
                    "id": "m1",
                    "kind": KIND_CONFIG,
                    "entryType": CONFIG_JOINT,
                    "config": config_a,
                    "nodes": ["a", "b"],
                },
                {
                    "id": "m1",
                    "kind": KIND_CONFIG,
                    "entryType": CONFIG_JOINT,
                    "config": config_b,
                    "nodes": ["c"],
                },
            ],
        }
    ]


def test_different_stable_config_payloads_violate():
    sim = _finished_sim()
    config_a = _stable(["a", "b", "c", "d"])
    config_b = _stable(["a", "b", "c", "e"])
    _set_log(sim, "a", [_cfg(1, 2, "m1", CONFIG_STABLE, config_a)])
    _set_log(sim, "b", [_cfg(1, 2, "m1", CONFIG_STABLE, config_b)])
    _set_log(sim, "c", [_cfg(1, 2, "m1", CONFIG_STABLE, config_b)])

    violations = _report(sim)["logMatching"]["violations"]
    assert violations == [
        {
            "index": 1,
            "term": 2,
            "variants": [
                {
                    "id": "m1",
                    "kind": KIND_CONFIG,
                    "entryType": CONFIG_STABLE,
                    "config": config_a,
                    "nodes": ["a"],
                },
                {
                    "id": "m1",
                    "kind": KIND_CONFIG,
                    "entryType": CONFIG_STABLE,
                    "config": config_b,
                    "nodes": ["b", "c"],
                },
            ],
        }
    ]


def test_identical_content_does_not_violate():
    sim = _finished_sim()
    entry = _cmd(1, 1, "x1", {"k": "v"})
    for name in sim.node_names:
        _set_log(sim, name, [dict(entry)])
    assert _report(sim)["logMatching"]["violations"] == []


def test_same_index_different_term_does_not_violate():
    sim = _finished_sim()
    # Log matching only compares entries sharing both index and term.
    _set_log(sim, "a", [_cmd(1, 1, "x1", "one")])
    _set_log(sim, "b", [_cmd(1, 2, "y2", "two")])
    _set_log(sim, "c", [_cmd(1, 1, "x1", "one")])
    assert _report(sim)["logMatching"]["violations"] == []


def test_node_missing_index_does_not_violate():
    sim = _finished_sim()
    _set_log(sim, "a", [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")])
    _set_log(sim, "b", [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")])
    # c simply has not received index 2; absence is not a disagreement.
    _set_log(sim, "c", [_cmd(1, 1, "x1", "one")])
    assert _report(sim)["logMatching"]["violations"] == []


def test_comparison_crosses_snapshot_compaction_boundary_agreement():
    sim = _finished_sim()
    applied = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")]
    # a folded both entries into its snapshot (its view comes from applied
    # history); b and c still hold them in the uncompacted log.
    _set_compacted(sim, "a", applied, [])
    _set_log(sim, "b", [dict(e) for e in applied])
    _set_log(sim, "c", [dict(e) for e in applied])
    assert _report(sim)["logMatching"]["violations"] == []


def test_comparison_crosses_snapshot_compaction_boundary_disagreement():
    sim = _finished_sim()
    # a's index-2 entry is compacted applied history; b's index-2 entry is
    # an uncompacted log entry with different content at the same term. c
    # agrees with a through its own log.
    _set_compacted(
        sim,
        "a",
        [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")],
        [_cmd(3, 1, "x3", "three")],
    )
    _set_log(
        sim,
        "b",
        [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "q2", "TWO"), _cmd(3, 1, "x3", "three")],
    )
    _set_log(
        sim,
        "c",
        [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two"), _cmd(3, 1, "x3", "three")],
    )

    violations = _report(sim)["logMatching"]["violations"]
    # Index 3 agrees across the boundary and must not be reported; the
    # compacted-prefix divergence at index 2 is found exactly once.
    assert violations == [
        {
            "index": 2,
            "term": 1,
            "variants": [
                {"id": "q2", "command": "TWO", "nodes": ["b"]},
                {"id": "x2", "command": "two", "nodes": ["a", "c"]},
            ],
        }
    ]


# -- state machine safety ---------------------------------------------------


def test_applied_different_client_commands_same_index_violates():
    sim = _finished_sim()
    _set_applied(sim, "a", [_cmd(1, 1, "x1", "one")])
    _set_applied(sim, "b", [_cmd(1, 1, "y1", "two")])
    _set_applied(sim, "c", [_cmd(1, 1, "x1", "one")])

    violations = _report(sim)["stateMachineSafety"]["violations"]
    assert violations == [
        {
            "index": 1,
            "variants": [
                {"term": 1, "id": "x1", "command": "one", "nodes": ["a", "c"]},
                {"term": 1, "id": "y1", "command": "two", "nodes": ["b"]},
            ],
        }
    ]


def test_command_and_config_at_same_applied_index_violate():
    sim = _finished_sim()
    config = _stable(["a", "b", "c", "d"])
    _set_applied(sim, "a", [_cmd(1, 1, "x1", "one")])
    _set_applied(sim, "b", [_cmd(1, 1, "x1", "one")])
    _set_applied(sim, "c", [_cfg(1, 1, "m1", CONFIG_STABLE, config)])

    violations = _report(sim)["stateMachineSafety"]["violations"]
    assert violations == [
        {
            "index": 1,
            "variants": [
                {"term": 1, "id": "m1", "kind": KIND_CONFIG,
                 "entryType": CONFIG_STABLE, "config": config, "nodes": ["c"]},
                {"term": 1, "id": "x1", "command": "one", "nodes": ["a", "b"]},
            ],
        }
    ]


def test_divergence_inside_compacted_applied_history_violates():
    sim = _finished_sim()
    # The disagreement sits at index 1, which all three nodes have since
    # folded into a snapshot; the applied history (which survives
    # compaction) still exposes it. Later indices agree.
    _set_compacted(
        sim,
        "a",
        [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two"), _cmd(3, 1, "x3", "three")],
        [],
    )
    _set_compacted(
        sim,
        "b",
        [_cmd(1, 1, "y1", "ONE"), _cmd(2, 1, "x2", "two"), _cmd(3, 1, "x3", "three")],
        [],
    )
    _set_compacted(
        sim,
        "c",
        [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two"), _cmd(3, 1, "x3", "three")],
        [],
    )
    # A snapshot is only structurally consistent once commit progress has
    # reached the applied position.
    for name in sim.node_names:
        sim.state[name].commit_index = 3

    violations = _report(sim)["stateMachineSafety"]["violations"]
    assert violations == [
        {
            "index": 1,
            "variants": [
                {"term": 1, "id": "x1", "command": "one", "nodes": ["a", "c"]},
                {"term": 1, "id": "y1", "command": "ONE", "nodes": ["b"]},
            ],
        }
    ]


def test_node_not_yet_applied_index_does_not_violate():
    sim = _finished_sim()
    _set_applied(sim, "a", [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")])
    _set_applied(sim, "b", [_cmd(1, 1, "x1", "one")])
    _set_applied(sim, "c", [_cmd(1, 1, "x1", "one")])
    assert _report(sim)["stateMachineSafety"]["violations"] == []


def test_identical_applied_content_does_not_violate():
    sim = _finished_sim()
    history = [_cmd(1, 1, "x1", "one"), _cmd(2, 1, "x2", "two")]
    for name in sim.node_names:
        _set_applied(sim, name, [dict(e) for e in history])
    assert _report(sim)["stateMachineSafety"]["violations"] == []
