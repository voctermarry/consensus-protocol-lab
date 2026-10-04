"""Final report generation.

Everything here is read-only over the finished simulation: the timeline, the
final node states and the bookkeeping accumulated during the run are
projected into the client/membership/read summaries and the election-safety,
log-matching, state-machine-safety, linearizability and liveness reports.
Nothing in this module mutates simulator state.
"""

from __future__ import annotations

import json

from .node import CONFIG_JOINT, KIND_CONFIG, _Node


def _log_matching_violations(sim) -> list[dict]:
    """Same index and term, but different content (id/command for client
    commands, or the configuration payload) across nodes, including
    indices folded into snapshots or occupied by configuration entries."""
    violations = []
    max_len = max((st.last_log_index() for st in sim.state.values()), default=0)
    for index in range(1, max_len + 1):
        by_term: dict[int, dict[tuple, dict]] = {}
        for name in sim.node_names:
            st = sim.state[name]
            entry = sim._entry_view(st, index)
            if entry is None:
                continue
            if entry.get("kind") == KIND_CONFIG:
                marker = json.dumps(
                    [
                        entry["id"],
                        KIND_CONFIG,
                        entry["entryType"],
                        entry.get("config"),
                        entry.get("action"),
                        entry.get("member"),
                    ],
                    ensure_ascii=False,
                    sort_keys=True,
                )
                bucket_view = {
                    "id": entry["id"],
                    "kind": KIND_CONFIG,
                    "entryType": entry["entryType"],
                    "config": entry.get("config"),
                    "nodes": [],
                }
            else:
                marker = json.dumps([entry["id"], entry["command"]], ensure_ascii=False, sort_keys=True)
                bucket_view = {"id": entry["id"], "command": entry["command"], "nodes": []}
            bucket = by_term.setdefault(entry["term"], {}).setdefault(marker, bucket_view)
            bucket["nodes"].append(name)
        for term, variants in sorted(by_term.items()):
            if len(variants) > 1:
                violations.append(
                    {
                        "index": index,
                        "term": term,
                        "variants": [bucket for _marker, bucket in sorted(variants.items())],
                    }
                )
    return violations

def _state_machine_safety_violations(sim) -> list[dict]:
    """Different nodes applied different entries at the same index.
    Configuration entries participate in index alignment alongside client
    commands, and applied histories recovered from snapshots are covered."""
    violations = []
    max_applied = max((st.last_applied for st in sim.state.values()), default=0)
    for index in range(1, max_applied + 1):
        variants: dict[str, dict] = {}
        for name in sim.node_names:
            st = sim.state[name]
            if index > st.last_applied:
                continue
            entry = st.applied[index - 1]
            if entry.get("kind") == KIND_CONFIG:
                marker = json.dumps(
                    [
                        entry["term"],
                        entry["id"],
                        KIND_CONFIG,
                        entry["entryType"],
                        entry.get("config"),
                    ],
                    ensure_ascii=False,
                    sort_keys=True,
                )
                bucket = variants.setdefault(
                    marker,
                    {
                        "term": entry["term"],
                        "id": entry["id"],
                        "kind": KIND_CONFIG,
                        "entryType": entry["entryType"],
                        "config": entry.get("config"),
                        "nodes": [],
                    },
                )
            else:
                marker = json.dumps([entry["term"], entry["id"], entry["command"]], ensure_ascii=False, sort_keys=True)
                bucket = variants.setdefault(
                    marker, {"term": entry["term"], "id": entry["id"], "command": entry["command"], "nodes": []}
                )
            bucket["nodes"].append(name)
        if len(variants) > 1:
            violations.append(
                {"index": index, "variants": [bucket for marker, bucket in sorted(variants.items())]}
            )
    return violations

def _clients_report(sim) -> dict:
    grouped: dict[str, list[dict]] = {
        "committed": [],
        "superseded": [],
        "pending": [],
        "rejected": [],
    }

    def find_applied(command_id: str) -> dict | None:
        for name in sim.node_names:
            for entry in sim.state[name].applied:
                if entry["id"] == command_id:
                    return entry
        return None

    def in_any_log(command_id: str) -> dict | None:
        for name in sim.node_names:
            for entry in sim.state[name].log:
                if entry["id"] == command_id:
                    return entry
        return None

    for command in sim.commands:
        command_id = command["id"]
        if command_id in sim.rejected:
            rejected = sim.rejected[command_id]
            grouped["rejected"].append(
                {
                    "id": command_id,
                    "node": command["node"],
                    "reason": rejected["reason"],
                    "knownLeader": rejected["knownLeader"],
                }
            )
            continue
        applied = find_applied(command_id)
        if applied is not None:
            grouped["committed"].append(
                {
                    "id": command_id,
                    "node": command["node"],
                    "index": applied["index"],
                    "term": applied["term"],
                }
            )
            continue
        present = in_any_log(command_id)
        if present is not None:
            grouped["pending"].append(
                {
                    "id": command_id,
                    "node": command["node"],
                    "index": present["index"],
                    "term": present["term"],
                }
            )
        else:
            grouped["superseded"].append({"id": command_id, "node": command["node"]})
    return grouped

def _node_report(sim, name: str, st: _Node) -> dict:
    report = {
        "role": st.role,
        "term": st.term,
        "votedFor": st.voted_for,
        "knownLeader": st.known_leader,
        "log": [dict(entry) for entry in st.log],
        "commitIndex": st.commit_index,
        "lastApplied": st.last_applied,
        "applied": [dict(entry) for entry in st.applied],
    }
    if sim.membership_enabled:
        # Voter/learner according to the node's newest configuration,
        # which governs voting and campaigning even before it commits.
        report["membershipRole"] = (
            "voter" if sim._is_voter(name, st.latest_config(sim.initial_config)) else "learner"
        )
    if sim.snapshot_threshold is not None:
        if st.snapshot_index > 0:
            report["snapshot"] = {
                "lastIncludedIndex": st.snapshot_index,
                "lastIncludedTerm": st.snapshot_term,
            }
        else:
            report["snapshot"] = None
    if sim.node_events_provided or sim.storage_faults_active:
        # Crash/restart lifecycle fields: explicit node events or injected
        # storage faults (the two never coexist in one scenario).
        report["online"] = st.online
        report["restartCount"] = st.restart_count
    return report

def _membership_report(sim) -> dict:
    # The reference node deterministically has the greatest committed
    # position, then the greatest log length (declared node order breaks
    # ties): committed truth dominates, so a deposed leader's stale
    # uncommitted suffix cannot describe a phase the cluster left.
    ref_name = max(
        sim.node_names,
        key=lambda name: (
            sim.state[name].commit_index,
            sim.state[name].last_log_index(),
            -sim.index[name],
        ),
    )
    ref = sim.state[ref_name]
    committed = ref.committed_config(sim.initial_config)
    latest = ref.latest_config(sim.initial_config)

    if latest.get("type") == CONFIG_JOINT:
        old, new = sim._config_groups(latest)
        current_stable = sorted(old)
        joint = {"id": _latest_joint_id(ref),
                 "old": sorted(old), "new": sorted(new)}
    elif committed.get("type") == CONFIG_JOINT:
        old, new = sim._config_groups(committed)
        current_stable = sorted(old)
        joint = {"id": _latest_joint_id(ref),
                 "old": sorted(old), "new": sorted(new)}
    else:
        _, stable_voters = sim._config_groups(committed)
        current_stable = sorted(stable_voters)
        joint = None

    joint_ids = set()
    for st in sim.state.values():
        for applied in st.applied:
            if applied.get("kind") == KIND_CONFIG and applied["entryType"] == CONFIG_JOINT:
                joint_ids.add(applied["id"])
        for log_entry in st.log:
            if log_entry.get("kind") == KIND_CONFIG and log_entry["entryType"] == CONFIG_JOINT:
                joint_ids.add(log_entry["id"])

    changes = []
    for change in sim.changes:
        change_id = change["id"]
        result = sim.change_results.get(change_id)
        entry = {
            "id": change_id,
            "node": change["node"],
            "action": change["action"],
            "member": change["member"],
        }
        if result is None or result.get("outcome") == "pending":
            entry["outcome"] = "pending"
            if sim.pending_catchup is not None and sim.pending_catchup["id"] == change_id:
                entry["phase"] = "catchingUp"
            elif change_id in joint_ids:
                phase_info = result.get("jointPhase") if result else None
                entry["phase"] = "joint"
                if phase_info is not None:
                    entry["joint"] = {
                        "old": sorted(phase_info["old"]),
                        "new": sorted(phase_info["new"]),
                    }
            else:
                entry["phase"] = "catchingUp"
        elif result.get("outcome") == "rejected":
            entry["outcome"] = "rejected"
            entry["reason"] = result["reason"]
        else:
            entry["outcome"] = "committed"
            entry["index"] = result["index"]
            entry["term"] = result["term"]
        changes.append(entry)

    return {
        "initial": sorted(sim.initial_config["new"]),
        "current": current_stable,
        "joint": joint,
        "changes": changes,
    }

def _latest_config_entry_id(st: _Node, entry_type: str) -> str | None:
    """The id of the newest committed configuration entry of a type,
    looking through the snapshot into the applied history."""
    found = None
    for entry in st.applied:
        if entry.get("kind") == KIND_CONFIG and entry["entryType"] == entry_type:
            found = entry["id"]
    upper = min(st.commit_index, st.last_log_index())
    for i in range(st.snapshot_index + 1, upper + 1):
        entry = st.log[i - 1 - st.snapshot_index]
        if entry.get("kind") == KIND_CONFIG and entry["entryType"] == entry_type:
            found = entry["id"]
    return found

def _latest_joint_id(st: _Node) -> str | None:
    """The id of the newest joint configuration entry the reference node
    holds, including one appended but not yet committed."""
    found = _latest_config_entry_id(st, CONFIG_JOINT)
    for entry in st.log:
        if entry.get("kind") == KIND_CONFIG and entry["entryType"] == CONFIG_JOINT:
            found = entry["id"]
    return found

def _reads_report(sim) -> list[dict]:
    """The final outcome of every read query, in input order. Queries
    still awaiting a majority confirmation when the simulation ends are
    reported as pending."""
    reads = []
    for query in sim.read_queries:
        query_id = query["id"]
        result = sim.read_results.get(query_id)
        if result is None:
            pending = sim.pending_reads[query_id]
            result = {
                "outcome": "pending",
                "term": pending["term"],
                "readIndex": pending["readIndex"],
            }
        reads.append({"id": query_id, "node": query["node"], **result})
    return reads

def _linearizability_violations(sim) -> list[dict]:
    """Per completed read, checked in input order:
    - nonPrefix: the returned state is not the complete client-command
      prefix of the serving leader's applied history up to readIndex
      (configuration entries excluded, compacted commands included);
    - staleRead: the state omits a write that was already committed when
      the query was accepted."""
    violations = []
    for query in sim.read_queries:
        query_id = query["id"]
        result = sim.read_results.get(query_id)
        if result is None or result.get("outcome") != "completed":
            continue
        read_index = result["readIndex"]
        st = sim.state[query["node"]]
        expected = [
            sim._read_state_entry(entry)
            for entry in st.applied[:read_index]
            if entry.get("kind") != KIND_CONFIG
        ]
        if result["state"] != expected:
            violations.append(
                {"type": "nonPrefix", "id": query_id, "readIndex": read_index}
            )
        present = {entry["id"] for entry in result["state"]}
        missing = sorted(sim.read_required.get(query_id, set()) - present)
        if missing:
            violations.append(
                {"type": "staleRead", "id": query_id, "missing": missing}
            )
    return violations

def _liveness_report(sim) -> dict:
    """Per-check outcomes in input order; every failed outcome is also
    listed as a violation."""
    checks = [sim.liveness_results[check["id"]] for check in sim.liveness_checks]
    violations = [check for check in checks if check["status"] == "failed"]
    return {"checks": checks, "violations": violations}

def build_report(sim) -> dict:
    leaders = {str(term): names for term, names in sorted(sim.leaders_by_term.items())}
    violations = [
        {"term": term, "leaders": list(names)}
        for term, names in sorted(sim.leaders_by_term.items())
        if len(names) > 1
    ]
    report = {
        "timeline": sim.timeline,
        "nodes": {
            name: _node_report(sim, name, st)
            for name, st in sim.state.items()
        },
        "clients": _clients_report(sim),
    }
    if sim.read_queries_provided:
        report["reads"] = _reads_report(sim)
    report["electionSafety"] = {
        "leadersByTerm": leaders,
        "violations": violations,
    }
    report["logMatching"] = {"violations": _log_matching_violations(sim)}
    report["stateMachineSafety"] = {"violations": _state_machine_safety_violations(sim)}
    if sim.read_queries_provided:
        report["linearizability"] = {
            "violations": _linearizability_violations(sim)
        }
    if sim.membership_enabled:
        report["membership"] = _membership_report(sim)
    if sim.liveness_provided:
        report["liveness"] = _liveness_report(sim)
    return report
