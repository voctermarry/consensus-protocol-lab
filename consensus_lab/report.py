"""Final report assembly for a finished simulation run.

The builder is a read-only view over a run's end state: it walks the node
states, per-id outcome ledgers and the recorded timeline history to produce
the report, and never mutates simulation state or re-runs protocol logic.
Keeping it here means the simulation kernel cannot let reporting feed back
into node transitions.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .node import KIND_CONFIG

if TYPE_CHECKING:
    from .node import Node


def read_state_entry(entry: dict) -> dict:
    """The read-state view of one applied client command. Configuration
    entries never appear in a read state."""
    return {
        "index": entry["index"],
        "term": entry["term"],
        "id": entry["id"],
        "command": entry["command"],
    }


class ReportBuilder:
    # The mixing class provides the full run state; declared only for clarity
    # and type checkers:
    if TYPE_CHECKING:
        node_names: list[str]
        index: dict[str, int]
        state: dict[str, Node]
        initial_config: dict
        timeline: list[dict]
        leaders_by_term: dict[int, list[str]]
        commands: list[dict]
        changes: list[dict]
        read_queries: list[dict]
        read_queries_provided: bool
        liveness_checks: list[dict]
        liveness_provided: bool
        liveness_results: dict[str, dict]
        membership_enabled: bool
        snapshot_threshold: int | None
        node_events_provided: bool
        rejected: dict[str, dict]
        change_results: dict[str, dict]
        pending_catchup: dict | None
        pending_reads: dict[str, dict]
        read_results: dict[str, dict]
        read_required: dict[str, set[str]]
        committed_command_ids: set[str]

    # -- shared read-only views ----------------------------------------------

    def _entry_view(self, st: Node, index: int) -> dict | None:
        """The (term, id, ...) a node holds for a global log index, looking
        through snapshot boundaries: applied entries cover the compacted
        prefix and the uncompacted log covers everything after it. Both
        client-command and configuration entries are covered."""
        if index <= 0 or index > st.last_log_index():
            return None
        if index <= st.last_applied:
            return st.applied[index - 1]
        if st.has_entry(index):
            return st.log[index - 1 - st.snapshot_index]
        return None

    def _log_matching_violations(self) -> list[dict]:
        """Same index and term, but different content (id/command for client
        commands, or the configuration payload) across nodes, including
        indices folded into snapshots or occupied by configuration entries."""
        violations = []
        max_len = max((st.last_log_index() for st in self.state.values()), default=0)
        for index in range(1, max_len + 1):
            by_term: dict[int, dict[str, dict]] = {}
            for name in self.node_names:
                st = self.state[name]
                entry = self._entry_view(st, index)
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

    def _state_machine_safety_violations(self) -> list[dict]:
        """Different nodes applied different entries at the same index.
        Configuration entries participate in index alignment alongside client
        commands, and applied histories recovered from snapshots are covered."""
        violations = []
        max_applied = max((st.last_applied for st in self.state.values()), default=0)
        for index in range(1, max_applied + 1):
            variants: dict[str, dict] = {}
            for name in self.node_names:
                st = self.state[name]
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

    def _clients_report(self) -> dict:
        grouped: dict[str, list[dict]] = {
            "committed": [],
            "superseded": [],
            "pending": [],
            "rejected": [],
        }

        def find_applied(command_id: str) -> dict | None:
            for name in self.node_names:
                for entry in self.state[name].applied:
                    if entry["id"] == command_id:
                        return entry
            return None

        def in_any_log(command_id: str) -> dict | None:
            for name in self.node_names:
                for entry in self.state[name].log:
                    if entry["id"] == command_id:
                        return entry
            return None

        for command in self.commands:
            command_id = command["id"]
            if command_id in self.rejected:
                rejected = self.rejected[command_id]
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

    def _node_report(self, name: str, st: Node) -> dict:
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
        if self.membership_enabled:
            # Voter/learner according to the node's newest configuration,
            # which governs voting and campaigning even before it commits.
            report["membershipRole"] = (
                "voter" if self._is_voter(name, st.latest_config(self.initial_config)) else "learner"
            )
        if self.snapshot_threshold is not None:
            if st.snapshot_index > 0:
                report["snapshot"] = {
                    "lastIncludedIndex": st.snapshot_index,
                    "lastIncludedTerm": st.snapshot_term,
                }
            else:
                report["snapshot"] = None
        if self.node_events_provided:
            report["online"] = st.online
            report["restartCount"] = st.restart_count
        return report

    def _membership_report(self) -> dict:
        # The reference node deterministically has the greatest committed
        # position, then the greatest log length (declared node order breaks
        # ties): committed truth dominates, so a deposed leader's stale
        # uncommitted suffix cannot describe a phase the cluster left.
        ref_name = max(
            self.node_names,
            key=lambda name: (
                self.state[name].commit_index,
                self.state[name].last_log_index(),
                -self.index[name],
            ),
        )
        ref = self.state[ref_name]
        committed = ref.committed_config(self.initial_config)
        latest = ref.latest_config(self.initial_config)

        if latest.get("type") == "joint":
            old, new = self._config_groups(latest)
            current_stable = sorted(old)
            joint = {"id": self._latest_joint_id(ref),
                     "old": sorted(old), "new": sorted(new)}
        elif committed.get("type") == "joint":
            old, new = self._config_groups(committed)
            current_stable = sorted(old)
            joint = {"id": self._latest_joint_id(ref),
                     "old": sorted(old), "new": sorted(new)}
        else:
            _, stable_voters = self._config_groups(committed)
            current_stable = sorted(stable_voters)
            joint = None

        joint_ids = set()
        for st in self.state.values():
            for applied in st.applied:
                if applied.get("kind") == KIND_CONFIG and applied["entryType"] == "joint":
                    joint_ids.add(applied["id"])
            for log_entry in st.log:
                if log_entry.get("kind") == KIND_CONFIG and log_entry["entryType"] == "joint":
                    joint_ids.add(log_entry["id"])

        changes = []
        for change in self.changes:
            change_id = change["id"]
            result = self.change_results.get(change_id)
            entry = {
                "id": change_id,
                "node": change["node"],
                "action": change["action"],
                "member": change["member"],
            }
            if result is None or result.get("outcome") == "pending":
                entry["outcome"] = "pending"
                if self.pending_catchup is not None and self.pending_catchup["id"] == change_id:
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
            "initial": sorted(self.initial_config["new"]),
            "current": current_stable,
            "joint": joint,
            "changes": changes,
        }

    def _latest_config_entry_id(self, st: Node, entry_type: str) -> str | None:
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

    def _latest_joint_id(self, st: Node) -> str | None:
        """The id of the newest joint configuration entry the reference node
        holds, including one appended but not yet committed."""
        found = self._latest_config_entry_id(st, "joint")
        for entry in st.log:
            if entry.get("kind") == KIND_CONFIG and entry["entryType"] == "joint":
                found = entry["id"]
        return found

    def _reads_report(self) -> list[dict]:
        """The final outcome of every read query, in input order. Queries
        still awaiting a majority confirmation when the simulation ends are
        reported as pending."""
        reads = []
        for query in self.read_queries:
            query_id = query["id"]
            result = self.read_results.get(query_id)
            if result is None:
                pending = self.pending_reads[query_id]
                result = {
                    "outcome": "pending",
                    "term": pending["term"],
                    "readIndex": pending["readIndex"],
                }
            reads.append({"id": query_id, "node": query["node"], **result})
        return reads

    def _linearizability_violations(self) -> list[dict]:
        """Per completed read, checked in input order:
        - nonPrefix: the returned state is not the complete client-command
          prefix of the serving leader's applied history up to readIndex
          (configuration entries excluded, compacted commands included);
        - staleRead: the state omits a write that was already committed when
          the query was accepted."""
        violations = []
        for query in self.read_queries:
            query_id = query["id"]
            result = self.read_results.get(query_id)
            if result is None or result.get("outcome") != "completed":
                continue
            read_index = result["readIndex"]
            st = self.state[query["node"]]
            expected = [
                read_state_entry(entry)
                for entry in st.applied[:read_index]
                if entry.get("kind") != KIND_CONFIG
            ]
            if result["state"] != expected:
                violations.append(
                    {"type": "nonPrefix", "id": query_id, "readIndex": read_index}
                )
            present = {entry["id"] for entry in result["state"]}
            missing = sorted(self.read_required.get(query_id, set()) - present)
            if missing:
                violations.append(
                    {"type": "staleRead", "id": query_id, "missing": missing}
                )
        return violations

    def _liveness_report(self) -> dict:
        """Per-check outcomes in input order; every failed outcome is also
        listed as a violation."""
        checks = [self.liveness_results[check["id"]] for check in self.liveness_checks]
        violations = [check for check in checks if check["status"] == "failed"]
        return {"checks": checks, "violations": violations}

    def build_report(self) -> dict:
        leaders = {str(term): names for term, names in sorted(self.leaders_by_term.items())}
        violations = [
            {"term": term, "leaders": list(names)}
            for term, names in sorted(self.leaders_by_term.items())
            if len(names) > 1
        ]
        report = {
            "timeline": self.timeline,
            "nodes": {
                name: self._node_report(name, st)
                for name, st in self.state.items()
            },
            "clients": self._clients_report(),
        }
        if self.read_queries_provided:
            report["reads"] = self._reads_report()
        report["electionSafety"] = {
            "leadersByTerm": leaders,
            "violations": violations,
        }
        report["logMatching"] = {"violations": self._log_matching_violations()}
        report["stateMachineSafety"] = {"violations": self._state_machine_safety_violations()}
        if self.read_queries_provided:
            report["linearizability"] = {
                "violations": self._linearizability_violations()
            }
        if self.membership_enabled:
            report["membership"] = self._membership_report()
        if self.liveness_provided:
            report["liveness"] = self._liveness_report()
        return report
