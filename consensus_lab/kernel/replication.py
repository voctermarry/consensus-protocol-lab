"""Log replication, snapshots and commit/apply.

This mixin owns the leader's per-peer replication traffic (heartbeats,
appendEntries, installSnapshot), the follower-side truncation/append and
snapshot-install transitions, leader commit advancement under joint
majorities, the apply loop (including snapshot creation), and client-command
intake. Configuration-entry effects cross into the membership mixin through
``_apply_config_entry`` / ``_progress_catchup``; a freshly elected leader lets
the membership mixin finish an open change via ``_resume_membership_change``.
"""

from __future__ import annotations

from ..events import _KIND_HEARTBEAT
from ..node import KIND_CONFIG, ROLE_FOLLOWER, ROLE_LEADER, _Node


class _ReplicationKernel:
    # -- leader broadcast ----------------------------------------------------

    def _send_heartbeats(self, name: str) -> None:
        """Per peer, send either a log-carrying appendEntries (when the peer is
        behind) or a bare heartbeat (when its log already matches). Mixing the
        two would let an empty heartbeat's leaderCommit make a divergent peer
        apply entries that the appendEntries is about to overwrite. The
        appendEntries handler truncates, appends and advances the commit in one
        atomic step, so it alone is safe for a lagging peer."""
        st = self.state[name]
        for peer in self.node_names:
            if peer == name:
                continue
            if st.match_index[peer] < st.last_log_index():
                self._replicate_to(name, peer)
            else:
                self.network.send(
                    name,
                    peer,
                    {"kind": "heartbeat", "term": st.term, "leaderCommit": st.commit_index},
                )
        self.queue.push(
            (self.now + self.heartbeat_interval, _KIND_HEARTBEAT, self.index[name], (name, st.term)),
        )

    def _replicate_to(self, name: str, peer: str) -> None:
        """Send the next due message for one peer: an installSnapshot when the
        peer's next position is already covered by the leader's snapshot,
        otherwise an appendEntries carrying the uncompacted suffix slice."""
        st = self.state[name]
        next_idx = st.next_index[peer]
        if next_idx <= st.snapshot_index:
            # The snapshot bundles the applied state through lastIncludedIndex.
            snapshot_entries = [dict(entry) for entry in st.applied[: st.snapshot_index]]
            self.network.send(
                name,
                peer,
                {
                    "kind": "installSnapshot",
                    "term": st.term,
                    "lastIncludedIndex": st.snapshot_index,
                    "lastIncludedTerm": st.snapshot_term,
                    "entries": snapshot_entries,
                },
            )
            return
        prev_idx = next_idx - 1
        entries = [dict(entry) for entry in st.log[next_idx - 1 - st.snapshot_index:]]
        self.network.send(
            name,
            peer,
            {
                "kind": "appendEntries",
                "term": st.term,
                "prevLogIndex": prev_idx,
                "prevLogTerm": st.term_at(prev_idx),
                "entries": entries,
                "leaderCommit": st.commit_index,
            },
        )

    # -- heartbeat / appendEntries / snapshot handlers -----------------------

    def _handle_heartbeat(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        accepted = msg["term"] >= st.term
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "heartbeat",
            "term": msg["term"],
            "result": "delivered",
            "detail": "accepted" if accepted else "staleTerm",
        })
        if not accepted:
            return
        if msg["term"] > st.term or st.role != ROLE_FOLLOWER:
            self._become_follower(dst, msg["term"], "heartbeat")
        else:
            self._reset_timeout(dst)
        st.known_leader = src
        # A current-term heartbeat is leader contact: it suppresses pre-vote
        # grants until the local election deadline next elapses (and stepping
        # down above canceled any active pre-vote round).
        st.leader_contact = self.now
        # Heartbeats carry leaderCommit even when no entry is in flight.
        if msg.get("leaderCommit", 0) > st.commit_index:
            st.commit_index = min(msg["leaderCommit"], st.last_log_index())
        self._advance_apply(dst)

    def _handle_append_entries(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        if msg["term"] < st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "appendEntries",
                "term": msg["term"],
                "result": "delivered",
                "detail": "staleTerm",
            })
            self.network.send(
                dst,
                src,
                {"kind": "appendReply", "term": st.term, "success": False, "matchIndex": 0},
            )
            return

        if msg["term"] > st.term or st.role != ROLE_FOLLOWER:
            self._become_follower(dst, msg["term"], "appendEntries")
        else:
            self._reset_timeout(dst)
        st.known_leader = src
        # A current-term appendEntries (even one that reports a prefix
        # conflict below) proves leader contact and suppresses pre-vote grants
        # until the next election deadline.
        st.leader_contact = self.now

        prev_index = msg["prevLogIndex"]
        prefix_ok = (
            prev_index >= st.snapshot_index
            and prev_index <= st.last_log_index()
            and st.term_at(prev_index) == msg["prevLogTerm"]
        )
        if not prefix_ok:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "appendEntries",
                "term": msg["term"],
                "result": "delivered",
                "detail": "conflict",
                "prevLogIndex": prev_index,
            })
            self.network.send(
                dst,
                src,
                {"kind": "appendReply", "term": st.term, "success": False, "matchIndex": 0},
            )
            return

        # Latest configuration decides voting/campaigning as soon as an entry
        # is appended, so start or stop the election timer when the batch
        # changes this node's membership. Compute eligibility before any
        # truncation or append.
        eligible_before = self._can_vote(dst)

        # Truncate the suffix at the first position whose term differs.
        conflict_index = 0
        for offset, entry in enumerate(msg["entries"]):
            index = prev_index + 1 + offset
            if st.has_entry(index) and st.log[index - 1 - st.snapshot_index]["term"] != entry["term"]:
                conflict_index = index
                del st.log[index - 1 - st.snapshot_index:]
                break

        # Append whatever is not already present with a matching term.
        appended = 0
        for offset, entry in enumerate(msg["entries"]):
            index = prev_index + 1 + offset
            if index > st.last_log_index():
                if entry.get("kind") == KIND_CONFIG:
                    stored = {
                        "index": index,
                        "term": entry["term"],
                        "kind": KIND_CONFIG,
                        "id": entry["id"],
                        "entryType": entry["entryType"],
                        "config": entry["config"],
                        "action": entry["action"],
                        "member": entry["member"],
                    }
                else:
                    stored = self._command_entry(
                        index, entry["term"], entry["id"], entry["command"]
                    )
                st.log.append(stored)
                appended += 1

        self._reconcile_election_timer(dst, eligible_before)

        config_entries = [
            {
                "index": entry["index"],
                "id": entry["id"],
                "entryType": entry["entryType"],
            }
            for entry in msg["entries"]
            if entry.get("kind") == KIND_CONFIG
        ]
        result_entry = {
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "appendEntries",
            "term": msg["term"],
            "result": "delivered",
            "detail": "conflict" if conflict_index else "accepted",
            "prevLogIndex": prev_index,
            "appended": appended,
            "lastLogIndex": st.last_log_index(),
        }
        if config_entries:
            # Configuration replication is reported separately from ordinary
            # client-command replication.
            result_entry["configEntries"] = config_entries
        self._record(result_entry)

        if msg["leaderCommit"] > st.commit_index:
            st.commit_index = min(msg["leaderCommit"], st.last_log_index())
        self._advance_apply(dst)

        self.network.send(
            dst,
            src,
            {
                "kind": "appendReply",
                "term": st.term,
                "success": True,
                "matchIndex": st.last_log_index(),
            },
        )

    def _handle_install_snapshot(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        included_index = msg["lastIncludedIndex"]
        included_term = msg["lastIncludedTerm"]
        if msg["term"] < st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshot",
                "term": msg["term"],
                "result": "delivered",
                "detail": "staleTerm",
                "lastIncludedIndex": included_index,
            })
            self.network.send(
                dst,
                src,
                {
                    "kind": "installSnapshotReply",
                    "term": st.term,
                    "result": "staleTerm",
                    "lastIncludedIndex": included_index,
                    "snapshotIndex": st.snapshot_index,
                    "lastLogIndex": st.last_log_index(),
                },
            )
            return

        if msg["term"] > st.term or st.role != ROLE_FOLLOWER:
            self._become_follower(dst, msg["term"], "installSnapshot")
        else:
            self._reset_timeout(dst)
        st.known_leader = src
        # A current-term installSnapshot is leader contact regardless of
        # whether the snapshot is installed or ignored as redundant.
        st.leader_contact = self.now

        if included_index <= st.snapshot_index:
            # An equal-or-newer snapshot already covers this position.
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshot",
                "term": msg["term"],
                "result": "delivered",
                "detail": "ignored",
                "lastIncludedIndex": included_index,
            })
            self.network.send(
                dst,
                src,
                {
                    "kind": "installSnapshotReply",
                    "term": st.term,
                    "result": "ignored",
                    "lastIncludedIndex": included_index,
                    "snapshotIndex": st.snapshot_index,
                    "lastLogIndex": st.last_log_index(),
                },
            )
            return

        # Accept the snapshot. A suffix entry at the snapshot boundary with a
        # matching term is still valid and is kept; any other suffix is
        # discarded, including a same-position entry from a different term.
        eligible_before = self._can_vote(dst)
        keep = (
            st.has_entry(included_index)
            and st.term_at(included_index) == included_term
        )
        snapshot_entries = [dict(entry) for entry in msg["entries"]]
        # Recover the configuration in force at the snapshot position from
        # the applied entries bundled into the snapshot.
        snap_config = None
        for bundled in snapshot_entries:
            if bundled.get("kind") == KIND_CONFIG:
                snap_config = bundled["config"]
        if keep:
            # The entry at included_index joins the snapshot; retain only the
            # entries strictly following it, together with whatever the
            # follower had already applied beyond the snapshot point (such
            # entries were committed and must not be applied twice).
            retained_applied = [
                dict(entry) for entry in st.applied if entry["index"] > included_index
            ]
            st.log = st.log[included_index - st.snapshot_index:]
            st.applied = snapshot_entries + retained_applied
        else:
            st.log = []
            # Snapshot contents arrive as already-applied state: restore them
            # silently, so no applied events are emitted for them.
            st.applied = snapshot_entries
        st.snapshot_index = included_index
        st.snapshot_term = included_term
        st.snapshot_config = snap_config
        if st.commit_index < included_index:
            st.commit_index = included_index
        st.last_applied = len(st.applied)

        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "installSnapshot",
            "term": msg["term"],
            "result": "delivered",
            "detail": "installed",
            "lastIncludedIndex": included_index,
            "lastIncludedTerm": included_term,
        })
        self._record({
            "type": "snapshotInstalled",
            "node": dst,
            "peer": src,
            "lastIncludedIndex": included_index,
            "lastIncludedTerm": included_term,
        })
        # Configuration and membership arrive as already-restored state, just
        # like commands: no configurationApplied events are emitted for them.
        self._restore_membership_from_snapshot(dst, snapshot_entries, snap_config, eligible_before)
        # Apply whatever suffix entries are now committed beyond the snapshot.
        self._advance_apply(dst)
        self.network.send(
            dst,
            src,
            {
                "kind": "installSnapshotReply",
                "term": st.term,
                "result": "installed",
                "lastIncludedIndex": included_index,
                "snapshotIndex": st.snapshot_index,
                "lastLogIndex": st.last_log_index(),
            },
        )

    def _handle_snapshot_reply(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        if msg["term"] > st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshotReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "higherTerm",
            })
            self._become_follower(dst, msg["term"], "higherTermMessage")
            return
        if st.role != ROLE_LEADER or msg["term"] != st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshotReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "ignored",
            })
            return

        result = msg["result"]
        if result == "installed":
            included = msg["lastIncludedIndex"]
            if included > st.match_index[src]:
                st.match_index[src] = included
            if included + 1 > st.next_index[src]:
                st.next_index[src] = included + 1
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshotReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "installed",
                "matchIndex": st.match_index[src],
            })
            # Resume log replication from the entry right after the snapshot.
            self._replicate_to(dst, src)
            self._progress_catchup(dst)
            return

        if result == "staleTerm":
            # Same-term refusal (the peer led in this term, for example): no
            # progress change; the next heartbeat retries reconciliation.
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshotReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "staleTerm",
            })
            return

        # The follower already holds an equal-or-newer snapshot: advance its
        # progress conservatively and let appendEntries reconcile the suffix.
        peer_snapshot = min(msg["snapshotIndex"], st.last_log_index())
        if peer_snapshot > st.match_index[src]:
            st.match_index[src] = peer_snapshot
        if peer_snapshot + 1 > st.next_index[src]:
            st.next_index[src] = peer_snapshot + 1
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "installSnapshotReply",
            "term": msg["term"],
            "result": "delivered",
            "detail": "ignored",
            "matchIndex": st.match_index[src],
        })
        self._replicate_to(dst, src)

    def _handle_append_reply(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        if msg["term"] > st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "appendReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "higherTerm",
            })
            self._become_follower(dst, msg["term"], "higherTermMessage")
            return
        if st.role != ROLE_LEADER or msg["term"] != st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "appendReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "ignored",
            })
            return

        if msg["success"]:
            # A follower with a longer divergent suffix can acknowledge a
            # batch whose shared positions already match; the leader must not
            # treat positions beyond its own log end as replicated.
            match = min(msg["matchIndex"], st.last_log_index())
            advanced = match > st.match_index[src]
            if advanced:
                st.match_index[src] = match
                if match + 1 > st.next_index[src]:
                    st.next_index[src] = match + 1
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "appendReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "matched",
                "matchIndex": st.match_index[src],
            })
            if advanced:
                self._advance_leader_commit(dst)
            self._progress_catchup(dst)
            return

        # Deterministic backoff: retry one log position earlier.
        if st.next_index[src] > 1:
            st.next_index[src] -= 1
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "appendReply",
            "term": msg["term"],
            "result": "delivered",
            "detail": "conflict",
            "nextIndex": st.next_index[src],
        })
        self._replicate_to(dst, src)

    # -- client commands ----------------------------------------------------

    def _on_client_command(self, command: dict) -> None:
        node = command["node"]
        command_id = command["id"]
        st = self.state[node]
        if not st.online:
            self.rejected[command_id] = {"node": node, "reason": "nodeDown", "knownLeader": None}
            self._record({
                "type": "clientResult",
                "node": node,
                "id": command_id,
                "result": "rejected",
                "reason": "nodeDown",
                "knownLeader": None,
            })
            return
        if st.role != ROLE_LEADER:
            self.rejected[command_id] = {"node": node, "reason": "notLeader", "knownLeader": st.known_leader}
            self._record({
                "type": "clientResult",
                "node": node,
                "id": command_id,
                "result": "rejected",
                "reason": "notLeader",
                "knownLeader": st.known_leader,
            })
            return

        index = st.last_log_index() + 1
        st.log.append(self._command_entry(index, st.term, command_id, command["command"]))
        self.accepted_command_ids.add(command_id)
        self._record({
            "type": "clientResult",
            "node": node,
            "id": command_id,
            "result": "accepted",
            "index": index,
            "term": st.term,
        })
        for peer in self.node_names:
            if peer != node:
                self._replicate_to(node, peer)

    # -- commit and apply ---------------------------------------------------

    def _advance_leader_commit(self, name: str) -> None:
        st = self.state[name]
        replicated = sorted([st.last_log_index()] + list(st.match_index.values()), reverse=True)
        # Entries replicated while a joint configuration is active need
        # strict majorities of BOTH voter sets; entries before/after it use
        # the single stable set.
        for candidate in replicated:
            if candidate <= st.commit_index:
                break
            if st.term_at(candidate) != st.term:
                continue
            # A server uses the newest config in its log as soon as it is
            # appended, so the joint entry is committed by majorities of
            # BOTH sets; the stable entry is only appended after the joint
            # entry commits, so it and later entries use the new single set.
            config = st.config_at(candidate + 1, self.initial_config)
            if self._replicated_by_majorities(name, candidate, config):
                # Track the client commands this commit covers so that reads
                # accepted later can be checked for staleness.
                for index in range(st.commit_index + 1, candidate + 1):
                    view = self._entry_view(st, index)
                    if view is not None and view.get("kind") != KIND_CONFIG:
                        self.committed_command_ids.add(view["id"])
                st.commit_index = candidate
                self._record({
                    "type": "commitAdvance",
                    "node": name,
                    "term": st.term,
                    "commitIndex": st.commit_index,
                })
                self._advance_apply(name)
                return

    def _advance_apply(self, name: str) -> None:
        st = self.state[name]
        while st.last_applied < st.commit_index:
            next_index = st.last_applied + 1
            if not st.has_entry(next_index):
                break
            st.last_applied = next_index
            entry = st.log[next_index - 1 - st.snapshot_index]
            if entry.get("kind") == KIND_CONFIG:
                st.applied.append(self._applied_config_view(entry))
                self._apply_config_entry(name, entry)
            else:
                st.applied.append(
                    self._command_entry(
                        entry["index"], entry["term"], entry["id"], entry["command"]
                    )
                )
                self._record({
                    "type": "applied",
                    "node": name,
                    "index": entry["index"],
                    "term": entry["term"],
                    "id": entry["id"],
                })
            if (
                self.snapshot_threshold is not None
                and st.last_applied - st.snapshot_index >= self.snapshot_threshold
            ):
                self._create_snapshot(name)

    @staticmethod
    def _applied_config_view(entry: dict) -> dict:
        return {
            "index": entry["index"],
            "term": entry["term"],
            "id": entry["id"],
            "kind": KIND_CONFIG,
            "entryType": entry["entryType"],
            "config": entry["config"],
            "action": entry["action"],
            "member": entry["member"],
        }

    def _create_snapshot(self, name: str) -> None:
        """Fold every applied entry through last_applied into the snapshot and
        delete the compacted prefix. Global indices are preserved: the
        surviving log suffix keeps its indices and commitIndex/lastApplied are
        not renumbered. The configuration in force at the snapshot index is
        folded in as well, so membership survives compaction."""
        st = self.state[name]
        target = st.last_applied
        cut = target - st.snapshot_index
        included_term = st.log[cut - 1]["term"]
        config = st.snapshot_config if st.snapshot_config else self.initial_config
        for offset in range(cut):
            entry = st.log[offset]
            if entry.get("kind") == KIND_CONFIG:
                config = entry["config"]
        st.snapshot_index = target
        st.snapshot_term = included_term
        st.snapshot_config = self._store_config(config)
        del st.log[:cut]
        self._record({
            "type": "snapshotCreated",
            "node": name,
            "lastIncludedIndex": target,
            "lastIncludedTerm": included_term,
        })

    def _entry_view(self, st: _Node, index: int) -> dict | None:
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
