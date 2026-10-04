"""Joint-consensus membership changes.

This mixin owns membership-request intake (including learner catch-up before
the joint entry is appended), appending joint/stable configuration entries,
reacting to a committed configuration entry (timer reconciliation, stable
follow-up, change-result bookkeeping), finishing a change a previous leader
left open, and restoring membership outcomes from an installed snapshot.

It reaches into replication deliberately: a new leader resumes a change only
after the election kernel calls ``_resume_membership_change`` from
``_become_leader``, committed joint entries trigger the stable entry's
replication, and a leader removed by its own change steps down through the
election kernel.
"""

from __future__ import annotations

from ..node import CONFIG_JOINT, CONFIG_STABLE, KIND_CONFIG, ROLE_FOLLOWER, ROLE_LEADER, _Node


class _MembershipKernel:
    def _resume_membership_change(self, name: str) -> None:
        """Finish a change a previous leader left open. If the newest config
        entry present is a joint entry (with no stable entry following it),
        append the stable entry now: winning this term's election required
        majorities of both joint sets, so the joint entry is known to be on
        those majorities even before commitIndex advanced to it."""
        st = self.state[name]
        if st.latest_config(self.initial_config).get("type") == CONFIG_JOINT:
            joint_entry = None
            # Newest joint entry in the uncompacted log, otherwise the newest
            # one folded into the snapshot's applied history.
            for log_entry in reversed(st.log):
                if log_entry.get("kind") == KIND_CONFIG and log_entry["entryType"] == CONFIG_JOINT:
                    joint_entry = log_entry
                    break
            if joint_entry is None:
                for applied in reversed(st.applied):
                    if (
                        applied.get("kind") == KIND_CONFIG
                        and applied["entryType"] == CONFIG_JOINT
                    ):
                        joint_entry = applied
                        break
            if joint_entry is not None:
                _old, new_voters = self._config_groups(joint_entry["config"])
                stable = self._stable_config(new_voters)
                self._append_config_entry(
                    name,
                    joint_entry["id"],
                    CONFIG_STABLE,
                    stable,
                    joint_entry["action"],
                    joint_entry["member"],
                )
                for peer in self.node_names:
                    if peer != name:
                        self._replicate_to(name, peer)
        if self.pending_catchup is not None:
            if self.pending_catchup["leader"] != name:
                # The old leader was replaced before it appended the joint
                # entry; the unfinished catch-up is abandoned.
                self.pending_catchup = None
            else:
                self._progress_catchup(name)

    def _change_in_progress(self, st: _Node, leader: str) -> bool:
        pending = self.pending_catchup
        if pending is not None and pending["leader"] == leader and st.role == ROLE_LEADER:
            return True
        # The change runs until its stable entry commits: a committed joint
        # config, or any not-yet-committed config entry (the appended stable
        # entry), both keep the cluster busy.
        if st.committed_config(self.initial_config).get("type") == CONFIG_JOINT:
            return True
        return any(
            entry.get("kind") == KIND_CONFIG and entry["index"] > st.commit_index
            for entry in st.log
        )

    def _record_membership_rejected(self, change: dict, reason: str) -> None:
        change_id = change["id"]
        self.change_results[change_id] = {
            "node": change["node"],
            "action": change["action"],
            "member": change["member"],
            "outcome": "rejected",
            "reason": reason,
        }
        self._record({
            "type": "membershipResult",
            "node": change["node"],
            "id": change_id,
            "action": change["action"],
            "member": change["member"],
            "result": "rejected",
            "reason": reason,
        })

    def _on_membership_change(self, change: dict) -> None:
        node = change["node"]
        change_id = change["id"]
        action = change["action"]
        member = change["member"]
        st = self.state[node]
        # Rejection precedence: receiver down -> not the leader -> another
        # change already running -> member state not applicable -> removing
        # would leave fewer than three voters.
        if not st.online:
            self._record_membership_rejected(change, "nodeDown")
            return
        if st.role != ROLE_LEADER:
            self._record_membership_rejected(change, "notLeader")
            return
        if self._change_in_progress(st, node):
            self._record_membership_rejected(change, "changeInProgress")
            return
        _, voters = self._config_groups(st.committed_config(self.initial_config))
        voters = frozenset(voters)
        if action == "add":
            if member in voters:
                self._record_membership_rejected(change, "alreadyMember")
                return
            new_voters = voters | {member}
        else:
            if member not in voters:
                self._record_membership_rejected(change, "notMember")
                return
            new_voters = voters - {member}
        if len(new_voters) < 3:
            self._record_membership_rejected(change, "minimumClusterSize")
            return

        self.change_results[change_id] = {
            "node": node,
            "action": action,
            "member": member,
            "outcome": "pending",
        }
        if action == "add":
            # The learner must first receive the log (or a snapshot) until it
            # matches the leader; only then is the joint config entry
            # appended. Heartbeats already stream entries to every node, but
            # replicate immediately so catch-up starts at once.
            self.pending_catchup = {
                "id": change_id,
                "member": member,
                "action": action,
                "leader": node,
            }
            self._record({
                "type": "membershipResult",
                "node": node,
                "id": change_id,
                "action": action,
                "member": member,
                "result": "accepted",
                "phase": "catchingUp",
            })
            self._replicate_to(node, member)
            self._progress_catchup(node)
        else:
            self._record({
                "type": "membershipResult",
                "node": node,
                "id": change_id,
                "action": action,
                "member": member,
                "result": "accepted",
            })
            self._append_joint_entry(node, change_id, action, member, voters)

    def _append_config_entry(
        self, name: str, change_id: str, entry_type: str, config: dict, action: str, member: str
    ) -> dict:
        st = self.state[name]
        entry = {
            "index": st.last_log_index() + 1,
            "term": st.term,
            "kind": KIND_CONFIG,
            "id": change_id,
            "entryType": entry_type,
            "config": self._store_config(config),
            "action": action,
            "member": member,
        }
        st.log.append(entry)
        return entry

    def _append_joint_entry(
        self, name: str, change_id: str, action: str, member: str, voters: frozenset[str]
    ) -> None:
        joint = self._joint_config(voters, action, member)
        self._append_config_entry(name, change_id, CONFIG_JOINT, joint, action, member)
        for peer in self.node_names:
            if peer != name:
                self._replicate_to(name, peer)

    def _progress_catchup(self, name: str) -> None:
        """Append the joint entry once the added learner has caught the
        leader's log end (via log replication or an installed snapshot)."""
        pending = self.pending_catchup
        if pending is None:
            return
        st = self.state[name]
        if st.role != ROLE_LEADER or pending["leader"] != name:
            return
        member = pending["member"]
        latest = st.latest_config(self.initial_config)
        if latest.get("type") == CONFIG_JOINT:
            # Another change already moved the cluster into a joint phase.
            return
        _, voters = self._config_groups(st.committed_config(self.initial_config))
        voters = frozenset(voters)
        if (pending["action"] == "add") == (member in voters):
            # The requested relationship no longer matches committed state.
            return
        if st.match_index.get(member, 0) < st.last_log_index():
            return
        self.pending_catchup = None
        self._append_joint_entry(name, pending["id"], pending["action"], member, voters)

    # -- application of committed configuration entries ----------------------

    @staticmethod
    def _has_following_stable(st: _Node, joint_entry: dict) -> bool:
        """Whether a stable entry for the same change already follows the
        joint entry in this node's log (e.g. appended when this leader first
        won the election)."""
        for log_entry in st.log:
            if (
                log_entry["index"] > joint_entry["index"]
                and log_entry.get("kind") == KIND_CONFIG
                and log_entry["entryType"] == CONFIG_STABLE
                and log_entry["id"] == joint_entry["id"]
            ):
                return True
        return False

    def _apply_config_entry(self, name: str, entry: dict) -> None:
        """React to a newly committed configuration: emit configurationApplied,
        start/stop election timers when membership changes, append the stable
        entry after a joint entry commits (on the leader), and make a removed
        leader step down once the stable configuration is committed."""
        st = self.state[name]
        config = entry["config"]
        old, new = self._config_groups(config)
        self._record({
            "type": "configurationApplied",
            "node": name,
            "index": entry["index"],
            "term": entry["term"],
            "id": entry["id"],
            "entryType": entry["entryType"],
            "config": config,
            "action": entry["action"],
            "member": entry["member"],
        })
        if entry["entryType"] == CONFIG_STABLE:
            # A stable entry keeps the new set in both halves, so the removed
            # members come from the preceding joint configuration (old set)
            # rather than from this entry itself.
            prior_old, _prior_new = self._config_groups(
                st.config_at(entry["index"], self.initial_config)
            )
            removed = prior_old - new
            result = self.change_results.get(entry["id"])
            if result is not None and result.get("outcome") != "committed":
                result["outcome"] = "committed"
                result["index"] = entry["index"]
                result["term"] = entry["term"]
                result.pop("jointPhase", None)
            # The first application of the stable entry is the moment the
            # change commits; later nodes applying it must not move the time.
            self.stable_commit_times.setdefault(entry["id"], self.now)
            # A leader removed by the change it proposed steps down at once;
            # its latest configuration no longer contains it, so it neither
            # campaigns nor votes afterwards.
            if name in removed and st.role == ROLE_LEADER:
                self._leader_removed(name, entry)
        else:
            result = self.change_results.get(entry["id"])
            if result is not None:
                result["jointPhase"] = {"id": entry["id"], "old": sorted(old), "new": sorted(new)}
            if st.role == ROLE_LEADER and not self._has_following_stable(st, entry):
                # The joint configuration is committed (or, for a leader that
                # won after the joint entry was already on both majorities, is
                # being caught up): append the stable configuration. A new
                # leader may already have appended one in _become_leader, in
                # which case it must not be duplicated.
                stable = self._stable_config(new)
                self._append_config_entry(
                    name, entry["id"], CONFIG_STABLE, stable, entry["action"], entry["member"]
                )
                for peer in self.node_names:
                    if peer != name:
                        self._replicate_to(name, peer)

    def _leader_removed(self, name: str, entry: dict) -> None:
        st = self.state[name]
        st.role = ROLE_FOLLOWER
        st.known_leader = None
        st.votes = set()
        st.next_index = {}
        st.match_index = {}
        # _reset_timeout cancels the timer for a now non-voting node.
        self._reset_timeout(name)
        self._record_state_change(name, "removedFromCluster")
        self._abandon_reads(name)

    def _restore_membership_from_snapshot(
        self,
        name: str,
        bundled_entries: list[dict],
        snap_config: dict | None,
        eligible_before: bool,
    ) -> None:
        """Restore voting membership and change outcomes from the applied
        entries folded into a received snapshot without re-applying them."""
        if snap_config is not None:
            eligible_after = self._is_voter(name, snap_config)
            if eligible_after and not eligible_before:
                self._reset_timeout(name)
            elif eligible_before and not eligible_after:
                self._invalidate_timeout(name)
        for bundled in bundled_entries:
            if bundled.get("kind") != KIND_CONFIG:
                continue
            change_id = bundled["id"]
            result = self.change_results.get(change_id)
            if result is None:
                continue
            if bundled["entryType"] == CONFIG_STABLE:
                if result.get("outcome") != "committed":
                    result["outcome"] = "committed"
                    result["index"] = bundled["index"]
                    result["term"] = bundled["term"]
                result.pop("jointPhase", None)
            else:
                old, new = self._config_groups(bundled["config"])
                result["jointPhase"] = {"id": change_id, "old": sorted(old), "new": sorted(new)}
