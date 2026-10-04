"""Node state transitions: the protocol core of the simulation.

The :class:`_Protocol` mixin holds every transition a node can go through —
elections and the optional pre-vote phase, log replication and compaction,
joint-consensus membership changes, read-only queries, client commands, the
crash/restart lifecycle and liveness evaluation — plus the handlers for
every message kind the network can deliver.

The composing simulator provides the shared run-time state: ``state`` (node
name -> ``_Node``), ``queue`` (``EventQueue``), ``network`` (``Network``),
the timeline recorder ``_record`` and the bookkeeping dictionaries. Every
method here only mutates that state, in the established deterministic order.
"""

from __future__ import annotations

from .events import _KIND_HEARTBEAT, _KIND_TIMEOUT
from .node import (
    CONFIG_JOINT,
    CONFIG_STABLE,
    KIND_COMMAND,
    KIND_CONFIG,
    ROLE_CANDIDATE,
    ROLE_FOLLOWER,
    ROLE_LEADER,
    ROLE_PRECANDIDATE,
    _Node,
)


class _Protocol:
    # -- timeline helpers -------------------------------------------------

    def _record(self, entry: dict) -> None:
        self.timeline.append({"seq": len(self.timeline) + 1, "time": self.now, **entry})

    def _record_state_change(self, name: str, reason: str) -> None:
        st = self.state[name]
        self._record({"type": "stateChange", "node": name, "term": st.term, "role": st.role, "reason": reason})

    # -- configuration -------------------------------------------------------

    def _command_entry(self, index: int, term: int, command_id: str, command: object) -> dict:
        """A client-command log/applied entry. When membership changes are
        enabled it carries ``kind: "command"`` so configuration entries
        (``kind: "config"``) are unambiguously distinguishable; without the
        feature the historical, untyped shape is preserved byte for byte."""
        entry = {"index": index, "term": term}
        if self.membership_enabled:
            entry["kind"] = KIND_COMMAND
        entry["id"] = command_id
        entry["command"] = command
        return entry

    @staticmethod
    def _config_groups(config: dict) -> tuple[frozenset[str], frozenset[str]]:
        """The (old, new) voter sets of a configuration. Stable configurations
        carry the same set twice; joint configurations carry both."""
        return frozenset(config["old"]), frozenset(config["new"])

    def _config_quorums(self, config: dict) -> list[frozenset[str]]:
        old, new = self._config_groups(config)
        if config.get("type") == CONFIG_JOINT:
            return [old, new]
        return [new]

    def _is_voter(self, name: str, config: dict) -> bool:
        old, new = self._config_groups(config)
        return name in old or name in new

    def _can_vote(self, name: str) -> bool:
        """Current voting/election eligibility from the node's newest
        configuration: learners and nodes not present in the latest config
        neither vote nor campaign. A removed node that learns a configuration
        adding it back regains eligibility."""
        st = self.state[name]
        return self._is_voter(name, st.latest_config(self.initial_config))

    @staticmethod
    def _store_config(config: dict) -> dict:
        old, new = _Protocol._config_groups(config)
        return {
            "type": config.get("type", CONFIG_STABLE),
            "old": sorted(old),
            "new": sorted(new),
        }

    def _joint_config(self, voters: frozenset[str], action: str, member: str) -> dict:
        if action == "add":
            new_voters = voters | {member}
        else:
            new_voters = voters - {member}
        return {"type": CONFIG_JOINT, "old": sorted(voters), "new": sorted(new_voters)}

    @staticmethod
    def _stable_config(voters: frozenset[str]) -> dict:
        ordered = sorted(voters)
        return {"type": CONFIG_STABLE, "old": ordered, "new": ordered}

    def _has_vote_majorities(self, candidate: str, votes: set[str], config: dict) -> bool:
        for group in self._config_quorums(config):
            needed = len(group) // 2 + 1
            if len([v for v in votes if v in group]) < needed:
                return False
        return True

    def _replicated_by_majorities(self, leader: str, index: int, config: dict) -> bool:
        """Whether ``index`` is present on a strict majority of every quorum
        group (the leader itself counts in any group it belongs to)."""
        st = self.state[leader]
        for group in self._config_quorums(config):
            needed = len(group) // 2 + 1
            points = []
            for peer in group:
                if peer == leader:
                    points.append(st.last_log_index())
                else:
                    points.append(st.match_index.get(peer, 0))
            if len([point for point in points if point >= index]) < needed:
                return False
        return True

    # -- state transitions --------------------------------------------------

    def _invalidate_timeout(self, name: str) -> None:
        self.state[name].timeout_gen += 1

    def _reset_timeout(self, name: str) -> None:
        st = self.state[name]
        if not self._can_vote(name):
            # Learners and removed members hold no election timer; bumping the
            # generation cancels any timer scheduled before their membership
            # changed.
            st.timeout_gen += 1
            return
        st.timeout_gen += 1
        self.queue.push(
            (self.now + self.timeouts[name], _KIND_TIMEOUT, self.index[name], (name, st.timeout_gen)),
        )

    def _reconcile_election_timer(self, name: str, eligible_before: bool) -> None:
        """Start or cancel a node's election timer when its configuration
        membership changes (e.g. a learner promoted by a replicated joint
        entry, or a member removed by a replicated stable entry)."""
        eligible_after = self._can_vote(name)
        if eligible_after and not eligible_before:
            self._reset_timeout(name)
        elif eligible_before and not eligible_after:
            self._invalidate_timeout(name)

    def _become_follower(self, name: str, term: int, reason: str) -> None:
        st = self.state[name]
        st.role = ROLE_FOLLOWER
        st.term = term
        st.voted_for = None
        st.known_leader = None
        st.votes = set()
        st.next_index = {}
        st.match_index = {}
        # The active round is abandoned; the round counter itself is a
        # lifetime-monotonic nonce so a reply in flight from a superseded
        # round (including one that straddles a restart) can never match a
        # later round.
        st.pre_term = 0
        st.pre_votes = set()
        st.pre_config = None
        # A new term means the previous term's leader contact no longer
        # counts; the heartbeat/appendEntries/installSnapshot handlers mark a
        # fresh contact immediately after stepping down on a leader message.
        st.leader_contact = -1
        self._reset_timeout(name)
        self._record_state_change(name, reason)
        # A deposed leader can no longer vouch for reads it accepted.
        self._abandon_reads(name)

    def _start_election(self, name: str) -> None:
        st = self.state[name]
        # The election is decided under the configuration in place just
        # before the candidate appends anything (joint configs require
        # majorities of both constituent sets).
        config = st.latest_config(self.initial_config)
        st.role = ROLE_CANDIDATE
        st.term += 1
        st.voted_for = name
        st.known_leader = None
        st.votes = {name}
        st.next_index = {}
        st.match_index = {}
        # Winning election traffic is not leader contact: nothing has
        # contacted this node from the new term yet.
        st.leader_contact = -1
        self._reset_timeout(name)
        self._record_state_change(name, "electionTimeout")
        for peer in self.node_names:
            if peer != name and self._is_voter(peer, config):
                self.network.send(
                    name,
                    peer,
                    {
                        "kind": "requestVote",
                        "term": st.term,
                        "lastLogIndex": st.last_log_index(),
                        "lastLogTerm": st.last_log_term(),
                    },
                )

    def _start_pre_vote(self, name: str) -> None:
        """Enter the pre-candidate phase for a new round. The current term,
        votedFor and the log are all left untouched; only a prospective term
        of term + 1 and a round number are attached to the probes."""
        st = self.state[name]
        config = st.latest_config(self.initial_config)
        st.role = ROLE_PRECANDIDATE
        st.pre_round += 1
        st.pre_term = st.term + 1
        st.pre_votes = {name}
        st.pre_config = config
        self._reset_timeout(name)
        self._record_state_change(name, "electionTimeout")
        for peer in self.node_names:
            if peer != name and self._is_voter(peer, config):
                self.network.send(
                    name,
                    peer,
                    {
                        "kind": "preVote",
                        "term": st.term,
                        "prospectiveTerm": st.pre_term,
                        "round": st.pre_round,
                        "lastLogIndex": st.last_log_index(),
                        "lastLogTerm": st.last_log_term(),
                    },
                )

    def _promote_from_pre_vote(self, name: str) -> None:
        """A pre-vote round won the required majorities: now start the real
        election, raising the term to the prospective term and persisting the
        self-vote, and broadcast RequestVote in that term."""
        st = self.state[name]
        config = st.pre_config
        prospective = st.pre_term
        st.role = ROLE_CANDIDATE
        st.pre_term = 0
        st.pre_votes = set()
        st.pre_config = None
        st.term = prospective
        st.voted_for = name
        st.known_leader = None
        st.votes = {name}
        st.next_index = {}
        st.match_index = {}
        # Promotion is election traffic, not leader contact.
        st.leader_contact = -1
        self._reset_timeout(name)
        # The transition to a real candidate in the prospective term is the
        # only state event the promotion needs; RequestVote traffic follows.
        self._record_state_change(name, "preVoteMajority")
        for peer in self.node_names:
            if peer != name and self._is_voter(peer, config):
                self.network.send(
                    name,
                    peer,
                    {
                        "kind": "requestVote",
                        "term": st.term,
                        "lastLogIndex": st.last_log_index(),
                        "lastLogTerm": st.last_log_term(),
                    },
                )

    def _become_leader(self, name: str, config: dict) -> None:
        st = self.state[name]
        st.role = ROLE_LEADER
        st.known_leader = name
        st.votes = set()
        st.pre_term = 0
        st.pre_votes = set()
        st.pre_config = None
        st.timeout_gen += 1  # leaders have no election timeout
        next_index = st.last_log_index() + 1
        # Replication progress is tracked for every other node: voters count
        # towards quorums, learners receive the log but never count.
        st.next_index = {peer: next_index for peer in self.node_names if peer != name}
        st.match_index = {peer: 0 for peer in self.node_names if peer != name}
        self._record_state_change(name, "majority")
        leaders = self.leaders_by_term.setdefault(st.term, [])
        if name not in leaders:
            leaders.append(name)
        self._send_heartbeats(name)
        self._resume_membership_change(name)

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

    # -- message dispatch -----------------------------------------------------

    def _deliver(self, msg: dict) -> None:
        if not self.network.deliver(msg, self.state[msg["dst"]].online):
            return
        kind = msg["kind"]
        if kind == "requestVote":
            self._handle_request_vote(msg)
        elif kind == "voteReply":
            self._handle_vote_reply(msg)
        elif kind == "preVote":
            self._handle_pre_vote(msg)
        elif kind == "preVoteReply":
            self._handle_pre_vote_reply(msg)
        elif kind == "heartbeat":
            self._handle_heartbeat(msg)
        elif kind == "appendEntries":
            self._handle_append_entries(msg)
        elif kind == "installSnapshot":
            self._handle_install_snapshot(msg)
        elif kind == "installSnapshotReply":
            self._handle_snapshot_reply(msg)
        elif kind == "readProbe":
            self._handle_read_probe(msg)
        elif kind == "readReply":
            self._handle_read_reply(msg)
        else:
            self._handle_append_reply(msg)

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

    def _handle_request_vote(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        # A higher term clears voted_for before the vote decision is made.
        effective_voted_for = None if msg["term"] > st.term else st.voted_for
        up_to_date = (
            msg["lastLogTerm"] > st.last_log_term()
            or (
                msg["lastLogTerm"] == st.last_log_term()
                and msg["lastLogIndex"] >= st.last_log_index()
            )
        )
        # Learners and removed members never grant a vote; candidates only
        # solicit voters, so stale RVs reaching a demoted node are denied.
        eligible_voter = self._can_vote(dst)
        granted = (
            eligible_voter
            and msg["term"] >= st.term
            and up_to_date
            and (effective_voted_for is None or effective_voted_for == src)
        )
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "requestVote",
            "term": msg["term"],
            "result": "delivered",
            "detail": "voteGranted" if granted else "voteDenied",
        })
        if msg["term"] > st.term:
            self._become_follower(dst, msg["term"], "higherTermMessage")
        if granted:
            st.voted_for = src
        self.network.send(dst, src, {"kind": "voteReply", "term": st.term, "granted": granted})

    def _handle_vote_reply(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        detail = "ignored"
        elected = False
        election_config = st.latest_config(self.initial_config)
        if msg["term"] > st.term:
            detail = "higherTerm"
        elif st.role == ROLE_CANDIDATE and msg["term"] == st.term and msg["granted"]:
            st.votes.add(src)
            detail = "voteCounted"
            elected = self._has_vote_majorities(dst, st.votes, election_config)
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "voteReply",
            "term": msg["term"],
            "result": "delivered",
            "detail": detail,
        })
        if msg["term"] > st.term:
            self._become_follower(dst, msg["term"], "higherTermMessage")
        elif elected:
            self._become_leader(dst, election_config)

    # -- pre-vote (optional phase) ------------------------------------------

    def _pre_vote_deadline_passed(self, name: str) -> bool:
        """Whether a current-term leader contact is old enough that the local
        election deadline has elapsed. No recorded contact (startup, restart
        or term entered through election traffic) always passes."""
        contact = self.state[name].leader_contact
        return contact < 0 or self.now >= contact + self.timeouts[name]

    def _handle_pre_vote(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        up_to_date = (
            msg["lastLogTerm"] > st.last_log_term()
            or (
                msg["lastLogTerm"] == st.last_log_term()
                and msg["lastLogIndex"] >= st.last_log_index()
            )
        )
        # A pre-vote never changes the receiver's term, vote or timer; it is
        # granted solely on merit and on the absence of a recently heard
        # current-term leader. Learners and removed members have no vote, the
        # current leader never grants one, a stale prospective term is
        # refused, and a node that heard a viable leader within its election
        # deadline refuses so it cannot help a partitioned peer disrupt the
        # stable leader.
        granted = (
            self._can_vote(dst)
            and st.role != ROLE_LEADER
            and msg["prospectiveTerm"] >= st.term + 1
            and up_to_date
            and self._pre_vote_deadline_passed(dst)
        )
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "preVote",
            "term": msg["term"],
            "round": msg["round"],
            "prospectiveTerm": msg["prospectiveTerm"],
            "result": "delivered",
            "detail": "granted" if granted else "rejected",
        })
        # Reply with the receiver's *current* (unchanged) term so a
        # pre-candidate facing a higher real term learns it and stands down.
        self.network.send(
            dst,
            src,
            {
                "kind": "preVoteReply",
                "term": st.term,
                "prospectiveTerm": msg["prospectiveTerm"],
                "round": msg["round"],
                "granted": granted,
            },
        )

    def _handle_pre_vote_reply(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        detail = "staleRound"
        won = False
        if msg["term"] > st.term:
            # A reply carrying a higher real term ends the round outright.
            detail = "higherTerm"
        elif (
            st.role == ROLE_PRECANDIDATE
            and msg["round"] == st.pre_round
            and msg["prospectiveTerm"] == st.pre_term
        ):
            if msg["granted"]:
                st.pre_votes.add(src)
                detail = "granted"
                if self._can_vote(dst) and self._has_vote_majorities(
                    dst, st.pre_votes, st.pre_config
                ):
                    won = True
            else:
                detail = "rejected"
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "preVoteReply",
            "term": msg["term"],
            "round": msg["round"],
            "prospectiveTerm": msg["prospectiveTerm"],
            "result": "delivered",
            "detail": detail,
        })
        if msg["term"] > st.term:
            self._become_follower(dst, msg["term"], "higherTermMessage")
        elif won:
            # Still online (delivery ensured it), eligible, same round and
            # unchanged term: raise the term to the prospective term and open
            # the real election.
            self._promote_from_pre_vote(dst)

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

    # -- read-only queries ----------------------------------------------------

    @staticmethod
    def _read_state_entry(entry: dict) -> dict:
        """The read-state view of one applied client command. Configuration
        entries never appear in a read state."""
        return {
            "index": entry["index"],
            "term": entry["term"],
            "id": entry["id"],
            "command": entry["command"],
        }

    def _on_read_query(self, query: dict) -> None:
        node = query["node"]
        query_id = query["id"]
        st = self.state[node]
        if not st.online:
            self.read_results[query_id] = {
                "outcome": "rejected",
                "reason": "nodeDown",
                "knownLeader": None,
            }
            self._record({
                "type": "readResult",
                "node": node,
                "id": query_id,
                "result": "rejected",
                "reason": "nodeDown",
                "knownLeader": None,
            })
            return
        if st.role != ROLE_LEADER:
            self.read_results[query_id] = {
                "outcome": "rejected",
                "reason": "notLeader",
                "knownLeader": st.known_leader,
            }
            self._record({
                "type": "readResult",
                "node": node,
                "id": query_id,
                "result": "rejected",
                "reason": "notLeader",
                "knownLeader": st.known_leader,
            })
            return

        # Accepted: the read linearizes at the leader's current committed (and
        # therefore applied) position once a same-term majority of the
        # configuration recorded here confirms the leader's authority.
        read_index = st.commit_index
        self.pending_reads[query_id] = {
            "node": node,
            "term": st.term,
            "readIndex": read_index,
            "config": st.latest_config(self.initial_config),
            "acks": {node},
        }
        self.read_required[query_id] = set(self.committed_command_ids)
        self._record({
            "type": "readResult",
            "node": node,
            "id": query_id,
            "result": "accepted",
            "term": st.term,
            "readIndex": read_index,
        })
        for peer in self.node_names:
            if peer != node:
                self.network.send(
                    node,
                    peer,
                    {
                        "kind": "readProbe",
                        "term": st.term,
                        "id": query_id,
                        "readIndex": read_index,
                    },
                )
        self._check_read_completion(query_id)

    def _check_read_completion(self, query_id: str) -> None:
        """Complete a pending read once a strict majority of every quorum
        group of the recorded configuration (stable: the voter set; joint:
        both the old and the new sets; learners never count) has acknowledged
        the probe in the same term."""
        read = self.pending_reads.get(query_id)
        if read is None:
            return
        name = read["node"]
        st = self.state[name]
        if st.role != ROLE_LEADER or st.term != read["term"]:
            return
        if not self._has_vote_majorities(name, read["acks"], read["config"]):
            return
        read_index = read["readIndex"]
        state = [
            self._read_state_entry(entry)
            for entry in st.applied[:read_index]
            if entry.get("kind") != KIND_CONFIG
        ]
        del self.pending_reads[query_id]
        self.read_results[query_id] = {
            "outcome": "completed",
            "term": read["term"],
            "readIndex": read_index,
            "state": state,
        }
        self._record({
            "type": "readResult",
            "node": name,
            "id": query_id,
            "result": "completed",
            "term": read["term"],
            "readIndex": read_index,
        })

    def _abandon_reads(self, name: str) -> None:
        """Fail every read still pending on a node that stopped being leader
        (stepped down, was removed from the cluster, or went offline)."""
        doomed = [
            query_id
            for query_id, read in self.pending_reads.items()
            if read["node"] == name
        ]
        for query_id in doomed:
            del self.pending_reads[query_id]
            self.read_results[query_id] = {
                "outcome": "rejected",
                "reason": "leadershipLost",
                "knownLeader": None,
            }
            self._record({
                "type": "readResult",
                "node": name,
                "id": query_id,
                "result": "rejected",
                "reason": "leadershipLost",
                "knownLeader": None,
            })

    def _handle_read_probe(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        accepted = msg["term"] >= st.term
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "readProbe",
            "term": msg["term"],
            "id": msg["id"],
            "result": "delivered",
            "detail": "accepted" if accepted else "staleTerm",
        })
        if accepted:
            if msg["term"] > st.term or st.role != ROLE_FOLLOWER:
                self._become_follower(dst, msg["term"], "readProbe")
            else:
                self._reset_timeout(dst)
            st.known_leader = src
        self.network.send(
            dst,
            src,
            {
                "kind": "readReply",
                "term": st.term,
                "id": msg["id"],
                "granted": accepted,
            },
        )

    def _handle_read_reply(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        query_id = msg["id"]
        if msg["term"] > st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "readReply",
                "term": msg["term"],
                "id": query_id,
                "result": "delivered",
                "detail": "higherTerm",
            })
            self._become_follower(dst, msg["term"], "higherTermMessage")
            return
        # Only a granted reply from the very round that is still pending —
        # same query id, same leader, same term — counts; replies from older
        # or newer rounds are ignored and never mixed in.
        read = self.pending_reads.get(query_id)
        counted = (
            st.role == ROLE_LEADER
            and msg["term"] == st.term
            and msg.get("granted", False)
            and read is not None
            and read["node"] == dst
            and read["term"] == st.term
        )
        if counted:
            read["acks"].add(src)
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "readReply",
            "term": msg["term"],
            "id": query_id,
            "result": "delivered",
            "detail": "acknowledged" if counted else "ignored",
        })
        if counted:
            self._check_read_completion(query_id)

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

    # -- membership changes --------------------------------------------------

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
        entry = self._append_config_entry(name, change_id, CONFIG_JOINT, joint, action, member)
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

    # -- timeouts, heartbeats, node lifecycle -----------------------------------------------------------------

    def _on_timeout(self, name: str, generation: int) -> None:
        st = self.state[name]
        if not st.online or generation != st.timeout_gen or st.role == ROLE_LEADER:
            return
        if not self._can_vote(name):
            # A learner's or removed member's stray timer must never start an
            # election.
            return
        self._record({"type": "timeout", "node": name, "term": st.term, "reason": "electionTimeout"})
        if self.pre_vote_enabled:
            # Both the first timeout and every later timeout (a failed
            # pre-vote round, or a real election that did not converge) open a
            # fresh pre-vote round; the term stays untouched until a round is
            # won.
            self._start_pre_vote(name)
        else:
            self._start_election(name)

    def _on_heartbeat(self, name: str, term: int) -> None:
        st = self.state[name]
        if st.online and st.role == ROLE_LEADER and st.term == term:
            self._send_heartbeats(name)

    def _on_node_event(self, event: dict) -> None:
        name = event["node"]
        st = self.state[name]
        if event["action"] == "crash":
            # The node goes silent: it sends nothing, processes nothing and
            # fires no timeouts or heartbeats until it restarts. Persisted
            # state (term, voted_for, log, commit_index, last_applied,
            # applied) is kept as-is.
            st.online = False
            st.timeout_gen += 1  # invalidate any pending election timeout
            # A crash cancels any open pre-vote round; its prospective term,
            # votes and electorate are volatile and not retained. The round
            # counter survives only as an in-memory generation (like
            # timeout_gen) so a reply that straddles the restart can never
            # match a later round.
            st.pre_term = 0
            st.pre_votes = set()
            st.pre_config = None
            st.leader_contact = -1
            self._record({"type": "nodeLifecycle", "node": name, "action": "crash"})
            # A crashed leader cannot confirm the reads it accepted.
            self._abandon_reads(name)
            return
        # Restart: persisted state survives; volatile state is reset. The
        # node comes back as a follower with no known leader, no candidate
        # votes and no leader replication progress, and its election timeout
        # is measured from the restart moment. Recovered entries are not
        # re-applied because last_applied/applied were restored with the log.
        st.online = True
        st.restart_count += 1
        st.role = ROLE_FOLLOWER
        st.known_leader = None
        st.votes = set()
        st.next_index = {}
        st.match_index = {}
        # The active pre-vote round is gone: prospective term, votes and
        # electorate are all volatile. The round counter only continues as an
        # in-memory generation (like timeout_gen) so a reply that straddles
        # the restart can never match a later round; the node waits a full
        # fresh timeout before campaigning again.
        st.pre_term = 0
        st.pre_votes = set()
        st.pre_config = None
        st.leader_contact = -1
        self._reset_timeout(name)
        self._record({"type": "nodeLifecycle", "node": name, "action": "restart"})

    # -- liveness checks -------------------------------------------------------

    def _has_online_leader(self) -> bool:
        return any(
            st.online and st.role == ROLE_LEADER for st in self.state.values()
        )

    def _command_present_anywhere(self, command_id: str) -> bool:
        """Whether an accepted command still survives in some node's applied
        history or uncompacted log suffix."""
        for st in self.state.values():
            if any(entry.get("id") == command_id for entry in st.applied):
                return True
            if any(entry.get("id") == command_id for entry in st.log):
                return True
        return False

    def _liveness_satisfied(self, check: dict) -> bool:
        check_type = check["type"]
        target = check["target"]
        if check_type == "leaderElected":
            return self.leader_online
        if check_type == "clientCommitted":
            return target in self.committed_command_ids
        if check_type == "readCompleted":
            result = self.read_results.get(target)
            return result is not None and result.get("outcome") == "completed"
        # membershipCommitted: the stable configuration entry has been applied
        # (the change committed).
        return target in self.stable_commit_times

    def _liveness_failure_reason(self, check: dict) -> str | None:
        """A terminal reason once the target can never satisfy the check;
        None while satisfaction is still possible."""
        check_type = check["type"]
        target = check["target"]
        if check_type == "clientCommitted":
            if target in self.rejected:
                return "targetRejected"
            if (
                target in self.accepted_command_ids
                and target not in self.committed_command_ids
                and not self._command_present_anywhere(target)
            ):
                # Accepted earlier, but its uncommitted entry was truncated by
                # a higher-term leader: it can never commit anymore.
                return "targetSuperseded"
            return None
        if check_type == "readCompleted":
            result = self.read_results.get(target)
            if result is not None and result.get("outcome") == "rejected":
                return "targetRejected"
            return None
        if check_type == "membershipCommitted":
            result = self.change_results.get(target)
            if result is not None and result.get("outcome") == "rejected":
                return "targetRejected"
            return None
        return None

    def _evaluate_liveness(self, time: int) -> None:
        """Run every open check whose window contains this timestamp, in
        input order, against the fully drained state at ``time``."""
        self.leader_online = self._has_online_leader()
        for check in self.liveness_checks:
            check_id = check["id"]
            if check_id in self.liveness_results:
                continue
            if time < check["startTime"] or time > check["deadline"]:
                continue
            result = {"id": check_id, "checkType": check["type"]}
            if check["target"] is not None:
                result["target"] = check["target"]
            if not self._liveness_satisfied(check):
                if time < check["deadline"]:
                    # The first satisfying moment may still arrive later in
                    # the window; a terminal target failure likewise surfaces
                    # as the reason at the deadline.
                    continue
                result["status"] = "failed"
                result["time"] = time
                result["reason"] = self._liveness_failure_reason(check) or "deadlineExceeded"
            else:
                result["status"] = "satisfied"
                result["time"] = time
            self.liveness_results[check_id] = result
            entry = {"type": "livenessResult", "id": check_id, "checkType": result["checkType"]}
            if check["target"] is not None:
                entry["target"] = check["target"]
            entry["status"] = result["status"]
            if result["status"] == "failed":
                entry["reason"] = result["reason"]
            self._record(entry)

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

