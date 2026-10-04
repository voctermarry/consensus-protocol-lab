"""Elections and the optional pre-vote phase.

``_Elections`` owns every transition that changes who leads: the election
timer lifecycle (schedule / cancel / reconcile on membership change), the
follower/candidate/preCandidate/leader transitions, the pre-vote round
(probe, grant, promotion) and the handlers for the four vote messages.

A higher-term message always funnels through ``_become_follower``, which is
the single step-down point: it abandons the active pre-vote round (the round
fields are cleared here) and delegates the cancellation of the node's
pending reads to the reads domain (``_abandon_reads``), so one transition
keeps both invariants without either domain reaching into the other.

Provides (to sibling domains):
    _become_follower   — the one step-down transition (replication, reads)
    _reset_timeout / _invalidate_timeout / _reconcile_election_timer —
        election-timer control used when membership or lifecycle changes
        eligibility (replication, membership, lifecycle)

Requires (from sibling domains):
    config._can_vote / _is_voter / _has_vote_majorities — eligibility and
        quorum arithmetic
    kernel._record / _record_state_change — timeline recording
    reads._abandon_reads — fail pending reads when leadership is lost
    replication._send_heartbeats — a fresh leader's first broadcast
    membership._resume_membership_change — finish an open membership change

Requires (from the composing simulator):
    ``state``, ``queue``, ``network``, ``now``, ``timeouts``,
    ``node_names``, ``index``, ``initial_config``, ``pre_vote_enabled`` and
    ``leaders_by_term``.
"""

from __future__ import annotations

from ..events import _KIND_TIMEOUT
from ..node import ROLE_CANDIDATE, ROLE_FOLLOWER, ROLE_LEADER, ROLE_PRECANDIDATE


class _Elections:
    # -- election timers ----------------------------------------------------

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

    # -- state transitions --------------------------------------------------

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

    # -- vote message handlers ------------------------------------------------

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

    # -- election timeout event ------------------------------------------------

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
