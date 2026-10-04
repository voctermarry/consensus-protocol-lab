"""Node crash/restart lifecycle and the heartbeat timer event.

This mixin owns the nodeEvents transitions (crash keeps persisted state and
silences the node; restart rebuilds volatile state and re-arms the election
timer at the restart moment) and the leader heartbeat timer. The persisted
fields live on the single shared ``_Node`` objects, so a crash needs no
copying: only volatile fields are cleared in place.
"""

from __future__ import annotations

from ..node import ROLE_FOLLOWER, ROLE_LEADER


class _LifecycleKernel:
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
