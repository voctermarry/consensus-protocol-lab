"""Node lifecycle: crash and restart.

``_Lifecycle`` owns the online/offline transitions. A crash silences the
node (it sends nothing, processes nothing and fires no timers) while its
persisted state — term, vote, log, snapshot, commit/apply progress — is
kept as-is; a restart brings the node back as a follower with all volatile
state (role, votes, replication progress, pre-vote round, leader-contact
clock) cleared and a fresh election timeout measured from the restart
moment. Reads the node was still vouching for are failed through the reads
domain's hook.

Provides:
    _on_node_event — the crash/restart event entry point (simulator loop)

Requires (from sibling domains):
    elections._reset_timeout — schedule the fresh post-restart timer
    reads._abandon_reads — a crashed leader cannot confirm reads
    kernel._record

Requires (from the composing simulator):
    ``state``.
"""

from __future__ import annotations

from ..node import ROLE_FOLLOWER


class _Lifecycle:
    # -- crash/restart ----------------------------------------------------------

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
