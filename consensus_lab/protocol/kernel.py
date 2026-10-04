"""Timeline recording and message dispatch: the shared kernel every
protocol domain runs on.

``_Kernel`` owns the two things no domain may reimplement: appending to the
single simulation timeline (``_record`` / ``_record_state_change``) and
routing a delivered message to its domain handler (``_deliver``). All
state changes, message sends, zero-delay cascades, commit applications and
liveness results flow through ``_record`` into the one timeline, in the
established deterministic order.

Provides (to every domain mixin):
    _record, _record_state_change, _deliver

Requires (from the composing simulator):
    ``timeline``, ``now``, ``state``, ``network`` and the per-domain
    message handlers mixed in alongside this class.
"""

from __future__ import annotations


class _Kernel:
    # -- timeline helpers -------------------------------------------------

    def _record(self, entry: dict) -> None:
        self.timeline.append({"seq": len(self.timeline) + 1, "time": self.now, **entry})

    def _record_state_change(self, name: str, reason: str) -> None:
        st = self.state[name]
        self._record({"type": "stateChange", "node": name, "term": st.term, "role": st.role, "reason": reason})

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
