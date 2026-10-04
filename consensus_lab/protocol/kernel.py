"""Timeline recording and message dispatch: the shared kernel every
protocol domain runs on.

``_Kernel`` owns the two things no domain may reimplement: appending to the
single simulation timeline (``_record`` / ``_record_state_change``) and
routing a delivered message to its domain handler (``_deliver`` /
``_route_message``). All
state changes, message sends, zero-delay cascades, commit applications and
liveness results flow through ``_record`` into the one timeline, in the
established deterministic order.

While the simulator runs a handler speculatively (a storage fault may fail
its persistence barrier), ``_record_buffer`` holds the handler's timeline
entries aside; they are either appended to the timeline in order or
discarded, so a failed barrier leaves no visible trace.

Provides (to every domain mixin):
    _record, _record_state_change, _deliver, _route_message

Requires (from the composing simulator):
    ``timeline``, ``now``, ``state``, ``network``, ``_record_buffer`` and
    the per-domain message handlers mixed in alongside this class.
"""

from __future__ import annotations


class _Kernel:
    # -- timeline helpers -------------------------------------------------

    def _record(self, entry: dict) -> None:
        seq = len(self.timeline) + 1
        if self._record_buffer is not None:
            # Speculative region: the entry is numbered as if already
            # appended and only reaches the timeline on commit.
            seq += len(self._record_buffer)
            self._record_buffer.append({"seq": seq, "time": self.now, **entry})
            return
        self.timeline.append({"seq": seq, "time": self.now, **entry})

    def _record_state_change(self, name: str, reason: str) -> None:
        st = self.state[name]
        self._record({"type": "stateChange", "node": name, "term": st.term, "role": st.role, "reason": reason})

    # -- message dispatch -----------------------------------------------------

    def _deliver(self, msg: dict) -> None:
        if not self.network.deliver(msg, self.state[msg["dst"]].online):
            return
        self._route_message(msg)

    def _route_message(self, msg: dict) -> None:
        """Dispatch an accepted envelope to its domain handler. Split from
        the delivery decision so the simulator can run the handler inside a
        storage-fault barrier."""
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
