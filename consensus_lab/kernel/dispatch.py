"""Incoming-message dispatch.

The network layer decides whether an envelope reaches its destination; once
it does, this mixin routes the envelope to the handler owned by the kernel
responsible for the message kind. Dispatch itself holds no state: every
handler mutates the single shared simulator state and records into the one
timeline.
"""

from __future__ import annotations


class _DispatchKernel:
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
