"""Read-only queries with leader-confirmation probes.

``_Reads`` owns the read-only query path: acceptance on the serving leader
(recording the term, the committed read index and the configuration the
confirmation is decided under), the readProbe/readReply exchange, majority
confirmation and completion, and the abandonment of reads whose leader is
gone.

The confirmation quorum is computed with the configuration primitives, so a
read accepted while a joint configuration is in force requires majorities
of both voter sets. Step-downs and crashes reach this domain through one
hook, ``_abandon_reads``, called by the elections and lifecycle domains;
the read domain never inspects election state beyond the leader/term check
in ``_check_read_completion``.

Provides (to sibling domains and the report layer):
    _abandon_reads    — fail every read pending on one node (elections,
        membership, lifecycle)
    _read_state_entry — the read-state view of one applied command (report)

Requires (from sibling domains):
    config._has_vote_majorities — confirmation quorum arithmetic
    elections._become_follower / _reset_timeout — a probe from a live
        leader is leader contact
    kernel._record

Requires (from the composing simulator):
    ``state``, ``network``, ``node_names``, ``initial_config``,
    ``pending_reads``, ``read_results``, ``read_required`` and
    ``committed_command_ids``.
"""

from __future__ import annotations

from ..node import KIND_CONFIG, ROLE_FOLLOWER, ROLE_LEADER


class _Reads:
    # -- query acceptance and completion ---------------------------------------

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

    # -- probe message handlers --------------------------------------------------

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
