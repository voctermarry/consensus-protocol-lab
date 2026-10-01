"""Deterministic Raft leader-election and log-replication simulation.

The simulation advances virtual time only: it never reads the wall clock and
never uses randomness, so identical input produces byte-identical output.
"""

from __future__ import annotations

import heapq
import json

ROLE_FOLLOWER = "follower"
ROLE_CANDIDATE = "candidate"
ROLE_LEADER = "leader"

# Event kinds, processed in this order when they share a timestamp.
_KIND_FAULT = 0
_KIND_MESSAGE = 1
_KIND_CLIENT = 2
# Replication traffic triggered by client commands (and its replies) lands
# here so that, even with zero message delay, all client commands sharing a
# timestamp are handled in input order before their reactions drain.
_KIND_REACTION = 3
_KIND_TIMEOUT = 4
_KIND_HEARTBEAT = 5

_TOP_LEVEL_FIELDS = {
    "nodes",
    "duration",
    "electionTimeouts",
    "heartbeatInterval",
    "messageDelay",
    "faults",
    "clientCommands",
}
_REQUIRED_FIELDS = _TOP_LEVEL_FIELDS - {"faults", "clientCommands"}
_FAULT_FIELDS = {"time", "action", "groups"}
_CLIENT_COMMAND_FIELDS = {"time", "node", "id", "command"}


class ScenarioError(Exception):
    """The scenario input is invalid."""


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_int(value: object, name: str, minimum: int) -> int:
    if not _is_int(value):
        raise ScenarioError(f"{name} must be an integer")
    if value < minimum:
        if minimum > 0:
            raise ScenarioError(f"{name} must be a positive integer")
        raise ScenarioError(f"{name} must be a non-negative integer")
    return value


def parse_scenario(raw: object) -> dict:
    """Validate the decoded JSON scenario and return a normalized config."""
    if not isinstance(raw, dict):
        raise ScenarioError("scenario must be a JSON object")
    unknown = sorted(set(raw) - _TOP_LEVEL_FIELDS)
    if unknown:
        raise ScenarioError(f"unknown field(s): {', '.join(unknown)}")
    missing = sorted(_REQUIRED_FIELDS - set(raw))
    if missing:
        raise ScenarioError(f"missing field(s): {', '.join(missing)}")

    nodes = raw["nodes"]
    if not isinstance(nodes, list) or len(nodes) < 3:
        raise ScenarioError("nodes must be a list of at least three names")
    for name in nodes:
        if not isinstance(name, str) or not name:
            raise ScenarioError("node names must be non-empty strings")
    if len(set(nodes)) != len(nodes):
        raise ScenarioError("node names must be unique")

    duration = _require_int(raw["duration"], "duration", 0)
    heartbeat = _require_int(raw["heartbeatInterval"], "heartbeatInterval", 1)
    delay = _require_int(raw["messageDelay"], "messageDelay", 0)

    timeouts_raw = raw["electionTimeouts"]
    if _is_int(timeouts_raw):
        _require_int(timeouts_raw, "electionTimeouts", 1)
        timeouts = {name: timeouts_raw for name in nodes}
    elif isinstance(timeouts_raw, dict):
        unknown_nodes = sorted(set(timeouts_raw) - set(nodes))
        if unknown_nodes:
            raise ScenarioError(
                f"electionTimeouts references unknown node(s): {', '.join(unknown_nodes)}"
            )
        missing_nodes = sorted(set(nodes) - set(timeouts_raw))
        if missing_nodes:
            raise ScenarioError(
                f"electionTimeouts is missing node(s): {', '.join(missing_nodes)}"
            )
        timeouts = {}
        for name in nodes:
            timeouts[name] = _require_int(timeouts_raw[name], f"electionTimeouts.{name}", 1)
    else:
        raise ScenarioError("electionTimeouts must be a positive integer or an object mapping node names to positive integers")

    faults = raw.get("faults", [])
    if not isinstance(faults, list):
        raise ScenarioError("faults must be a list")
    normalized_faults = []
    node_set = set(nodes)
    for index, fault in enumerate(faults):
        label = f"faults[{index}]"
        if not isinstance(fault, dict):
            raise ScenarioError(f"{label} must be an object")
        unknown = sorted(set(fault) - _FAULT_FIELDS)
        if unknown:
            raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
        if "time" not in fault or "action" not in fault:
            raise ScenarioError(f"{label} must have time and action")
        time = _require_int(fault["time"], f"{label}.time", 0)
        if time > duration:
            raise ScenarioError(f"{label}.time is beyond the simulation duration")
        action = fault["action"]
        if action == "partition":
            if "groups" not in fault:
                raise ScenarioError(f"{label} partition requires groups")
            groups = fault["groups"]
            if not isinstance(groups, list) or len(groups) != 2:
                raise ScenarioError(f"{label}.groups must contain exactly two groups")
            seen: set[str] = set()
            normalized_groups = []
            for group in groups:
                if not isinstance(group, list) or not group:
                    raise ScenarioError(f"{label}.groups entries must be non-empty lists")
                for member in group:
                    if member not in node_set:
                        raise ScenarioError(f"{label}.groups references unknown node: {member!r}")
                    if member in seen:
                        raise ScenarioError(f"{label}.groups must not overlap")
                    seen.add(member)
                normalized_groups.append(list(group))
            if seen != node_set:
                raise ScenarioError(f"{label}.groups must cover every node")
            normalized_faults.append({"time": time, "action": "partition", "groups": normalized_groups})
        elif action == "heal":
            if "groups" in fault:
                raise ScenarioError(f"{label} heal must not have groups")
            normalized_faults.append({"time": time, "action": "heal"})
        else:
            raise ScenarioError(f"{label}.action must be partition or heal")

    commands = raw.get("clientCommands", [])
    if not isinstance(commands, list):
        raise ScenarioError("clientCommands must be a list")
    normalized_commands = []
    seen_ids: set[str] = set()
    for index, command in enumerate(commands):
        label = f"clientCommands[{index}]"
        if not isinstance(command, dict):
            raise ScenarioError(f"{label} must be an object")
        unknown = sorted(set(command) - _CLIENT_COMMAND_FIELDS)
        if unknown:
            raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
        missing = sorted(_CLIENT_COMMAND_FIELDS - set(command))
        if missing:
            raise ScenarioError(f"{label} missing field(s): {', '.join(missing)}")
        time = _require_int(command["time"], f"{label}.time", 0)
        if time > duration:
            raise ScenarioError(f"{label}.time is beyond the simulation duration")
        node = command["node"]
        if not isinstance(node, str) or not node:
            raise ScenarioError(f"{label}.node must be a non-empty string")
        if node not in node_set:
            raise ScenarioError(f"{label}.node references unknown node: {node!r}")
        command_id = command["id"]
        if not isinstance(command_id, str) or not command_id:
            raise ScenarioError(f"{label}.id must be a non-empty string")
        if command_id in seen_ids:
            raise ScenarioError(f"{label}.id duplicates a previous id: {command_id!r}")
        seen_ids.add(command_id)
        # "command" may be any JSON value; decoding already guaranteed validity.
        normalized_commands.append(
            {"time": time, "node": node, "id": command_id, "command": command["command"]}
        )

    return {
        "nodes": list(nodes),
        "duration": duration,
        "electionTimeouts": timeouts,
        "heartbeatInterval": heartbeat,
        "messageDelay": delay,
        "faults": normalized_faults,
        "clientCommands": normalized_commands,
    }


class _Node:
    __slots__ = (
        "role",
        "term",
        "voted_for",
        "known_leader",
        "votes",
        "timeout_gen",
        "log",
        "commit_index",
        "last_applied",
        "applied",
        "next_index",
        "match_index",
    )

    def __init__(self) -> None:
        self.role = ROLE_FOLLOWER
        self.term = 0
        self.voted_for: str | None = None
        self.known_leader: str | None = None
        self.votes: set[str] = set()
        self.timeout_gen = 0
        # Log entries are 1-indexed: {"index", "term", "id", "command"}.
        self.log: list[dict] = []
        self.commit_index = 0
        self.last_applied = 0
        self.applied: list[dict] = []
        # Leader-only replication progress, keyed by peer name.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

    def last_log_index(self) -> int:
        return len(self.log)

    def last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else 0

    def term_at(self, index: int) -> int:
        if index <= 0:
            return 0
        return self.log[index - 1]["term"]


class _Simulator:
    def __init__(self, config: dict) -> None:
        self.node_names: list[str] = config["nodes"]
        self.duration: int = config["duration"]
        self.timeouts: dict[str, int] = config["electionTimeouts"]
        self.heartbeat_interval: int = config["heartbeatInterval"]
        self.delay: int = config["messageDelay"]
        self.faults: list[dict] = config["faults"]
        self.commands: list[dict] = config["clientCommands"]
        self.state = {name: _Node() for name in self.node_names}
        self.index = {name: i for i, name in enumerate(self.node_names)}
        self.majority = len(self.node_names) // 2 + 1
        self.timeline: list[dict] = []
        self.leaders_by_term: dict[int, list[str]] = {}
        self.partition: list[frozenset[str]] | None = None
        self.queue: list[tuple] = []
        self.send_counter = 0
        self.now = 0
        # While draining client commands (and the replication cascade they
        # trigger), new messages land in the REACTION phase instead of the
        # regular MESSAGE phase.
        self.reaction_phase = False
        # Commands rejected at the door (target was not leader) never reach a
        # log. Final committed / superseded / pending outcomes are derived
        # from the end state; knownLeader is captured at rejection time.
        self.rejected: dict[str, dict] = {}

    def run(self) -> dict:
        for order, fault in enumerate(self.faults):
            heapq.heappush(self.queue, (fault["time"], _KIND_FAULT, order, fault))
        for order, command in enumerate(self.commands):
            heapq.heappush(self.queue, (command["time"], _KIND_CLIENT, order, command))
        for name in self.node_names:
            heapq.heappush(self.queue, (self.timeouts[name], _KIND_TIMEOUT, self.index[name], (name, 0)))
        while self.queue:
            time, kind, _order, payload = heapq.heappop(self.queue)
            if time > self.duration:
                break
            self.now = time
            self.reaction_phase = kind in (_KIND_CLIENT, _KIND_REACTION)
            if kind == _KIND_FAULT:
                self._apply_fault(payload)
            elif kind in (_KIND_MESSAGE, _KIND_REACTION):
                self._deliver(payload)
            elif kind == _KIND_CLIENT:
                self._on_client_command(payload)
            elif kind == _KIND_TIMEOUT:
                self._on_timeout(*payload)
            else:
                self._on_heartbeat(*payload)
        return self._report()

    # -- timeline helpers -------------------------------------------------

    def _record(self, entry: dict) -> None:
        self.timeline.append({"seq": len(self.timeline) + 1, "time": self.now, **entry})

    def _record_state_change(self, name: str, reason: str) -> None:
        st = self.state[name]
        self._record({"type": "stateChange", "node": name, "term": st.term, "role": st.role, "reason": reason})

    # -- state transitions --------------------------------------------------

    def _reset_timeout(self, name: str) -> None:
        st = self.state[name]
        st.timeout_gen += 1
        heapq.heappush(
            self.queue,
            (self.now + self.timeouts[name], _KIND_TIMEOUT, self.index[name], (name, st.timeout_gen)),
        )

    def _become_follower(self, name: str, term: int, reason: str) -> None:
        st = self.state[name]
        st.role = ROLE_FOLLOWER
        st.term = term
        st.voted_for = None
        st.known_leader = None
        st.votes = set()
        st.next_index = {}
        st.match_index = {}
        self._reset_timeout(name)
        self._record_state_change(name, reason)

    def _start_election(self, name: str) -> None:
        st = self.state[name]
        st.role = ROLE_CANDIDATE
        st.term += 1
        st.voted_for = name
        st.known_leader = None
        st.votes = {name}
        st.next_index = {}
        st.match_index = {}
        self._reset_timeout(name)
        self._record_state_change(name, "electionTimeout")
        for peer in self.node_names:
            if peer != name:
                self._send(
                    name,
                    peer,
                    {
                        "kind": "requestVote",
                        "term": st.term,
                        "lastLogIndex": st.last_log_index(),
                        "lastLogTerm": st.last_log_term(),
                    },
                )

    def _become_leader(self, name: str) -> None:
        st = self.state[name]
        st.role = ROLE_LEADER
        st.known_leader = name
        st.votes = set()
        st.timeout_gen += 1  # leaders have no election timeout
        next_index = st.last_log_index() + 1
        st.next_index = {peer: next_index for peer in self.node_names if peer != name}
        st.match_index = {peer: 0 for peer in self.node_names if peer != name}
        self._record_state_change(name, "majority")
        leaders = self.leaders_by_term.setdefault(st.term, [])
        if name not in leaders:
            leaders.append(name)
        self._send_heartbeats(name)

    # -- messages -----------------------------------------------------------

    def _send(self, src: str, dst: str, msg: dict) -> None:
        self._record({"type": "messageSend", "node": src, "peer": dst, "message": msg["kind"], "term": msg["term"]})
        self.send_counter += 1
        envelope = {**msg, "src": src, "dst": dst}
        # Zero-delay replication triggered while draining a client-command
        # batch shares the batch's timestamp and must follow the remaining
        # client commands; a positive delay lands strictly in the future and
        # queues as a normal message arrival.
        in_phase = self.reaction_phase and self.delay == 0
        kind = _KIND_REACTION if in_phase else _KIND_MESSAGE
        heapq.heappush(self.queue, (self.now + self.delay, kind, self.send_counter, envelope))

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
                self._send(
                    name,
                    peer,
                    {"kind": "heartbeat", "term": st.term, "leaderCommit": st.commit_index},
                )
        heapq.heappush(
            self.queue,
            (self.now + self.heartbeat_interval, _KIND_HEARTBEAT, self.index[name], (name, st.term)),
        )

    def _replicate_to(self, name: str, peer: str) -> None:
        """Send the next due appendEntries message for one peer."""
        st = self.state[name]
        next_idx = st.next_index[peer]
        prev_idx = next_idx - 1
        entries = [dict(entry) for entry in st.log[prev_idx:]]
        self._send(
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

    def _connected(self, a: str, b: str) -> bool:
        if self.partition is None:
            return True
        return any(a in group and b in group for group in self.partition)

    def _deliver(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        if not self._connected(src, dst):
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": msg["kind"],
                "term": msg["term"],
                "result": "dropped",
                "reason": "partition",
            })
            return
        kind = msg["kind"]
        if kind == "requestVote":
            self._handle_request_vote(msg)
        elif kind == "voteReply":
            self._handle_vote_reply(msg)
        elif kind == "heartbeat":
            self._handle_heartbeat(msg)
        elif kind == "appendEntries":
            self._handle_append_entries(msg)
        else:
            self._handle_append_reply(msg)

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
        granted = (
            msg["term"] >= st.term
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
        self._send(dst, src, {"kind": "voteReply", "term": st.term, "granted": granted})

    def _handle_vote_reply(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        detail = "ignored"
        elected = False
        if msg["term"] > st.term:
            detail = "higherTerm"
        elif st.role == ROLE_CANDIDATE and msg["term"] == st.term and msg["granted"]:
            st.votes.add(src)
            detail = "voteCounted"
            elected = len(st.votes) >= self.majority
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
            self._become_leader(dst)

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
            self._send(
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

        prev_index = msg["prevLogIndex"]
        prefix_ok = prev_index <= st.last_log_index() and st.term_at(prev_index) == msg["prevLogTerm"]
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
            self._send(
                dst,
                src,
                {"kind": "appendReply", "term": st.term, "success": False, "matchIndex": 0},
            )
            return

        # Truncate the suffix at the first position whose term differs.
        conflict_index = 0
        for offset, entry in enumerate(msg["entries"]):
            index = prev_index + 1 + offset
            if index <= st.last_log_index() and st.log[index - 1]["term"] != entry["term"]:
                conflict_index = index
                del st.log[index - 1 :]
                break

        # Append whatever is not already present with a matching term.
        appended = 0
        for offset, entry in enumerate(msg["entries"]):
            index = prev_index + 1 + offset
            if index > st.last_log_index():
                st.log.append(
                    {
                        "index": index,
                        "term": entry["term"],
                        "id": entry["id"],
                        "command": entry["command"],
                    }
                )
                appended += 1

        self._record({
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
        })

        if msg["leaderCommit"] > st.commit_index:
            st.commit_index = min(msg["leaderCommit"], st.last_log_index())
        self._advance_apply(dst)

        self._send(
            dst,
            src,
            {
                "kind": "appendReply",
                "term": st.term,
                "success": True,
                "matchIndex": st.last_log_index(),
            },
        )

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
            match = msg["matchIndex"]
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
        if st.role != ROLE_LEADER:
            self.rejected[command_id] = {"node": node, "knownLeader": st.known_leader}
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
        st.log.append(
            {"index": index, "term": st.term, "id": command_id, "command": command["command"]}
        )
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
        candidate = replicated[self.majority - 1]
        if candidate > st.commit_index and st.term_at(candidate) == st.term:
            st.commit_index = candidate
            self._record({
                "type": "commitAdvance",
                "node": name,
                "term": st.term,
                "commitIndex": st.commit_index,
            })
            self._advance_apply(name)

    def _advance_apply(self, name: str) -> None:
        st = self.state[name]
        while st.last_applied < st.commit_index:
            st.last_applied += 1
            entry = st.log[st.last_applied - 1]
            st.applied.append(
                {"index": entry["index"], "term": entry["term"], "id": entry["id"], "command": entry["command"]}
            )
            self._record({
                "type": "applied",
                "node": name,
                "index": entry["index"],
                "term": entry["term"],
                "id": entry["id"],
            })

    # -- timeouts, heartbeats, faults ---------------------------------------

    def _on_timeout(self, name: str, generation: int) -> None:
        st = self.state[name]
        if generation != st.timeout_gen or st.role == ROLE_LEADER:
            return
        self._record({"type": "timeout", "node": name, "term": st.term, "reason": "electionTimeout"})
        self._start_election(name)

    def _on_heartbeat(self, name: str, term: int) -> None:
        st = self.state[name]
        if st.role == ROLE_LEADER and st.term == term:
            self._send_heartbeats(name)

    def _apply_fault(self, fault: dict) -> None:
        if fault["action"] == "partition":
            self.partition = [frozenset(group) for group in fault["groups"]]
            self._record({"type": "fault", "action": "partition", "groups": fault["groups"]})
        else:
            self.partition = None
            self._record({"type": "fault", "action": "heal"})

    # -- report ---------------------------------------------------------------

    def _log_matching_violations(self) -> list[dict]:
        """Same index and term, but different content (id/command) across nodes."""
        violations = []
        max_len = max((st.last_log_index() for st in self.state.values()), default=0)
        for index in range(1, max_len + 1):
            by_term: dict[int, dict[str, dict]] = {}
            for name in self.node_names:
                st = self.state[name]
                if index > st.last_log_index():
                    continue
                entry = st.log[index - 1]
                marker = json.dumps([entry["id"], entry["command"]], ensure_ascii=False, sort_keys=True)
                bucket = by_term.setdefault(
                    entry["term"],
                    {},
                ).setdefault(marker, {"id": entry["id"], "command": entry["command"], "nodes": []})
                bucket["nodes"].append(name)
            for term, variants in sorted(by_term.items()):
                if len(variants) > 1:
                    violations.append(
                        {
                            "index": index,
                            "term": term,
                            "variants": [bucket for _marker, bucket in sorted(variants.items())],
                        }
                    )
        return violations

    def _state_machine_safety_violations(self) -> list[dict]:
        """Different nodes applied different commands at the same index."""
        violations = []
        max_applied = max((st.last_applied for st in self.state.values()), default=0)
        for index in range(1, max_applied + 1):
            variants: dict[str, dict] = {}
            for name in self.node_names:
                st = self.state[name]
                if index > st.last_applied:
                    continue
                entry = st.applied[index - 1]
                marker = json.dumps([entry["term"], entry["id"], entry["command"]], ensure_ascii=False, sort_keys=True)
                bucket = variants.setdefault(
                    marker, {"term": entry["term"], "id": entry["id"], "command": entry["command"], "nodes": []}
                )
                bucket["nodes"].append(name)
            if len(variants) > 1:
                violations.append(
                    {"index": index, "variants": [bucket for marker, bucket in sorted(variants.items())]}
                )
        return violations

    def _clients_report(self) -> dict:
        grouped: dict[str, list[dict]] = {
            "committed": [],
            "superseded": [],
            "pending": [],
            "rejected": [],
        }

        def find_applied(command_id: str) -> dict | None:
            for name in self.node_names:
                for entry in self.state[name].applied:
                    if entry["id"] == command_id:
                        return entry
            return None

        def in_any_log(command_id: str) -> dict | None:
            for name in self.node_names:
                for entry in self.state[name].log:
                    if entry["id"] == command_id:
                        return entry
            return None

        for command in self.commands:
            command_id = command["id"]
            if command_id in self.rejected:
                rejected = self.rejected[command_id]
                grouped["rejected"].append(
                    {
                        "id": command_id,
                        "node": command["node"],
                        "reason": "notLeader",
                        "knownLeader": rejected["knownLeader"],
                    }
                )
                continue
            applied = find_applied(command_id)
            if applied is not None:
                grouped["committed"].append(
                    {
                        "id": command_id,
                        "node": command["node"],
                        "index": applied["index"],
                        "term": applied["term"],
                    }
                )
                continue
            present = in_any_log(command_id)
            if present is not None:
                grouped["pending"].append(
                    {
                        "id": command_id,
                        "node": command["node"],
                        "index": present["index"],
                        "term": present["term"],
                    }
                )
            else:
                grouped["superseded"].append({"id": command_id, "node": command["node"]})
        return grouped

    def _report(self) -> dict:
        leaders = {str(term): names for term, names in sorted(self.leaders_by_term.items())}
        violations = [
            {"term": term, "leaders": list(names)}
            for term, names in sorted(self.leaders_by_term.items())
            if len(names) > 1
        ]
        return {
            "timeline": self.timeline,
            "nodes": {
                name: {
                    "role": st.role,
                    "term": st.term,
                    "votedFor": st.voted_for,
                    "knownLeader": st.known_leader,
                    "log": [dict(entry) for entry in st.log],
                    "commitIndex": st.commit_index,
                    "lastApplied": st.last_applied,
                    "applied": [dict(entry) for entry in st.applied],
                }
                for name, st in self.state.items()
            },
            "clients": self._clients_report(),
            "electionSafety": {
                "leadersByTerm": leaders,
                "violations": violations,
            },
            "logMatching": {"violations": self._log_matching_violations()},
            "stateMachineSafety": {"violations": self._state_machine_safety_violations()},
        }


def run_simulation(raw: object) -> dict:
    """Validate a decoded JSON scenario and run the deterministic simulation."""
    return _Simulator(parse_scenario(raw)).run()
