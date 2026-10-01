"""Deterministic Raft simulation: leader election, log replication and commit.

The simulation advances virtual time only: it never reads the wall clock and
never uses randomness, so identical input produces byte-identical output.
"""

from __future__ import annotations

import heapq

ROLE_FOLLOWER = "follower"
ROLE_CANDIDATE = "candidate"
ROLE_LEADER = "leader"

# Event kinds, processed in this order when they share a timestamp.
_KIND_FAULT = 0
_KIND_MESSAGE = 1
_KIND_CLIENT = 2
_KIND_TIMEOUT = 3
_KIND_HEARTBEAT = 4

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

    node_set = set(nodes)

    faults = raw.get("faults", [])
    if not isinstance(faults, list):
        raise ScenarioError("faults must be a list")
    normalized_faults = []
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

    client_commands_raw = raw.get("clientCommands", [])
    if not isinstance(client_commands_raw, list):
        raise ScenarioError("clientCommands must be a list")
    normalized_commands = []
    seen_ids: set[str] = set()
    for index, client_command in enumerate(client_commands_raw):
        label = f"clientCommands[{index}]"
        if not isinstance(client_command, dict):
            raise ScenarioError(f"{label} must be an object")
        unknown = sorted(set(client_command) - _CLIENT_COMMAND_FIELDS)
        if unknown:
            raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
        missing = sorted(_CLIENT_COMMAND_FIELDS - set(client_command))
        if missing:
            raise ScenarioError(f"{label} is missing field(s): {', '.join(missing)}")
        time = _require_int(client_command["time"], f"{label}.time", 0)
        if time > duration:
            raise ScenarioError(f"{label}.time is beyond the simulation duration")
        node = client_command["node"]
        if not isinstance(node, str) or node not in node_set:
            raise ScenarioError(f"{label}.node references unknown node: {node!r}")
        command_id = client_command["id"]
        if not isinstance(command_id, str) or not command_id:
            raise ScenarioError(f"{label}.id must be a non-empty string")
        if command_id in seen_ids:
            raise ScenarioError(f"duplicate client command id: {command_id!r}")
        seen_ids.add(command_id)
        normalized_commands.append(
            {"time": time, "node": node, "id": command_id, "command": client_command["command"]}
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
        self.log: list[dict] = []
        self.commit_index = 0
        self.last_applied = 0
        self.applied: list[dict] = []
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}


class _Simulator:
    def __init__(self, config: dict) -> None:
        self.node_names: list[str] = config["nodes"]
        self.duration: int = config["duration"]
        self.timeouts: dict[str, int] = config["electionTimeouts"]
        self.heartbeat_interval: int = config["heartbeatInterval"]
        self.delay: int = config["messageDelay"]
        self.faults: list[dict] = config["faults"]
        self.client_commands: list[dict] = config["clientCommands"]
        self.state = {name: _Node() for name in self.node_names}
        self.index = {name: i for i, name in enumerate(self.node_names)}
        self.majority = len(self.node_names) // 2 + 1
        self.timeline: list[dict] = []
        self.leaders_by_term: dict[int, list[str]] = {}
        self.partition: list[frozenset[str]] | None = None
        self.queue: list[tuple] = []
        self.send_counter = 0
        self.now = 0
        # Client command tracking, in input order.
        self.clients: dict[str, dict] = {}
        self.committed_ids: set[str] = set()

    def run(self) -> dict:
        for order, fault in enumerate(self.faults):
            heapq.heappush(self.queue, (fault["time"], _KIND_FAULT, order, fault))
        for order, command in enumerate(self.client_commands):
            heapq.heappush(self.queue, (command["time"], _KIND_CLIENT, order, command))
        for name in self.node_names:
            heapq.heappush(self.queue, (self.timeouts[name], _KIND_TIMEOUT, self.index[name], (name, 0)))
        while self.queue:
            time, kind, _order, payload = heapq.heappop(self.queue)
            if time > self.duration:
                break
            self.now = time
            if kind == _KIND_FAULT:
                self._apply_fault(payload)
            elif kind == _KIND_MESSAGE:
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
        self._reset_timeout(name)
        self._record_state_change(name, reason)

    def _start_election(self, name: str) -> None:
        st = self.state[name]
        st.role = ROLE_CANDIDATE
        st.term += 1
        st.voted_for = name
        st.known_leader = None
        st.votes = {name}
        self._reset_timeout(name)
        self._record_state_change(name, "electionTimeout")
        last_log_term = st.log[-1]["term"] if st.log else 0
        for peer in self.node_names:
            if peer != name:
                self._send(name, peer, {
                    "kind": "requestVote",
                    "term": st.term,
                    "lastLogIndex": len(st.log),
                    "lastLogTerm": last_log_term,
                })

    def _become_leader(self, name: str) -> None:
        st = self.state[name]
        st.role = ROLE_LEADER
        st.known_leader = name
        st.votes = set()
        st.timeout_gen += 1  # leaders have no election timeout
        st.next_index = {peer: len(st.log) + 1 for peer in self.node_names if peer != name}
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
        heapq.heappush(self.queue, (self.now + self.delay, _KIND_MESSAGE, self.send_counter, envelope))

    def _send_heartbeats(self, name: str) -> None:
        st = self.state[name]
        for peer in self.node_names:
            if peer != name:
                self._send(name, peer, {"kind": "heartbeat", "term": st.term})
        heapq.heappush(
            self.queue,
            (self.now + self.heartbeat_interval, _KIND_HEARTBEAT, self.index[name], (name, st.term)),
        )

    def _send_append_entries(self, name: str, peer: str) -> None:
        st = self.state[name]
        next_index = st.next_index.get(peer, len(st.log) + 1)
        prev_index = next_index - 1
        prev_term = st.log[prev_index - 1]["term"] if prev_index > 0 else 0
        entries = [dict(entry) for entry in st.log[next_index - 1:]]
        self._send(name, peer, {
            "kind": "appendEntries",
            "term": st.term,
            "prevLogIndex": prev_index,
            "prevLogTerm": prev_term,
            "entries": entries,
            "leaderCommit": st.commit_index,
        })

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
        if msg["kind"] == "requestVote":
            self._handle_request_vote(msg)
        elif msg["kind"] == "voteReply":
            self._handle_vote_reply(msg)
        elif msg["kind"] == "appendEntries":
            self._handle_append_entries(msg)
        elif msg["kind"] == "appendEntriesReply":
            self._handle_append_entries_reply(msg)
        else:
            self._handle_heartbeat(msg)

    def _handle_request_vote(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        # A higher term clears voted_for before the vote decision is made.
        effective_voted_for = None if msg["term"] > st.term else st.voted_for
        # The candidate's log must be at least as up-to-date as the voter's.
        last_log_term = st.log[-1]["term"] if st.log else 0
        up_to_date = msg["lastLogTerm"] > last_log_term or (
            msg["lastLogTerm"] == last_log_term and msg["lastLogIndex"] >= len(st.log)
        )
        granted = (
            msg["term"] >= st.term
            and (effective_voted_for is None or effective_voted_for == src)
            and up_to_date
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
            self._send(dst, src, {
                "kind": "appendEntriesReply",
                "term": st.term,
                "success": False,
                "matchIndex": len(st.log),
            })
            return
        prev_index = msg["prevLogIndex"]
        entries = msg["entries"]
        if prev_index > len(st.log):
            detail, success = "missingPrefix", False
        elif prev_index > 0 and st.log[prev_index - 1]["term"] != msg["prevLogTerm"]:
            detail, success = "conflict", False
        else:
            detail, success = "accepted", True
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "appendEntries",
            "term": msg["term"],
            "result": "delivered",
            "detail": detail,
        })
        if msg["term"] > st.term or st.role != ROLE_FOLLOWER:
            self._become_follower(dst, msg["term"], "appendEntries")
        else:
            self._reset_timeout(dst)
        st.known_leader = src
        if success:
            self._merge_entries(dst, prev_index, entries)
            if msg["leaderCommit"] > st.commit_index:
                st.commit_index = min(msg["leaderCommit"], len(st.log))
                self._apply_committed(dst)
        elif detail == "conflict":
            # Delete the conflicting entry and everything after it.
            self._truncate_log(dst, prev_index)
        match_index = prev_index + len(entries) if success else len(st.log)
        self._send(dst, src, {
            "kind": "appendEntriesReply",
            "term": st.term,
            "success": success,
            "matchIndex": match_index,
        })

    def _handle_append_entries_reply(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        detail = "ignored"
        if msg["term"] > st.term:
            detail = "higherTerm"
        elif st.role == ROLE_LEADER and msg["term"] == st.term:
            if msg["success"]:
                detail = "accepted"
                st.match_index[src] = max(st.match_index.get(src, 0), msg["matchIndex"])
                st.next_index[src] = st.match_index[src] + 1
            else:
                detail = "rejected"
                # Deterministic backoff: step one entry back and retry.
                st.next_index[src] = max(1, st.next_index.get(src, len(st.log) + 1) - 1)
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "appendEntriesReply",
            "term": msg["term"],
            "result": "delivered",
            "detail": detail,
        })
        if msg["term"] > st.term:
            self._become_follower(dst, msg["term"], "higherTermMessage")
        elif detail == "accepted":
            self._advance_commit(dst)
        elif detail == "rejected":
            self._send_append_entries(dst, src)

    # -- log replication ------------------------------------------------------

    def _merge_entries(self, name: str, prev_index: int, entries: list[dict]) -> None:
        st = self.state[name]
        for offset, entry in enumerate(entries):
            index = prev_index + 1 + offset
            if index <= len(st.log):
                if st.log[index - 1]["term"] != entry["term"]:
                    self._truncate_log(name, index)
                    st.log.extend(dict(e) for e in entries[offset:])
                    break
                # Already have this entry; keep going.
            else:
                st.log.extend(dict(e) for e in entries[offset:])
                break

    def _truncate_log(self, name: str, index: int) -> None:
        st = self.state[name]
        removed = st.log[index - 1:]
        del st.log[index - 1:]
        for entry in removed:
            self._mark_superseded(name, entry, "logOverwritten")

    def _apply_committed(self, name: str) -> None:
        st = self.state[name]
        while st.last_applied < st.commit_index:
            st.last_applied += 1
            entry = st.log[st.last_applied - 1]
            st.applied.append({"index": entry["index"], "id": entry["id"], "command": entry["command"]})
            self._record({"type": "apply", "node": name, "index": entry["index"], "id": entry["id"]})

    def _advance_commit(self, name: str) -> None:
        st = self.state[name]
        new_commit = st.commit_index
        for index in range(len(st.log), st.commit_index, -1):
            if st.log[index - 1]["term"] != st.term:
                continue  # only current-term entries advance the commit index
            replicated = 1 + sum(1 for match in st.match_index.values() if match >= index)
            if replicated >= self.majority:
                new_commit = index
                break
        if new_commit == st.commit_index:
            return
        st.commit_index = new_commit
        self._apply_committed(name)
        for entry in st.log[:new_commit]:
            command_id = entry["id"]
            if command_id in self.clients and command_id not in self.committed_ids:
                self.committed_ids.add(command_id)
                self.clients[command_id]["status"] = "committed"
                self._record({
                    "type": "clientResult",
                    "node": name,
                    "id": command_id,
                    "result": "committed",
                    "index": entry["index"],
                    "term": entry["term"],
                })
        # Pending commands whose slot was committed to a different entry can
        # never commit now.
        for command_id, record in self.clients.items():
            if record["status"] == "pending" and record["index"] <= new_commit:
                if st.log[record["index"] - 1]["id"] != command_id:
                    record["status"] = "superseded"
                    self._record({
                        "type": "clientResult",
                        "node": name,
                        "id": command_id,
                        "result": "superseded",
                        "index": record["index"],
                        "term": record["term"],
                        "reason": "conflictingEntryCommitted",
                    })
        for peer in self.node_names:
            if peer != name:
                self._send_append_entries(name, peer)

    # -- client commands ------------------------------------------------------

    def _on_client_command(self, command: dict) -> None:
        name = command["node"]
        command_id = command["id"]
        st = self.state[name]
        record = {"node": name, "time": command["time"], "status": "pending"}
        self.clients[command_id] = record
        if st.role != ROLE_LEADER:
            record["status"] = "rejected"
            record["knownLeader"] = st.known_leader
            self._record({
                "type": "clientResult",
                "node": name,
                "id": command_id,
                "result": "rejected",
                "reason": "notLeader",
                "knownLeader": st.known_leader,
            })
            return
        entry = {"index": len(st.log) + 1, "term": st.term, "id": command_id, "command": command["command"]}
        st.log.append(entry)
        record["index"] = entry["index"]
        record["term"] = entry["term"]
        self._record({
            "type": "clientResult",
            "node": name,
            "id": command_id,
            "result": "accepted",
            "index": entry["index"],
            "term": entry["term"],
        })
        for peer in self.node_names:
            if peer != name:
                self._send_append_entries(name, peer)

    def _mark_superseded(self, name: str, entry: dict, reason: str) -> None:
        command_id = entry["id"]
        record = self.clients.get(command_id)
        if record is None or record["status"] != "pending" or record["node"] != name:
            return
        record["status"] = "superseded"
        self._record({
            "type": "clientResult",
            "node": name,
            "id": command_id,
            "result": "superseded",
            "index": entry["index"],
            "term": entry["term"],
            "reason": reason,
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
        violations = []
        for i, first in enumerate(self.node_names):
            for second in self.node_names[i + 1:]:
                log_a = self.state[first].log
                log_b = self.state[second].log
                for pos in range(min(len(log_a), len(log_b))):
                    if log_a[pos]["term"] == log_b[pos]["term"] and log_a[:pos] != log_b[:pos]:
                        violations.append({
                            "index": pos + 1,
                            "term": log_a[pos]["term"],
                            "nodes": [first, second],
                        })
        violations.sort(key=lambda v: (v["index"], v["nodes"]))
        return violations

    def _state_machine_violations(self) -> list[dict]:
        applied_by_index: dict[int, dict[str, object]] = {}
        for name in self.node_names:
            for applied in self.state[name].applied:
                applied_by_index.setdefault(applied["index"], {})[name] = applied["command"]
        violations = []
        for index in sorted(applied_by_index):
            commands = applied_by_index[index]
            values = list(commands.values())
            if any(value != values[0] for value in values[1:]):
                violations.append({"index": index, "applied": commands})
        return violations

    def _report(self) -> dict:
        leaders = {str(term): names for term, names in sorted(self.leaders_by_term.items())}
        violations = [
            {"term": term, "leaders": list(names)}
            for term, names in sorted(self.leaders_by_term.items())
            if len(names) > 1
        ]
        clients = {}
        for command_id, record in self.clients.items():
            summary = {"node": record["node"], "time": record["time"], "status": record["status"]}
            if record["status"] == "rejected":
                summary["knownLeader"] = record["knownLeader"]
            else:
                summary["index"] = record["index"]
                summary["term"] = record["term"]
            clients[command_id] = summary
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
            "clients": clients,
            "electionSafety": {
                "leadersByTerm": leaders,
                "violations": violations,
            },
            "logMatching": {
                "violations": self._log_matching_violations(),
            },
            "stateMachineSafety": {
                "violations": self._state_machine_violations(),
            },
        }


def run_simulation(raw: object) -> dict:
    """Validate a decoded JSON scenario and run the deterministic simulation."""
    return _Simulator(parse_scenario(raw)).run()
