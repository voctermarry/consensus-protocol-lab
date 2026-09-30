"""Deterministic Raft leader-election simulation driven purely by virtual time.

The simulation never reads a wall clock and never draws random numbers: every
timeout, message delay and fault comes from the scenario input, and event
ordering at identical virtual times is fixed (faults first, then message
deliveries, election timeouts and heartbeats; node events follow the order of
the ``nodes`` array). Every emitted timeline record carries a monotonically
increasing ``seq`` so that the same input always yields byte-identical output.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any

__all__ = ["SimulationError", "Fault", "Scenario", "parse_scenario", "run_simulation"]


class SimulationError(ValueError):
    """Raised for any invalid scenario input."""


_TOP_LEVEL_KEYS = {
    "nodes",
    "duration",
    "electionTimeouts",
    "heartbeatInterval",
    "messageDelay",
    "faults",
}
_FAULT_KEYS = {"time", "action", "groups"}
_REQUIRED_KEYS = ("nodes", "duration", "electionTimeouts", "heartbeatInterval", "messageDelay")


@dataclass(frozen=True)
class Fault:
    """A partition/heal fault injected at a fixed virtual time."""

    time: int
    action: str
    groups: tuple[tuple[str, ...], tuple[str, ...]] | None


@dataclass(frozen=True)
class Scenario:
    """Validated simulation scenario."""

    nodes: list[str]
    duration: int
    election_timeouts: list[int]
    heartbeat_interval: int
    message_delay: int
    faults: list[Fault]


def _is_int(value: Any) -> bool:
    # bool is a subclass of int; reject True/False explicitly.
    return isinstance(value, int) and not isinstance(value, bool)


def parse_scenario(raw: Any) -> Scenario:
    """Validate decoded JSON and build a :class:`Scenario`.

    Raises :class:`SimulationError` with a one-line human-readable message on
    any structural or semantic problem.
    """

    if not isinstance(raw, dict):
        raise SimulationError("scenario must be a JSON object")

    unknown = set(raw) - _TOP_LEVEL_KEYS
    if unknown:
        raise SimulationError("unknown field in scenario")
    for key in _REQUIRED_KEYS:
        if key not in raw:
            raise SimulationError(f"missing field: {key}")

    nodes_raw = raw["nodes"]
    if not isinstance(nodes_raw, list) or len(nodes_raw) < 3:
        raise SimulationError("nodes must be a list of at least three names")
    nodes: list[str] = []
    for name in nodes_raw:
        if not isinstance(name, str) or name == "":
            raise SimulationError("node names must be non-empty strings")
        if name in nodes:
            raise SimulationError("node names must be unique")
        nodes.append(name)
    node_set = set(nodes)

    duration = raw["duration"]
    if not _is_int(duration) or duration < 0:
        raise SimulationError("duration must be a non-negative integer (milliseconds)")

    timeouts_raw = raw["electionTimeouts"]
    if not isinstance(timeouts_raw, list) or len(timeouts_raw) != len(nodes):
        raise SimulationError("electionTimeouts must be a list with one timeout per node")
    for value in timeouts_raw:
        if not _is_int(value) or value <= 0:
            raise SimulationError("election timeouts must be positive integers (milliseconds)")

    heartbeat_interval = raw["heartbeatInterval"]
    if not _is_int(heartbeat_interval) or heartbeat_interval <= 0:
        raise SimulationError("heartbeatInterval must be a positive integer (milliseconds)")

    message_delay = raw["messageDelay"]
    if not _is_int(message_delay) or message_delay <= 0:
        raise SimulationError("messageDelay must be a positive integer (milliseconds)")

    faults_raw = raw.get("faults", [])
    if not isinstance(faults_raw, list):
        raise SimulationError("faults must be a list")

    faults: list[Fault] = []
    for i, item in enumerate(faults_raw):
        where = f"faults[{i}]"
        if not isinstance(item, dict):
            raise SimulationError(f"{where} must be an object")
        unknown_fault = set(item) - _FAULT_KEYS
        if unknown_fault:
            raise SimulationError(f"unknown field in {where}")
        if "time" not in item or "action" not in item:
            raise SimulationError(f"{where} requires time and action")

        fault_time = item["time"]
        if not _is_int(fault_time) or fault_time < 0 or fault_time > duration:
            raise SimulationError(f"{where}.time must be within 0..duration")

        action = item["action"]
        if action not in ("partition", "heal"):
            raise SimulationError(f"{where}.action must be 'partition' or 'heal'")

        groups: tuple[tuple[str, ...], tuple[str, ...]] | None = None
        if action == "partition":
            if "groups" not in item:
                raise SimulationError(f"{where} partition requires groups")
            groups_raw = item["groups"]
            if (
                not isinstance(groups_raw, list)
                or len(groups_raw) != 2
                or not all(isinstance(side, list) for side in groups_raw)
            ):
                raise SimulationError(f"{where}.groups must be two lists")
            flat: list[str] = []
            for side in groups_raw:
                if not side:
                    raise SimulationError(f"{where}.groups partitions must be non-empty")
                for member in side:
                    if not isinstance(member, str) or member not in node_set:
                        raise SimulationError(f"{where}.groups references an unknown node")
                    flat.append(member)
            if len(flat) != len(set(flat)) or set(flat) != node_set:
                raise SimulationError(
                    f"{where}.groups must be disjoint and together cover all nodes"
                )
            groups = (tuple(groups_raw[0]), tuple(groups_raw[1]))
        elif "groups" in item:
            raise SimulationError(f"{where} heal must not specify groups")

        faults.append(Fault(fault_time, action, groups))

    return Scenario(
        nodes=nodes,
        duration=duration,
        election_timeouts=list(timeouts_raw),
        heartbeat_interval=heartbeat_interval,
        message_delay=message_delay,
        faults=faults,
    )


@dataclass
class _Node:
    name: str
    election_timeout: int
    role: str = "follower"
    term: int = 0
    voted_for: str | None = None
    known_leader: str | None = None
    deadline: int = 0
    next_heartbeat: int | None = None
    votes: set[str] = field(default_factory=set)


# Message kinds.
_REQUEST_VOTE = "RequestVote"
_APPEND_ENTRIES = "AppendEntries"


def run_simulation(scenario: Scenario) -> dict[str, Any]:
    """Run the deterministic simulation and return the JSON-serialisable result."""

    names = list(scenario.nodes)
    nodes = [
        _Node(name=name, election_timeout=timeout, deadline=timeout)
        for name, timeout in zip(names, scenario.election_timeouts)
    ]
    majority = len(nodes) // 2 + 1

    # Stable ordering: faults at the same time stay in input order.
    fault_order = sorted(range(len(scenario.faults)), key=lambda i: scenario.faults[i].time)

    # Node name -> partition side (0/1); None means the cluster is connected.
    partition: dict[str, int] | None = None

    # Heap entries: (due_time, target_index, sent_seq, sender_index, term, kind)
    mailbox: list[tuple[int, int, int, int, int, str]] = []
    timeline: list[dict[str, Any]] = []
    seq = 0

    def emit(entry: dict[str, Any]) -> int:
        nonlocal seq
        seq += 1
        record = {"seq": seq, "time": None, "node": None, "term": None, "peer": None}
        record.update(entry)
        timeline.append(record)
        return seq

    def connected(a: str, b: str) -> bool:
        return partition is None or partition[a] == partition[b]

    def send(now: int, sender: int, target: int, term: int, kind: str) -> None:
        sent_seq = emit(
            {
                "time": now,
                "type": "message-sent",
                "node": names[sender],
                "peer": names[target],
                "term": term,
                "message": kind,
            }
        )
        due = now + scenario.message_delay
        if due <= scenario.duration:
            heapq.heappush(mailbox, (due, target, sent_seq, sender, term, kind))

    def step_down(index: int, new_term: int, now: int, reason: str) -> None:
        """Update to a higher term and return to follower, resetting the timer."""

        node = nodes[index]
        node.term = new_term
        node.role = "follower"
        node.voted_for = None
        node.known_leader = None
        node.votes = set()
        node.deadline = now + node.election_timeout
        emit(
            {
                "time": now,
                "type": "state",
                "node": node.name,
                "term": node.term,
                "role": node.role,
                "reason": reason,
            }
        )

    def elect(index: int, now: int) -> None:
        node = nodes[index]
        node.role = "leader"
        node.known_leader = node.name
        node.next_heartbeat = now  # first heartbeat fires in the heartbeat phase
        emit(
            {
                "time": now,
                "type": "state",
                "node": node.name,
                "term": node.term,
                "role": node.role,
                "reason": "elected",
            }
        )

    def deliver(now: int, target: int, sender: int, term: int, kind: str) -> None:
        node = nodes[target]
        sender_name = names[sender]

        if not connected(sender_name, node.name):
            emit(
                {
                    "time": now,
                    "type": "message",
                    "node": node.name,
                    "peer": sender_name,
                    "term": term,
                    "message": kind,
                    "result": "dropped",
                    "reason": "partition",
                }
            )
            return

        if kind == _REQUEST_VOTE:
            if term < node.term:
                emit(
                    {
                        "time": now,
                        "type": "message",
                        "node": node.name,
                        "peer": sender_name,
                        "term": term,
                        "message": kind,
                        "result": "denied",
                        "reason": "stale-term",
                    }
                )
                return
            if term > node.term:
                step_down(target, term, now, "higher-term")
            if node.voted_for is not None and node.voted_for != sender_name:
                emit(
                    {
                        "time": now,
                        "type": "message",
                        "node": node.name,
                        "peer": sender_name,
                        "term": term,
                        "message": kind,
                        "result": "denied",
                        "reason": "already-voted",
                    }
                )
                return

            node.voted_for = sender_name
            emit(
                {
                    "time": now,
                    "type": "message",
                    "node": node.name,
                    "peer": sender_name,
                    "term": term,
                    "message": kind,
                    "result": "delivered",
                    "reason": "vote-granted",
                }
            )
            candidate = nodes[sender]
            if candidate.role == "candidate" and candidate.term == term:
                candidate.votes.add(node.name)
                if len(candidate.votes) >= majority:
                    elect(sender, now)
            return

        # AppendEntries (heartbeat)
        if term < node.term:
            emit(
                {
                    "time": now,
                    "type": "message",
                    "node": node.name,
                    "peer": sender_name,
                    "term": term,
                    "message": kind,
                    "result": "ignored",
                    "reason": "stale-term",
                }
            )
            return

        if term > node.term:
            step_down(target, term, now, "higher-term")
        elif node.role != "follower":
            # Same-term candidate (or stale leader) acknowledges the leader.
            node.role = "follower"
            node.votes = set()
            emit(
                {
                    "time": now,
                    "type": "state",
                    "node": node.name,
                    "term": node.term,
                    "role": node.role,
                    "reason": "acknowledge-leader",
                }
            )
        node.known_leader = sender_name
        node.deadline = now + node.election_timeout
        emit(
            {
                "time": now,
                "type": "message",
                "node": node.name,
                "peer": sender_name,
                "term": term,
                "message": kind,
                "result": "delivered",
                "reason": "heartbeat-accepted",
            }
        )

    def on_timeout(index: int, now: int) -> None:
        node = nodes[index]
        node.term += 1
        node.role = "candidate"
        node.voted_for = node.name
        node.known_leader = None
        node.votes = {node.name}
        node.deadline = now + node.election_timeout
        emit(
            {
                "time": now,
                "type": "state",
                "node": node.name,
                "term": node.term,
                "role": node.role,
                "reason": "election-timeout",
            }
        )
        for peer in range(len(nodes)):
            if peer != index:
                send(now, index, peer, node.term, _REQUEST_VOTE)
        if len(node.votes) >= majority:  # unreachable while at least 3 nodes
            elect(index, now)

    fault_index = 0
    now = 0
    while now <= scenario.duration:
        # Phase 1: faults at this time (input order among equal times).
        while fault_index < len(fault_order):
            fault = scenario.faults[fault_order[fault_index]]
            if fault.time != now:
                break
            if fault.action == "partition":
                assert fault.groups is not None
                partition = {}
                for side, group in enumerate(fault.groups):
                    for member in group:
                        partition[member] = side
                emit(
                    {
                        "time": now,
                        "type": "fault",
                        "action": "partition",
                        "groups": [list(group) for group in fault.groups],
                    }
                )
            else:
                partition = None
                emit({"time": now, "type": "fault", "action": "heal", "groups": None})
            fault_index += 1

        # Phase 2: message deliveries, ordered by recipient node then send order.
        while mailbox and mailbox[0][0] == now:
            _, target, _, sender, term, kind = heapq.heappop(mailbox)
            deliver(now, target, sender, term, kind)

        # Phase 3: election timeouts (nodes in input order).
        for index, node in enumerate(nodes):
            if node.role != "leader" and node.deadline == now:
                on_timeout(index, now)

        # Phase 4: periodic heartbeats (leaders in input order).
        for index, node in enumerate(nodes):
            if node.role == "leader" and node.next_heartbeat == now:
                for peer in range(len(nodes)):
                    if peer != index:
                        send(now, index, peer, node.term, _APPEND_ENTRIES)
                node.next_heartbeat = now + scenario.heartbeat_interval

        upcoming: list[int] = []
        if fault_index < len(fault_order):
            upcoming.append(scenario.faults[fault_order[fault_index]].time)
        if mailbox:
            upcoming.append(mailbox[0][0])
        for node in nodes:
            if node.role == "leader":
                if node.next_heartbeat is not None:
                    upcoming.append(node.next_heartbeat)
            else:
                upcoming.append(node.deadline)
        future = [moment for moment in upcoming if now < moment <= scenario.duration]
        if not future:
            break
        now = min(future)

    return _build_result(nodes, timeline)


def _build_result(nodes: list[_Node], timeline: list[dict[str, Any]]) -> dict[str, Any]:
    leaders_by_term: dict[int, list[str]] = {}
    for record in timeline:
        if record["type"] == "state" and record["role"] == "leader":
            leaders = leaders_by_term.setdefault(record["term"], [])
            if record["node"] not in leaders:
                leaders.append(record["node"])

    terms_report: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    for term in sorted(leaders_by_term):
        leaders = leaders_by_term[term]
        entry = {"term": term, "leaders": leaders, "violation": len(leaders) > 1}
        terms_report.append(entry)
        if entry["violation"]:
            violations.append({"term": term, "leaders": list(leaders)})

    final_nodes = {
        node.name: {
            "role": node.role,
            "term": node.term,
            "votedFor": node.voted_for,
            "knownLeader": node.known_leader,
        }
        for node in nodes
    }

    return {
        "timeline": timeline,
        "nodes": final_nodes,
        "electionSafety": {"terms": terms_report, "violations": violations},
    }
