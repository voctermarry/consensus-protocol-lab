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
_KIND_NODE_EVENT = 1
_KIND_MESSAGE = 2
_KIND_CLIENT = 3
# Membership requests are request-shaped like client commands; the replication
# they trigger drains in the REACTION phase, after every request at the same
# timestamp has been accepted or rejected in input order.
_KIND_MEMBERSHIP = 4
_KIND_REACTION = 5
_KIND_TIMEOUT = 6
_KIND_HEARTBEAT = 7

_TOP_LEVEL_FIELDS = {
    "nodes",
    "duration",
    "electionTimeouts",
    "heartbeatInterval",
    "messageDelay",
    "faults",
    "clientCommands",
    "nodeEvents",
    "snapshotThreshold",
    "initialMembers",
    "membershipChanges",
}
_REQUIRED_FIELDS = _TOP_LEVEL_FIELDS - {
    "faults",
    "clientCommands",
    "nodeEvents",
    "snapshotThreshold",
    "initialMembers",
    "membershipChanges",
}
_FAULT_FIELDS = {"time", "action", "groups"}
_CLIENT_COMMAND_FIELDS = {"time", "node", "id", "command"}
_NODE_EVENT_FIELDS = {"time", "node", "action"}
_MEMBERSHIP_CHANGE_FIELDS = {"time", "node", "id", "action", "member"}

# Membership-change actions.
MEMBER_ADD = "add"
MEMBER_REMOVE = "remove"
# Values of a configuration log entry's "entryType" field. Config entries are
# otherwise replicated like client commands but are never driven by clients.
CONFIG_ENTRY = "configuration"
# Configuration phases (also the key names of a joint config entry).
PHASE_STABLE = "stable"
PHASE_JOINT = "joint"
MIN_VOTERS = 3


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

    node_events_provided = "nodeEvents" in raw
    node_events = raw.get("nodeEvents", [])
    if not isinstance(node_events, list):
        raise ScenarioError("nodeEvents must be a list")
    normalized_node_events = []
    last_action: dict[str, str] = {}
    for index, event in enumerate(node_events):
        label = f"nodeEvents[{index}]"
        if not isinstance(event, dict):
            raise ScenarioError(f"{label} must be an object")
        unknown = sorted(set(event) - _NODE_EVENT_FIELDS)
        if unknown:
            raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
        missing = sorted(_NODE_EVENT_FIELDS - set(event))
        if missing:
            raise ScenarioError(f"{label} missing field(s): {', '.join(missing)}")
        time = _require_int(event["time"], f"{label}.time", 0)
        if time > duration:
            raise ScenarioError(f"{label}.time is beyond the simulation duration")
        node = event["node"]
        if not isinstance(node, str) or not node:
            raise ScenarioError(f"{label}.node must be a non-empty string")
        if node not in node_set:
            raise ScenarioError(f"{label}.node references unknown node: {node!r}")
        action = event["action"]
        if action not in ("crash", "restart"):
            raise ScenarioError(f"{label}.action must be crash or restart")
        previous = last_action.get(node)
        if previous is None and action != "crash":
            raise ScenarioError(f"{label}: first event for node {node!r} must be crash")
        if previous == action:
            raise ScenarioError(
                f"{label}: events for node {node!r} must alternate crash and restart"
            )
        last_action[node] = action
        normalized_node_events.append({"time": time, "node": node, "action": action})

    snapshot_threshold = None
    if "snapshotThreshold" in raw:
        snapshot_threshold = _require_int(
            raw["snapshotThreshold"], "snapshotThreshold", 1
        )

    membership_provided = "initialMembers" in raw or "membershipChanges" in raw
    initial_members: list[str] | None = None
    membership_changes: list[dict] = []
    if membership_provided:
        if "initialMembers" not in raw or "membershipChanges" not in raw:
            raise ScenarioError(
                "initialMembers and membershipChanges must be provided together"
            )
        raw_members = raw["initialMembers"]
        if not isinstance(raw_members, list) or len(raw_members) < MIN_VOTERS:
            raise ScenarioError(
                f"initialMembers must be a list of at least {MIN_VOTERS} names"
            )
        normalized_members = []
        seen_members: set[str] = set()
        for member in raw_members:
            label = "initialMembers"
            if not isinstance(member, str) or not member:
                raise ScenarioError(f"{label} entries must be non-empty strings")
            if member not in node_set:
                raise ScenarioError(
                    f"{label} references unknown node: {member!r}"
                )
            if member in seen_members:
                raise ScenarioError(f"{label} entries must be unique: {member!r}")
            seen_members.add(member)
            normalized_members.append(member)
        initial_members = normalized_members

        raw_changes = raw["membershipChanges"]
        if not isinstance(raw_changes, list):
            raise ScenarioError("membershipChanges must be a list")
        seen_change_ids: set[str] = set()
        for index, change in enumerate(raw_changes):
            label = f"membershipChanges[{index}]"
            if not isinstance(change, dict):
                raise ScenarioError(f"{label} must be an object")
            unknown = sorted(set(change) - _MEMBERSHIP_CHANGE_FIELDS)
            if unknown:
                raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
            missing = sorted(_MEMBERSHIP_CHANGE_FIELDS - set(change))
            if missing:
                raise ScenarioError(f"{label} missing field(s): {', '.join(missing)}")
            time = _require_int(change["time"], f"{label}.time", 0)
            if time > duration:
                raise ScenarioError(f"{label}.time is beyond the simulation duration")
            node = change["node"]
            if not isinstance(node, str) or not node:
                raise ScenarioError(f"{label}.node must be a non-empty string")
            if node not in node_set:
                raise ScenarioError(f"{label}.node references unknown node: {node!r}")
            change_id = change["id"]
            if not isinstance(change_id, str) or not change_id:
                raise ScenarioError(f"{label}.id must be a non-empty string")
            if change_id in seen_change_ids:
                raise ScenarioError(f"{label}.id duplicates a previous id: {change_id!r}")
            if change_id in seen_ids:
                raise ScenarioError(
                    f"{label}.id duplicates a clientCommands id: {change_id!r}"
                )
            seen_change_ids.add(change_id)
            action = change["action"]
            if action not in (MEMBER_ADD, MEMBER_REMOVE):
                raise ScenarioError(f"{label}.action must be add or remove")
            member = change["member"]
            if not isinstance(member, str) or not member:
                raise ScenarioError(f"{label}.member must be a non-empty string")
            if member not in node_set:
                raise ScenarioError(f"{label}.member references unknown node: {member!r}")
            membership_changes.append(
                {
                    "time": time,
                    "node": node,
                    "id": change_id,
                    "action": action,
                    "member": member,
                }
            )

    return {
        "nodes": list(nodes),
        "duration": duration,
        "electionTimeouts": timeouts,
        "heartbeatInterval": heartbeat,
        "messageDelay": delay,
        "faults": normalized_faults,
        "clientCommands": normalized_commands,
        "nodeEvents": normalized_node_events,
        "nodeEventsProvided": node_events_provided,
        "snapshotThreshold": snapshot_threshold,
        "membershipProvided": membership_provided,
        "initialMembers": initial_members,
        "membershipChanges": membership_changes,
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
        "snapshot_index",
        "snapshot_term",
        "commit_index",
        "last_applied",
        "applied",
        "next_index",
        "match_index",
        "online",
        "restart_count",
        "latest_config",
    )

    def __init__(self, initial_config: dict) -> None:
        self.role = ROLE_FOLLOWER
        self.term = 0
        self.voted_for: str | None = None
        self.known_leader: str | None = None
        self.votes: set[str] = set()
        self.timeout_gen = 0
        # Log entries keep their global indices: self.log holds only the
        # uncompacted suffix, so entry for global index i is self.log[i - 1 -
        # self.snapshot_index]. snapshot_index/snapshot_term describe the last
        # entry folded into the (modeled) snapshot; 0 means no snapshot yet.
        self.log: list[dict] = []
        self.snapshot_index = 0
        self.snapshot_term = 0
        self.commit_index = 0
        self.last_applied = 0
        self.applied: list[dict] = []
        # Leader-only replication progress, keyed by peer name.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        # Crash/restart lifecycle. term, voted_for, log, snapshot_index,
        # snapshot_term, commit_index, last_applied, applied and
        # latest_config model persisted state: every change to them is made
        # (synchronously) before the response that depends on it, so they
        # survive a crash and are simply kept on restart. Everything else is
        # volatile and is reset when the node comes back up.
        self.online = True
        self.restart_count = 0
        # The newest configuration the node knows (appended to its log, or
        # restored from a snapshot): {"phase": "stable", "members": [...]} or
        # {"phase": "joint", "old": [...], "new": [...]}.
        self.latest_config: dict = {
            "phase": PHASE_STABLE,
            "members": list(initial_config["members"]),
        }

    def last_log_index(self) -> int:
        return self.snapshot_index + len(self.log)

    def last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else self.snapshot_term

    def term_at(self, index: int) -> int:
        if index <= 0:
            return 0
        if index == self.snapshot_index:
            return self.snapshot_term
        if index < self.snapshot_index:
            raise IndexError(f"index {index} is covered by the snapshot")
        return self.log[index - 1 - self.snapshot_index]["term"]

    def has_entry(self, index: int) -> bool:
        return self.snapshot_index < index <= self.last_log_index()


class _Simulator:
    def __init__(self, config: dict) -> None:
        self.node_names: list[str] = config["nodes"]
        self.duration: int = config["duration"]
        self.timeouts: dict[str, int] = config["electionTimeouts"]
        self.heartbeat_interval: int = config["heartbeatInterval"]
        self.delay: int = config["messageDelay"]
        self.faults: list[dict] = config["faults"]
        self.commands: list[dict] = config["clientCommands"]
        self.node_events: list[dict] = config["nodeEvents"]
        self.node_events_provided: bool = config["nodeEventsProvided"]
        self.snapshot_threshold: int | None = config["snapshotThreshold"]
        self.membership_enabled: bool = config["membershipProvided"]
        if self.membership_enabled:
            self.membership_changes: list[dict] = config["membershipChanges"]
            initial_members = list(config["initialMembers"])
        else:
            self.membership_changes = []
            initial_members = list(self.node_names)
        self.initial_members: list[str] = initial_members
        # latest_config holds the newest configuration shared by every node at
        # startup; it is a stable config over the initial voters. Learners are
        # exactly the declared nodes outside it.
        bootstrap_config = {"phase": PHASE_STABLE, "members": list(initial_members)}
        self.state = {name: _Node(bootstrap_config) for name in self.node_names}
        self.index = {name: i for i, name in enumerate(self.node_names)}
        self.initial_voters: frozenset[str] = frozenset(initial_members)
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
        # Commands rejected at the door (target was not leader, or was down)
        # never reach a log. Final committed / superseded / pending outcomes
        # are derived from the end state; reason and knownLeader are captured
        # at rejection time.
        self.rejected: dict[str, dict] = {}
        # Membership-change outcomes, captured at request time and at the
        # moments configurations apply, so the final summary is deterministic
        # even after crashes.
        self.change_results: dict[str, dict] = {}
        # The change currently in flight cluster-wide (None when idle): the
        # raw request plus bookkeeping for its catch-up, joint and stable
        # entries. Re-derived from the replicated log whenever a new leader
        # takes over, so it survives crashes and leadership changes.
        self.pending_change: dict | None = None
        # Cluster-wide committed configuration view for the final summary:
        # the voters of the newest committed stable config, and the joint
        # configuration in the window between a committed joint entry and its
        # committed stable entry.
        self.current_stable_members: list[str] = list(initial_members)
        self.joint_phase: dict | None = None

    # -- configuration helpers ----------------------------------------------

    @staticmethod
    def _config_majorities(config: dict) -> list[frozenset[str]]:
        """The voter sets whose own strict majority a joint configuration
        requires simultaneously; a stable config yields a single set."""
        if config["phase"] == PHASE_JOINT:
            return [frozenset(config["old"]), frozenset(config["new"])]
        return [frozenset(config["members"])]

    def _is_voter(self, name: str, config: dict) -> bool:
        return any(name in voters for voters in self._config_majorities(config))

    @staticmethod
    def _majority_of(count: int) -> int:
        return count // 2 + 1

    def _has_joint_majority(self, votes: set[str], config: dict) -> bool:
        cast = set(votes)
        for voters in self._config_majorities(config):
            if len(cast & voters) < self._majority_of(len(voters)):
                return False
        return True

    def _config_entry(self, index: int, term: int, config: dict, change_id: str | None) -> dict:
        return {
            "index": index,
            "term": term,
            "entryType": CONFIG_ENTRY,
            "id": change_id,
            "configuration": {
                "phase": config["phase"],
                **(
                    {"members": list(config["members"])}
                    if config["phase"] == PHASE_STABLE
                    else {"old": list(config["old"]), "new": list(config["new"])}
                ),
            },
        }

    @staticmethod
    def _entry_is_config(entry: dict) -> bool:
        return entry.get("entryType") == CONFIG_ENTRY

    def _stored_entry(self, index: int, wire: dict) -> dict:
        """A log/applied entry as stored locally and forwarded on the wire:
        configuration entries carry their configuration instead of a client
        command, so the two kinds are always distinguishable byte for byte."""
        if self._entry_is_config(wire):
            return {
                "index": index,
                "term": wire["term"],
                "entryType": CONFIG_ENTRY,
                "id": wire["id"],
                "configuration": dict(wire["configuration"]),
            }
        return {
            "index": index,
            "term": wire["term"],
            "id": wire["id"],
            "command": wire["command"],
        }

    def _entry_config(self, entry: dict) -> dict:
        return dict(entry["configuration"])

    def run(self) -> dict:
        for order, fault in enumerate(self.faults):
            heapq.heappush(self.queue, (fault["time"], _KIND_FAULT, order, fault))
        for order, event in enumerate(self.node_events):
            heapq.heappush(self.queue, (event["time"], _KIND_NODE_EVENT, order, event))
        for order, command in enumerate(self.commands):
            heapq.heappush(self.queue, (command["time"], _KIND_CLIENT, order, command))
        for order, change in enumerate(self.membership_changes):
            heapq.heappush(self.queue, (change["time"], _KIND_MEMBERSHIP, order, change))
        for name in self.node_names:
            heapq.heappush(self.queue, (self.timeouts[name], _KIND_TIMEOUT, self.index[name], (name, 0)))
        while self.queue:
            time, kind, _order, payload = heapq.heappop(self.queue)
            if time > self.duration:
                break
            self.now = time
            self.reaction_phase = kind in (_KIND_CLIENT, _KIND_MEMBERSHIP, _KIND_REACTION)
            if kind == _KIND_FAULT:
                self._apply_fault(payload)
            elif kind == _KIND_NODE_EVENT:
                self._on_node_event(payload)
            elif kind in (_KIND_MESSAGE, _KIND_REACTION):
                self._deliver(payload)
            elif kind == _KIND_CLIENT:
                self._on_client_command(payload)
            elif kind == _KIND_MEMBERSHIP:
                self._on_membership_change(payload)
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

    def _can_campaign(self, name: str) -> bool:
        """Only current voters may start an election. Declared learners and
        nodes removed by a committed configuration stay followers; the
        decision reads the node's restored (persisted) configuration, so it
        holds across a crash restart."""
        return self._is_voter(name, self.state[name].latest_config)

    def _can_vote(self, name: str) -> bool:
        return self._is_voter(name, self.state[name].latest_config)

    def _recompute_latest_config(self, st: _Node) -> dict:
        """Newest configuration entry the node can see through its applied
        history (snapshot included) and uncompacted log; bootstrap config when
        no configuration entry exists yet. Configuration entries take effect
        as soon as they are appended, not only once committed."""
        config: dict | None = None
        for entry in st.applied:
            if self._entry_is_config(entry):
                config = self._entry_config(entry)
        for entry in st.log:
            if self._entry_is_config(entry):
                config = self._entry_config(entry)
        if config is None:
            return {"phase": PHASE_STABLE, "members": list(self.initial_voters)}
        return config

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
        voting_peers = set()
        for voters in self._config_majorities(st.latest_config):
            voting_peers.update(voters)
        for peer in self.node_names:
            if peer != name and peer in voting_peers:
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
        # Resume any membership change left mid-flight by the previous leader:
        # the configuration lives in the replicated log, so a joint entry is
        # finished with its stable entry, and an appended-but-uncommitted
        # stable entry keeps the change pending until it commits.
        self._adopt_pending_change(name)
        self._send_heartbeats(name)

    def _newest_config_entry(self, st: _Node) -> dict | None:
        newest = None
        for entry in st.applied:
            if self._entry_is_config(entry):
                newest = entry
        for entry in st.log:
            if self._entry_is_config(entry):
                newest = entry
        return newest

    def _change_shape(self, st: _Node, change_id: str) -> tuple[str | None, str | None]:
        """Recover (action, member) for a stable entry from its joint entry:
        the member is the one added to, or removed from, the old set."""
        for entry in list(st.applied) + list(st.log):
            if (
                self._entry_is_config(entry)
                and entry.get("id") == change_id
                and entry["configuration"]["phase"] == PHASE_JOINT
            ):
                old = frozenset(entry["configuration"]["old"])
                new = frozenset(entry["configuration"]["new"])
                added = [n for n in self.node_names if n in new - old]
                if added:
                    return MEMBER_ADD, added[0]
                removed = [n for n in self.node_names if n in old - new]
                if removed:
                    return MEMBER_REMOVE, removed[0]
        return None, None

    def _adopt_pending_change(self, leader: str) -> None:
        """Resume a membership change left in flight by a prior leader (or by
        this leader before a crash). The configuration lives in the replicated
        log: a joint entry is finished by appending its stable entry, and an
        appended-but-uncommitted stable entry simply stays pending until it
        commits. Nothing is re-applied: the entries already exist."""
        st = self.state[leader]
        newest = self._newest_config_entry(st)
        if newest is None:
            self.pending_change = None
            return
        config = self._entry_config(newest)
        change_id = newest["id"]
        if config["phase"] == PHASE_JOINT:
            old = [n for n in self.node_names if n in frozenset(config["old"])]
            new = [n for n in self.node_names if n in frozenset(config["new"])]
            added = [n for n in new if n not in frozenset(old)]
            removed = [n for n in old if n not in frozenset(new)]
            if added:
                action, member = MEMBER_ADD, added[0]
            else:
                action, member = MEMBER_REMOVE, removed[0]
            self.pending_change = {
                "id": change_id,
                "action": action,
                "member": member,
                "node": leader,
                "phase": "joint",
                "jointIndex": newest["index"],
                "stableIndex": None,
            }
            if newest["index"] <= st.commit_index:
                # The joint entry is already committed: finish immediately.
                self._append_stable_entry(leader)
        else:
            if newest["index"] <= st.commit_index:
                # The stable entry is already committed on the new leader:
                # the change is over. Historical configurationApplied events
                # are not re-emitted; just clear the in-flight slot and, if
                # the outcome was recorded on a leader that has since crashed,
                # mark it committed from the replicated log.
                if change_id not in self.change_results or (
                    self.change_results[change_id].get("result") == "pending"
                ):
                    action, member = self._change_shape(st, change_id)
                    joint = self._find_config_entry(change_id, PHASE_JOINT)
                    self.change_results[change_id] = {
                        "id": change_id,
                        "node": leader,
                        "action": action,
                        "member": member,
                        "result": "committed",
                        "jointIndex": joint["index"] if joint is not None else None,
                        "stableIndex": newest["index"],
                    }
                self.pending_change = None
            else:
                # Stable entry appended but not yet committed: replication
                # and the commit machinery finish it. Recover the request's
                # action/member from the preceding joint entry so finalize
                # bookkeeping stays complete after a leadership change.
                action, member = self._change_shape(st, change_id)
                self.pending_change = {
                    "id": change_id,
                    "node": leader,
                    "action": action,
                    "member": member,
                    "phase": "stable",
                    "jointIndex": None,
                    "stableIndex": newest["index"],
                }

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
        """Send the next due message for one peer: an installSnapshot when the
        peer's next position is already covered by the leader's snapshot,
        otherwise an appendEntries carrying the uncompacted suffix slice."""
        st = self.state[name]
        next_idx = st.next_index[peer]
        if next_idx <= st.snapshot_index:
            # The snapshot bundles the applied state through lastIncludedIndex.
            snapshot_entries = [dict(entry) for entry in st.applied[: st.snapshot_index]]
            self._send(
                name,
                peer,
                {
                    "kind": "installSnapshot",
                    "term": st.term,
                    "lastIncludedIndex": st.snapshot_index,
                    "lastIncludedTerm": st.snapshot_term,
                    "entries": snapshot_entries,
                },
            )
            return
        prev_idx = next_idx - 1
        entries = [dict(entry) for entry in st.log[next_idx - 1 - st.snapshot_index:]]
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
        if not self.state[dst].online:
            # The destination crashed after this message was sent; it is
            # dropped on arrival. Messages the crashed node itself sent
            # earlier are unaffected and still land at their original time.
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": msg["kind"],
                "term": msg["term"],
                "result": "dropped",
                "reason": "nodeDown",
            })
            return
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
        elif kind == "installSnapshot":
            self._handle_install_snapshot(msg)
        elif kind == "installSnapshotReply":
            self._handle_snapshot_reply(msg)
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
        eligible_voter = self._can_vote(dst)
        candidate_in_config = self._is_voter(src, st.latest_config)
        granted = (
            eligible_voter
            and candidate_in_config
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
            elected = self._has_joint_majority(st.votes, st.latest_config)
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
        prefix_ok = (
            prev_index >= st.snapshot_index
            and prev_index <= st.last_log_index()
            and st.term_at(prev_index) == msg["prevLogTerm"]
        )
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
            if st.has_entry(index) and st.log[index - 1 - st.snapshot_index]["term"] != entry["term"]:
                conflict_index = index
                del st.log[index - 1 - st.snapshot_index:]
                break

        # Append whatever is not already present with a matching term.
        appended = 0
        appended_configs: list[dict] = []
        for offset, wire in enumerate(msg["entries"]):
            index = prev_index + 1 + offset
            if index > st.last_log_index():
                stored = self._stored_entry(index, wire)
                st.log.append(stored)
                appended += 1
                if self._entry_is_config(stored):
                    appended_configs.append(
                        {
                            "index": index,
                            "term": stored["term"],
                            "phase": stored["configuration"]["phase"],
                            "id": stored.get("id"),
                        }
                    )

        record = {
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
        }
        if self.membership_enabled and appended_configs:
            record["configurations"] = appended_configs
        self._record(record)
        if appended_configs or conflict_index:
            # A newly appended configuration takes effect immediately, and a
            # truncation may remove the newest one; recompute from the
            # persisted prefix (applied history incl. snapshot) plus the log.
            st.latest_config = self._recompute_latest_config(st)

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

    def _handle_install_snapshot(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        included_index = msg["lastIncludedIndex"]
        included_term = msg["lastIncludedTerm"]
        if msg["term"] < st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshot",
                "term": msg["term"],
                "result": "delivered",
                "detail": "staleTerm",
                "lastIncludedIndex": included_index,
            })
            self._send(
                dst,
                src,
                {
                    "kind": "installSnapshotReply",
                    "term": st.term,
                    "result": "staleTerm",
                    "lastIncludedIndex": included_index,
                    "snapshotIndex": st.snapshot_index,
                    "lastLogIndex": st.last_log_index(),
                },
            )
            return

        if msg["term"] > st.term or st.role != ROLE_FOLLOWER:
            self._become_follower(dst, msg["term"], "installSnapshot")
        else:
            self._reset_timeout(dst)
        st.known_leader = src

        if included_index <= st.snapshot_index:
            # An equal-or-newer snapshot already covers this position.
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshot",
                "term": msg["term"],
                "result": "delivered",
                "detail": "ignored",
                "lastIncludedIndex": included_index,
            })
            self._send(
                dst,
                src,
                {
                    "kind": "installSnapshotReply",
                    "term": st.term,
                    "result": "ignored",
                    "lastIncludedIndex": included_index,
                    "snapshotIndex": st.snapshot_index,
                    "lastLogIndex": st.last_log_index(),
                },
            )
            return

        # Accept the snapshot. A suffix entry at the snapshot boundary with a
        # matching term is still valid and is kept; any other suffix is
        # discarded, including a same-position entry from a different term.
        keep = (
            st.has_entry(included_index)
            and st.term_at(included_index) == included_term
        )
        snapshot_entries = [dict(entry) for entry in msg["entries"]]
        if keep:
            # The entry at included_index joins the snapshot; retain only the
            # entries strictly following it, together with whatever the
            # follower had already applied beyond the snapshot point (such
            # entries were committed and must not be applied twice).
            retained_applied = [
                dict(entry) for entry in st.applied if entry["index"] > included_index
            ]
            st.log = st.log[included_index - st.snapshot_index:]
            st.applied = snapshot_entries + retained_applied
        else:
            st.log = []
            # Snapshot contents arrive as already-applied state: restore them
            # silently, so no applied events are emitted for them.
            st.applied = snapshot_entries
        st.snapshot_index = included_index
        st.snapshot_term = included_term
        if st.commit_index < included_index:
            st.commit_index = included_index
        st.last_applied = len(st.applied)
        # Snapshot contents are restored silently: the newest configuration
        # folded into the snapshot becomes the node's known configuration
        # without re-emitting configurationApplied events.
        st.latest_config = self._recompute_latest_config(st)

        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "installSnapshot",
            "term": msg["term"],
            "result": "delivered",
            "detail": "installed",
            "lastIncludedIndex": included_index,
            "lastIncludedTerm": included_term,
        })
        self._record({
            "type": "snapshotInstalled",
            "node": dst,
            "peer": src,
            "lastIncludedIndex": included_index,
            "lastIncludedTerm": included_term,
        })
        # Apply whatever suffix entries are now committed beyond the snapshot.
        self._advance_apply(dst)
        self._send(
            dst,
            src,
            {
                "kind": "installSnapshotReply",
                "term": st.term,
                "result": "installed",
                "lastIncludedIndex": included_index,
                "snapshotIndex": st.snapshot_index,
                "lastLogIndex": st.last_log_index(),
            },
        )

    def _handle_snapshot_reply(self, msg: dict) -> None:
        src, dst = msg["src"], msg["dst"]
        st = self.state[dst]
        if msg["term"] > st.term:
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshotReply",
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
                "message": "installSnapshotReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "ignored",
            })
            return

        result = msg["result"]
        if result == "installed":
            included = msg["lastIncludedIndex"]
            if included > st.match_index[src]:
                st.match_index[src] = included
            if included + 1 > st.next_index[src]:
                st.next_index[src] = included + 1
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshotReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "installed",
                "matchIndex": st.match_index[src],
            })
            # Resume log replication from the entry right after the snapshot.
            self._replicate_to(dst, src)
            # A snapshot may itself have completed an addition's catch-up when
            # there is no suffix left to replicate.
            self._progress_membership(dst, src)
            return

        if result == "staleTerm":
            # Same-term refusal (the peer led in this term, for example): no
            # progress change; the next heartbeat retries reconciliation.
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": "installSnapshotReply",
                "term": msg["term"],
                "result": "delivered",
                "detail": "staleTerm",
            })
            return

        # The follower already holds an equal-or-newer snapshot: advance its
        # progress conservatively and let appendEntries reconcile the suffix.
        peer_snapshot = min(msg["snapshotIndex"], st.last_log_index())
        if peer_snapshot > st.match_index[src]:
            st.match_index[src] = peer_snapshot
        if peer_snapshot + 1 > st.next_index[src]:
            st.next_index[src] = peer_snapshot + 1
        self._record({
            "type": "messageResult",
            "node": dst,
            "peer": src,
            "message": "installSnapshotReply",
            "term": msg["term"],
            "result": "delivered",
            "detail": "ignored",
            "matchIndex": st.match_index[src],
        })
        self._replicate_to(dst, src)

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
                self._progress_membership(dst, src)
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
        if not st.online:
            self.rejected[command_id] = {"node": node, "reason": "nodeDown", "knownLeader": None}
            self._record({
                "type": "clientResult",
                "node": node,
                "id": command_id,
                "result": "rejected",
                "reason": "nodeDown",
                "knownLeader": None,
            })
            return
        if st.role != ROLE_LEADER:
            self.rejected[command_id] = {"node": node, "reason": "notLeader", "knownLeader": st.known_leader}
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

    # -- membership changes ---------------------------------------------------

    def _on_membership_change(self, change: dict) -> None:
        node = change["node"]
        change_id = change["id"]
        action = change["action"]
        member = change["member"]
        st = self.state[node]

        def reject(reason: str) -> None:
            self.change_results[change_id] = {
                "id": change_id,
                "node": node,
                "action": action,
                "member": member,
                "result": "rejected",
                "reason": reason,
            }
            self._record({
                "type": "membershipResult",
                "node": node,
                "id": change_id,
                "action": action,
                "member": member,
                "result": "rejected",
                "reason": reason,
            })

        # The checks run in the exact precedence promised by the scenario
        # contract: liveness first, then leadership, an in-flight change,
        # whether the action fits the member's current state, and finally the
        # three-voter floor for a removal.
        if not st.online:
            reject("nodeDown")
            return
        if st.role != ROLE_LEADER:
            reject("notLeader")
            return
        if self.pending_change is not None:
            reject("changeInProgress")
            return
        is_voter = self._is_voter(member, st.latest_config)
        if action == MEMBER_ADD and is_voter:
            reject("alreadyMember")
            return
        if action == MEMBER_REMOVE and not is_voter:
            reject("notMember")
            return
        if action == MEMBER_REMOVE:
            active_voters: set[str] = set()
            for voters in self._config_majorities(st.latest_config):
                active_voters.update(voters)
            remaining = [n for n in self._ordered_members(frozenset(active_voters)) if n != member]
            if len(remaining) < MIN_VOTERS:
                reject("minimumClusterSize")
                return

        self.change_results[change_id] = {
            "id": change_id,
            "node": node,
            "action": action,
            "member": member,
            "result": "pending",
        }
        self.pending_change = {
            "id": change_id,
            "node": node,
            "action": action,
            "member": member,
            "phase": "catchup",
            "jointIndex": None,
            "stableIndex": None,
            "catchupTarget": st.last_log_index(),
        }
        self._record({
            "type": "membershipResult",
            "node": node,
            "id": change_id,
            "action": action,
            "member": member,
            "result": "accepted",
            "phase": "catchup" if action == MEMBER_ADD else "joint",
        })
        if action == MEMBER_ADD:
            # Bring the learner up first, by log replication or a snapshot
            # install; the joint entry is appended once it matches the
            # leader through catchupTarget.
            self._replicate_to(node, member)
            self._progress_membership(node, member)
        else:
            # A removal enters joint consensus immediately.
            self._append_joint_entry(node)

    def _progress_membership(self, leader: str, peer: str) -> None:
        """Advance an in-flight change when a peer's replication progress
        moves: finish learner catch-up for an addition."""
        change = self.pending_change
        st = self.state[leader]
        if (
            st.role != ROLE_LEADER
            or change is None
            or change["phase"] != "catchup"
            or change["action"] != MEMBER_ADD
            or peer != change["member"]
        ):
            return
        if st.match_index.get(peer, 0) >= change["catchupTarget"]:
            self._append_joint_entry(leader)

    def _ordered_members(self, members: frozenset[str]) -> list[str]:
        return [name for name in self.node_names if name in frozenset(members)]

    def _append_joint_entry(self, leader: str) -> None:
        st = self.state[leader]
        change = self.pending_change
        old_members = self._ordered_members(frozenset(st.latest_config["members"]))
        old_set = frozenset(old_members)
        if change["action"] == MEMBER_ADD:
            new_set = old_set | {change["member"]}
        else:
            new_set = old_set - {change["member"]}
        joint = {
            "phase": PHASE_JOINT,
            "old": self._ordered_members(old_set),
            "new": self._ordered_members(new_set),
        }
        index = st.last_log_index() + 1
        entry = self._config_entry(index, st.term, joint, change["id"])
        st.log.append(entry)
        st.latest_config = joint
        change["phase"] = "joint"
        change["jointIndex"] = index
        for peer in self.node_names:
            if peer != leader:
                self._replicate_to(leader, peer)

    def _append_stable_entry(self, leader: str) -> None:
        st = self.state[leader]
        change = self.pending_change
        joint = st.latest_config
        new_members = self._ordered_members(frozenset(joint["new"]))
        stable = {"phase": PHASE_STABLE, "members": new_members}
        index = st.last_log_index() + 1
        entry = self._config_entry(index, st.term, stable, change["id"])
        st.log.append(entry)
        st.latest_config = stable
        change["phase"] = "stable"
        change["stableIndex"] = index
        for peer in self.node_names:
            if peer != leader:
                self._replicate_to(leader, peer)

    # -- commit and apply ---------------------------------------------------

    def _replicated_on_config(self, leader: str, st: _Node, index: int, config: dict) -> bool:
        """Whether index is available on a strict majority of every voter set
        of the given (stable or joint) configuration, counting the leader
        itself (which holds the index) when it belongs to that set."""
        leader_holds = st.last_log_index() >= index
        for voters in self._config_majorities(config):
            count = 1 if leader_holds and leader in voters else 0
            for peer in voters:
                if peer == leader:
                    continue
                if st.match_index.get(peer, 0) >= index:
                    count += 1
            if count < self._majority_of(len(voters)):
                return False
        return True

    def _advance_leader_commit(self, name: str) -> None:
        st = self.state[name]
        config = st.latest_config
        # Scan from the tip down: entries from older terms are never committed
        # by counting alone, so the highest eligible current-term index with a
        # joint (or stable) quorum is the new commit position.
        for candidate in range(st.last_log_index(), st.commit_index, -1):
            if st.term_at(candidate) != st.term:
                continue
            if not self._replicated_on_config(name, st, candidate, config):
                continue
            st.commit_index = candidate
            self._record({
                "type": "commitAdvance",
                "node": name,
                "term": st.term,
                "commitIndex": st.commit_index,
            })
            self._advance_apply(name)
            return

    def _advance_apply(self, name: str) -> None:
        st = self.state[name]
        while st.last_applied < st.commit_index:
            next_index = st.last_applied + 1
            if not st.has_entry(next_index):
                break
            st.last_applied = next_index
            entry = st.log[next_index - 1 - st.snapshot_index]
            stored = self._stored_entry(entry["index"], entry)
            st.applied.append(stored)
            if self._entry_is_config(stored):
                self._apply_configuration(name, stored)
            else:
                self._record({
                    "type": "applied",
                    "node": name,
                    "index": entry["index"],
                    "term": entry["term"],
                    "id": entry["id"],
                })
            if (
                self.snapshot_threshold is not None
                and st.last_applied - st.snapshot_index >= self.snapshot_threshold
            ):
                self._create_snapshot(name)

    def _apply_configuration(self, name: str, entry: dict) -> None:
        """A committed configuration entry takes effect: it is recorded (but
        never emits a client-style applied event), the leader completes the
        joint consensus protocol, and a leader removed by the stable config
        steps down immediately."""
        st = self.state[name]
        config = self._entry_config(entry)
        st.latest_config = config
        phase = config["phase"]
        applied_event = {
            "type": "configurationApplied",
            "node": name,
            "index": entry["index"],
            "term": entry["term"],
            "phase": phase,
            "id": entry.get("id"),
        }
        if self.membership_enabled:
            applied_event["configuration"] = config
        self._record(applied_event)

        if not self.membership_enabled:
            return
        if phase == PHASE_JOINT:
            self.joint_phase = {"old": list(config["old"]), "new": list(config["new"])}
            if st.role == ROLE_LEADER and self.pending_change is not None:
                if self.pending_change.get("phase") == "joint":
                    self.pending_change["jointIndex"] = entry["index"]
                    # The joint configuration is committed: append the stable
                    # configuration that ends the change.
                    self._append_stable_entry(name)
            return

        # Stable configuration committed.
        self.joint_phase = None
        self.current_stable_members = list(config["members"])
        new_members = frozenset(config["members"])
        if st.role == ROLE_LEADER and self.pending_change is not None:
            change = self.pending_change
            change["phase"] = "stable"
            change["stableIndex"] = entry["index"]
            self.change_results[change["id"]] = {
                **self._change_request_view(change),
                "result": "committed",
                "jointIndex": change.get("jointIndex"),
                "stableIndex": entry["index"],
            }
            self.pending_change = None
        if name not in new_members:
            # The removed leader becomes a follower at once and, from now on,
            # neither campaigns nor votes (its persisted configuration no
            # longer contains it).
            if st.role != ROLE_FOLLOWER:
                self._become_follower(name, st.term, "configurationCommitted")
            st.known_leader = (
                st.known_leader if st.known_leader in new_members else None
            )

    @staticmethod
    def _change_request_view(change: dict) -> dict:
        return {
            "id": change["id"],
            "node": change.get("node"),
            "action": change.get("action"),
            "member": change.get("member"),
        }

    def _create_snapshot(self, name: str) -> None:
        """Fold every applied entry through last_applied into the snapshot and
        delete the compacted prefix. Global indices are preserved: the
        surviving log suffix keeps its indices and commitIndex/lastApplied are
        not renumbered."""
        st = self.state[name]
        target = st.last_applied
        cut = target - st.snapshot_index
        included_term = st.log[cut - 1]["term"]
        st.snapshot_index = target
        st.snapshot_term = included_term
        del st.log[:cut]
        self._record({
            "type": "snapshotCreated",
            "node": name,
            "lastIncludedIndex": target,
            "lastIncludedTerm": included_term,
        })

    # -- timeouts, heartbeats, faults ---------------------------------------

    def _on_timeout(self, name: str, generation: int) -> None:
        st = self.state[name]
        if not st.online or generation != st.timeout_gen or st.role == ROLE_LEADER:
            return
        if not self._can_campaign(name):
            # Learners and removed nodes keep arming timeouts but never vote
            # or campaign; reschedule so the timer keeps ticking deterministically.
            self._reset_timeout(name)
            return
        self._record({"type": "timeout", "node": name, "term": st.term, "reason": "electionTimeout"})
        self._start_election(name)

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
            self._record({"type": "nodeLifecycle", "node": name, "action": "crash"})
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
        self._reset_timeout(name)
        self._record({"type": "nodeLifecycle", "node": name, "action": "restart"})

    def _apply_fault(self, fault: dict) -> None:
        if fault["action"] == "partition":
            self.partition = [frozenset(group) for group in fault["groups"]]
            self._record({"type": "fault", "action": "partition", "groups": fault["groups"]})
        else:
            self.partition = None
            self._record({"type": "fault", "action": "heal"})

    # -- report ---------------------------------------------------------------

    def _entry_view(self, st: _Node, index: int) -> dict | None:
        """The (term, id, command) a node holds for a global log index, looking
        through snapshot boundaries: applied entries cover the compacted prefix
        and the uncompacted log covers everything after it."""
        if index <= 0 or index > st.last_log_index():
            return None
        if index <= st.last_applied:
            return st.applied[index - 1]
        if st.has_entry(index):
            return st.log[index - 1 - st.snapshot_index]
        return None

    def _log_matching_violations(self) -> list[dict]:
        """Same index and term, but different content across nodes, including
        indices folded into snapshots. Client entries differ in id/command and
        configuration entries in their configuration; the two kinds can never
        match each other at the same index and term either."""
        violations = []
        max_len = max((st.last_log_index() for st in self.state.values()), default=0)
        for index in range(1, max_len + 1):
            by_term: dict[int, dict[str, dict]] = {}
            for name in self.node_names:
                st = self.state[name]
                entry = self._entry_view(st, index)
                if entry is None:
                    continue
                if self._entry_is_config(entry):
                    marker = json.dumps(
                        ["configuration", entry["configuration"]],
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    bucket = by_term.setdefault(entry["term"], {}).setdefault(
                        marker,
                        {
                            "entryType": CONFIG_ENTRY,
                            "configuration": dict(entry["configuration"]),
                            "nodes": [],
                        },
                    )
                else:
                    marker = json.dumps(
                        ["command", entry["id"], entry["command"]],
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    bucket = by_term.setdefault(entry["term"], {}).setdefault(
                        marker,
                        {"id": entry["id"], "command": entry["command"], "nodes": []},
                    )
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
        """Different nodes applied different entries at the same index.
        Configuration entries are applied (committed) too and participate in
        the check; client entries never collide with configuration entries."""
        violations = []
        max_applied = max((st.last_applied for st in self.state.values()), default=0)
        for index in range(1, max_applied + 1):
            variants: dict[str, dict] = {}
            for name in self.node_names:
                st = self.state[name]
                if index > st.last_applied:
                    continue
                entry = st.applied[index - 1]
                if self._entry_is_config(entry):
                    marker = json.dumps(
                        ["configuration", entry["term"], entry["configuration"]],
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    bucket = variants.setdefault(
                        marker,
                        {
                            "term": entry["term"],
                            "entryType": CONFIG_ENTRY,
                            "configuration": dict(entry["configuration"]),
                            "nodes": [],
                        },
                    )
                else:
                    marker = json.dumps(
                        ["command", entry["term"], entry["id"], entry["command"]],
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    bucket = variants.setdefault(
                        marker,
                        {"term": entry["term"], "id": entry["id"], "command": entry["command"], "nodes": []},
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
                        "reason": rejected["reason"],
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

    def _node_report(self, name: str, st: _Node) -> dict:
        report = {
            "role": st.role,
            "term": st.term,
            "votedFor": st.voted_for,
            "knownLeader": st.known_leader,
            "log": [dict(entry) for entry in st.log],
            "commitIndex": st.commit_index,
            "lastApplied": st.last_applied,
            "applied": [dict(entry) for entry in st.applied],
        }
        if self.membership_enabled:
            report["membershipRole"] = self._membership_role(name, st.latest_config)
        if self.snapshot_threshold is not None:
            if st.snapshot_index > 0:
                report["snapshot"] = {
                    "lastIncludedIndex": st.snapshot_index,
                    "lastIncludedTerm": st.snapshot_term,
                }
            else:
                report["snapshot"] = None
        if self.node_events_provided:
            report["online"] = st.online
            report["restartCount"] = st.restart_count
        return report

    def _membership_role(self, name: str, config: dict) -> str:
        if config["phase"] == PHASE_JOINT:
            in_old = name in frozenset(config["old"])
            in_new = name in frozenset(config["new"])
            if in_old and in_new:
                return "voter"
            if in_old:
                return "voterOld"
            if in_new:
                return "voterNew"
            return "learner"
        return "voter" if name in frozenset(config["members"]) else "learner"

    def _find_config_entry(self, change_id: str, phase: str | None = None) -> dict | None:
        """Newest config entry for a change id visible in any node's applied
        history (snapshots included) or uncompacted log."""
        newest = None
        for node_name in self.node_names:
            st = self.state[node_name]
            for entry in list(st.applied) + list(st.log):
                if not self._entry_is_config(entry) or entry.get("id") != change_id:
                    continue
                if phase is not None and entry["configuration"]["phase"] != phase:
                    continue
                if newest is None or entry["index"] > newest["index"]:
                    newest = entry
        return newest

    def _stable_is_committed(self, change_id: str) -> dict | None:
        """A stable configuration applied (committed) on any node ends the
        change; entries folded into a snapshot stay in applied history, so a
        compacted stable entry is still detected."""
        for node_name in self.node_names:
            for entry in self.state[node_name].applied:
                if (
                    self._entry_is_config(entry)
                    and entry.get("id") == change_id
                    and entry["configuration"]["phase"] == PHASE_STABLE
                ):
                    return entry
        return None

    def _membership_report(self) -> dict:
        changes = []
        for change in self.membership_changes:
            change_id = change["id"]
            recorded = self.change_results.get(change_id)
            entry: dict = {
                "id": change_id,
                "node": change["node"],
                "action": change["action"],
                "member": change["member"],
            }
            if recorded is not None and recorded.get("result") == "rejected":
                entry["outcome"] = "rejected"
                entry["reason"] = recorded["reason"]
                changes.append(entry)
                continue
            stable_applied = self._stable_is_committed(change_id)
            if stable_applied is not None:
                joint = self._find_config_entry(change_id, PHASE_JOINT)
                entry["outcome"] = "committed"
                entry["jointIndex"] = joint["index"] if joint is not None else None
                entry["stableIndex"] = stable_applied["index"]
                changes.append(entry)
                continue
            # Still in flight at the end of the scenario.
            stable = self._find_config_entry(change_id, PHASE_STABLE)
            joint = self._find_config_entry(change_id, PHASE_JOINT)
            entry["outcome"] = "pending"
            if stable is not None:
                entry["phase"] = "stable"
                entry["stableIndex"] = stable["index"]
            elif joint is not None:
                entry["phase"] = "joint"
                entry["jointIndex"] = joint["index"]
            else:
                entry["phase"] = "catchup"
            changes.append(entry)
        return {
            "initialMembers": list(self.initial_members),
            "currentMembers": list(self.current_stable_members),
            "joint": None
            if self.joint_phase is None
            else {"old": list(self.joint_phase["old"]), "new": list(self.joint_phase["new"])},
            "changes": changes,
        }

    def _report(self) -> dict:
        leaders = {str(term): names for term, names in sorted(self.leaders_by_term.items())}
        violations = [
            {"term": term, "leaders": list(names)}
            for term, names in sorted(self.leaders_by_term.items())
            if len(names) > 1
        ]
        report = {
            "timeline": self.timeline,
            "nodes": {
                name: self._node_report(name, st)
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
        if self.membership_enabled:
            report["membership"] = self._membership_report()
        return report


def run_simulation(raw: object) -> dict:
    """Validate a decoded JSON scenario and run the deterministic simulation."""
    return _Simulator(parse_scenario(raw)).run()
