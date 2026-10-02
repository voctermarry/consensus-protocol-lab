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
# Replication traffic triggered by client commands (and its replies) lands
# here so that, even with zero message delay, all client commands sharing a
# timestamp are handled in input order before their reactions drain.
_KIND_REACTION = 4
# Membership requests sit behind every pending reaction: the heap keeps
# (t, REACTION, ...) entries ahead of (t, MEMBERSHIP, ...), so a request's own
# zero-delay cascade likewise drains before the next timed event.
_KIND_MEMBERSHIP = 5
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
    "messageFaults",
}
_REQUIRED_FIELDS = _TOP_LEVEL_FIELDS - {
    "faults",
    "clientCommands",
    "nodeEvents",
    "snapshotThreshold",
    "initialMembers",
    "membershipChanges",
    "messageFaults",
}
_FAULT_FIELDS = {"time", "action", "groups"}
_MESSAGE_FAULT_FIELDS = {"from", "to", "message", "occurrence", "action", "delay"}
_MESSAGE_FAULT_REQUIRED = {"from", "to", "message", "occurrence", "action"}
_MESSAGE_KINDS = (
    "requestVote",
    "voteReply",
    "heartbeat",
    "appendEntries",
    "appendReply",
    "installSnapshot",
    "installSnapshotReply",
)
_CLIENT_COMMAND_FIELDS = {"time", "node", "id", "command"}
_NODE_EVENT_FIELDS = {"time", "node", "action"}
_MEMBERSHIP_CHANGE_FIELDS = {"time", "node", "id", "action", "member"}

# Log-entry kinds: ordinary client commands versus replicated configuration
# entries created by joint-consensus membership changes.
KIND_COMMAND = "command"
KIND_CONFIG = "config"
CONFIG_JOINT = "joint"
CONFIG_STABLE = "stable"


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

    message_faults = raw.get("messageFaults", [])
    if not isinstance(message_faults, list):
        raise ScenarioError("messageFaults must be a list")
    normalized_message_faults = []
    seen_selectors: dict[tuple, int] = {}
    for index, message_fault in enumerate(message_faults):
        label = f"messageFaults[{index}]"
        if not isinstance(message_fault, dict):
            raise ScenarioError(f"{label} must be an object")
        unknown = sorted(set(message_fault) - _MESSAGE_FAULT_FIELDS)
        if unknown:
            raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
        missing = sorted(_MESSAGE_FAULT_REQUIRED - set(message_fault))
        if missing:
            raise ScenarioError(f"{label} missing field(s): {', '.join(missing)}")
        src = message_fault["from"]
        if not isinstance(src, str) or not src:
            raise ScenarioError(f"{label}.from must be a non-empty string")
        if src not in node_set:
            raise ScenarioError(f"{label}.from references unknown node: {src!r}")
        dst = message_fault["to"]
        if not isinstance(dst, str) or not dst:
            raise ScenarioError(f"{label}.to must be a non-empty string")
        if dst not in node_set:
            raise ScenarioError(f"{label}.to references unknown node: {dst!r}")
        if src == dst:
            raise ScenarioError(f"{label}.from and {label}.to must be different nodes")
        kind = message_fault["message"]
        if kind not in _MESSAGE_KINDS:
            raise ScenarioError(
                f"{label}.message must be one of: {', '.join(_MESSAGE_KINDS)}"
            )
        occurrence = _require_int(message_fault["occurrence"], f"{label}.occurrence", 1)
        action = message_fault["action"]
        if action not in ("drop", "delay"):
            raise ScenarioError(f"{label}.action must be drop or delay")
        extra_delay = None
        if action == "drop":
            if "delay" in message_fault:
                raise ScenarioError(f"{label} drop must not have delay")
        else:
            if "delay" not in message_fault:
                raise ScenarioError(f"{label} delay requires delay")
            extra_delay = _require_int(message_fault["delay"], f"{label}.delay", 0)
        selector = (src, dst, kind, occurrence)
        if selector in seen_selectors:
            raise ScenarioError(
                f"{label} duplicates the selector of messageFaults[{seen_selectors[selector]}]"
            )
        seen_selectors[selector] = index
        normalized = {
            "index": index,
            "from": src,
            "to": dst,
            "message": kind,
            "occurrence": occurrence,
            "action": action,
        }
        if extra_delay is not None:
            normalized["delay"] = extra_delay
        normalized_message_faults.append(normalized)

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

    has_initial_members = "initialMembers" in raw
    has_membership_changes = "membershipChanges" in raw
    if has_initial_members != has_membership_changes:
        raise ScenarioError(
            "initialMembers and membershipChanges must be provided together"
        )

    initial_members: list[str] | None = None
    membership_enabled = has_initial_members
    if has_initial_members:
        raw_initial = raw["initialMembers"]
        if not isinstance(raw_initial, list) or len(raw_initial) < 3:
            raise ScenarioError(
                "initialMembers must be a list of at least three node names"
            )
        for name in raw_initial:
            if not isinstance(name, str) or not name:
                raise ScenarioError("initialMembers entries must be non-empty strings")
            if name not in node_set:
                raise ScenarioError(
                    f"initialMembers references unknown node: {name!r}"
                )
        if len(set(raw_initial)) != len(raw_initial):
            raise ScenarioError("initialMembers entries must be unique")
        initial_members = list(raw_initial)

        raw_changes = raw["membershipChanges"]
        if not isinstance(raw_changes, list):
            raise ScenarioError("membershipChanges must be a list")
        seen_change_ids: set[str] = set()
        normalized_changes = []
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
                raise ScenarioError(
                    f"{label}.id duplicates a previous id: {change_id!r}"
                )
            seen_change_ids.add(change_id)
            if change_id in seen_ids:
                raise ScenarioError(
                    f"{label}.id duplicates a clientCommands id: {change_id!r}"
                )
            action = change["action"]
            if action not in ("add", "remove"):
                raise ScenarioError(f"{label}.action must be add or remove")
            member = change["member"]
            if not isinstance(member, str) or not member:
                raise ScenarioError(f"{label}.member must be a non-empty string")
            if member not in node_set:
                raise ScenarioError(
                    f"{label}.member references unknown node: {member!r}"
                )
            normalized_changes.append(
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
        "messageFaults": normalized_message_faults,
        "clientCommands": normalized_commands,
        "nodeEvents": normalized_node_events,
        "nodeEventsProvided": node_events_provided,
        "snapshotThreshold": snapshot_threshold,
        "membershipEnabled": membership_enabled,
        "initialMembers": initial_members,
        "membershipChanges": normalized_changes if has_initial_members else None,
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
        "snapshot_config",
        "commit_index",
        "last_applied",
        "applied",
        "next_index",
        "match_index",
        "online",
        "restart_count",
    )

    def __init__(self) -> None:
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
        # Configuration in effect at snapshot_index (None before any
        # snapshot). Config entries can be compacted into snapshots like any
        # committed entry, so membership survives compaction.
        self.snapshot_config: dict | None = None
        self.commit_index = 0
        self.last_applied = 0
        self.applied: list[dict] = []
        # Leader-only replication progress, keyed by peer name.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        # Crash/restart lifecycle. term, voted_for, log, snapshot_index,
        # snapshot_term, snapshot_config, commit_index, last_applied, applied
        # and the configuration encoded in the log all model persisted state:
        # every change to them is made (synchronously) before the response that
        # depends on it, so they survive a crash and are simply kept on
        # restart. Everything else is volatile and is reset on restart.
        self.online = True
        self.restart_count = 0

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

    def config_at(self, index: int, initial_config: dict) -> dict | None:
        """The configuration in effect immediately *before* the entry at
        ``index`` is applied: the latest config entry with a smaller index, or
        the snapshot config / initial configuration. Returns None when no
        configuration entry precedes ``index`` (the caller substitutes the
        initial configuration)."""
        config = self.snapshot_config if self.snapshot_config else initial_config
        upper = min(index - 1, self.last_log_index())
        for i in range(self.snapshot_index + 1, upper + 1):
            entry = self.log[i - 1 - self.snapshot_index]
            if entry.get("kind") == KIND_CONFIG:
                config = entry["config"]
        return config

    def latest_config(self, initial_config: dict) -> dict:
        """The newest configuration present in this node's log (or its
        snapshot), whether or not it is committed or applied."""
        result = self.snapshot_config if self.snapshot_config else initial_config
        for entry in self.log:
            if entry.get("kind") == KIND_CONFIG:
                result = entry["config"]
        return result

    def committed_config(self, initial_config: dict) -> dict:
        """The configuration established by the newest committed config
        entry (or the snapshot config, which only covers committed state)."""
        config = self.snapshot_config if self.snapshot_config else initial_config
        upper = min(self.commit_index, self.last_log_index())
        for i in range(self.snapshot_index + 1, upper + 1):
            entry = self.log[i - 1 - self.snapshot_index]
            if entry.get("kind") == KIND_CONFIG:
                config = entry["config"]
        return config


class _Simulator:
    def __init__(self, config: dict) -> None:
        self.node_names: list[str] = config["nodes"]
        self.duration: int = config["duration"]
        self.timeouts: dict[str, int] = config["electionTimeouts"]
        self.heartbeat_interval: int = config["heartbeatInterval"]
        self.delay: int = config["messageDelay"]
        self.faults: list[dict] = config["faults"]
        # Per-message fault rules, keyed by the full selector
        # (from, to, message, occurrence); the per-(from, to, message) send
        # counter below numbers actual sends from the start of the run.
        self.message_faults: dict[tuple, dict] = {
            (rule["from"], rule["to"], rule["message"], rule["occurrence"]): rule
            for rule in config["messageFaults"]
        }
        self.message_fault_counts: dict[tuple, int] = {}
        self.commands: list[dict] = config["clientCommands"]
        self.node_events: list[dict] = config["nodeEvents"]
        self.node_events_provided: bool = config["nodeEventsProvided"]
        self.snapshot_threshold: int | None = config["snapshotThreshold"]
        self.membership_enabled: bool = config["membershipEnabled"]
        self.changes: list[dict] = config["membershipChanges"] or []
        if self.membership_enabled:
            initial_members = frozenset(config["initialMembers"])
            self.initial_config: dict = {
                "type": CONFIG_STABLE,
                "old": initial_members,
                "new": initial_members,
            }
        else:
            all_nodes = frozenset(self.node_names)
            self.initial_config = {"type": CONFIG_STABLE, "old": all_nodes, "new": all_nodes}
        self.state = {name: _Node() for name in self.node_names}
        self.index = {name: i for i, name in enumerate(self.node_names)}
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
        # Membership-change bookkeeping.
        # change_id -> {"node", "action", "member", "result", "index"?, "term"?,
        #               "jointIndex"?, "stableIndex"?, "reason"?}
        self.change_results: dict[str, dict] = {}
        # change_id of the change whose joint entry is committed but whose
        # stable entry has not been appended yet, if any.
        # Learner catch-up state for the in-progress add: once the target
        # learner's matchIndex reaches the pre-joint log end, the leader
        # appends the joint config entry.
        self.pending_catchup: dict | None = None

    def run(self) -> dict:
        for order, fault in enumerate(self.faults):
            heapq.heappush(self.queue, (fault["time"], _KIND_FAULT, order, fault))
        for order, event in enumerate(self.node_events):
            heapq.heappush(self.queue, (event["time"], _KIND_NODE_EVENT, order, event))
        for order, command in enumerate(self.commands):
            heapq.heappush(self.queue, (command["time"], _KIND_CLIENT, order, command))
        for order, change in enumerate(self.changes):
            heapq.heappush(self.queue, (change["time"], _KIND_MEMBERSHIP, order, change))
        for name in self.node_names:
            if self._is_voter(name, self.initial_config):
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

    # -- configuration -------------------------------------------------------

    def _command_entry(self, index: int, term: int, command_id: str, command: object) -> dict:
        """A client-command log/applied entry. When membership changes are
        enabled it carries ``kind: "command"`` so configuration entries
        (``kind: "config"``) are unambiguously distinguishable; without the
        feature the historical, untyped shape is preserved byte for byte."""
        entry = {"index": index, "term": term}
        if self.membership_enabled:
            entry["kind"] = KIND_COMMAND
        entry["id"] = command_id
        entry["command"] = command
        return entry

    @staticmethod
    def _config_groups(config: dict) -> tuple[frozenset[str], frozenset[str]]:
        """The (old, new) voter sets of a configuration. Stable configurations
        carry the same set twice; joint configurations carry both."""
        return frozenset(config["old"]), frozenset(config["new"])

    def _config_quorums(self, config: dict) -> list[frozenset[str]]:
        old, new = self._config_groups(config)
        if config.get("type") == CONFIG_JOINT:
            return [old, new]
        return [new]

    def _is_voter(self, name: str, config: dict) -> bool:
        old, new = self._config_groups(config)
        return name in old or name in new

    def _can_vote(self, name: str) -> bool:
        """Current voting/election eligibility from the node's newest
        configuration: learners and nodes not present in the latest config
        neither vote nor campaign. A removed node that learns a configuration
        adding it back regains eligibility."""
        st = self.state[name]
        return self._is_voter(name, st.latest_config(self.initial_config))

    @staticmethod
    def _store_config(config: dict) -> dict:
        old, new = _Simulator._config_groups(config)
        return {
            "type": config.get("type", CONFIG_STABLE),
            "old": sorted(old),
            "new": sorted(new),
        }

    def _joint_config(self, voters: frozenset[str], action: str, member: str) -> dict:
        if action == "add":
            new_voters = voters | {member}
        else:
            new_voters = voters - {member}
        return {"type": CONFIG_JOINT, "old": sorted(voters), "new": sorted(new_voters)}

    @staticmethod
    def _stable_config(voters: frozenset[str]) -> dict:
        ordered = sorted(voters)
        return {"type": CONFIG_STABLE, "old": ordered, "new": ordered}

    def _has_vote_majorities(self, candidate: str, votes: set[str], config: dict) -> bool:
        for group in self._config_quorums(config):
            needed = len(group) // 2 + 1
            if len([v for v in votes if v in group]) < needed:
                return False
        return True

    def _replicated_by_majorities(self, leader: str, index: int, config: dict) -> bool:
        """Whether ``index`` is present on a strict majority of every quorum
        group (the leader itself counts in any group it belongs to)."""
        st = self.state[leader]
        for group in self._config_quorums(config):
            needed = len(group) // 2 + 1
            points = []
            for peer in group:
                if peer == leader:
                    points.append(st.last_log_index())
                else:
                    points.append(st.match_index.get(peer, 0))
            if len([point for point in points if point >= index]) < needed:
                return False
        return True

    # -- state transitions --------------------------------------------------

    def _invalidate_timeout(self, name: str) -> None:
        self.state[name].timeout_gen += 1

    def _reset_timeout(self, name: str) -> None:
        st = self.state[name]
        if not self._can_vote(name):
            # Learners and removed members hold no election timer; bumping the
            # generation cancels any timer scheduled before their membership
            # changed.
            st.timeout_gen += 1
            return
        st.timeout_gen += 1
        heapq.heappush(
            self.queue,
            (self.now + self.timeouts[name], _KIND_TIMEOUT, self.index[name], (name, st.timeout_gen)),
        )

    def _reconcile_election_timer(self, name: str, eligible_before: bool) -> None:
        """Start or cancel a node's election timer when its configuration
        membership changes (e.g. a learner promoted by a replicated joint
        entry, or a member removed by a replicated stable entry)."""
        eligible_after = self._can_vote(name)
        if eligible_after and not eligible_before:
            self._reset_timeout(name)
        elif eligible_before and not eligible_after:
            self._invalidate_timeout(name)

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
        # The election is decided under the configuration in place just
        # before the candidate appends anything (joint configs require
        # majorities of both constituent sets).
        config = st.latest_config(self.initial_config)
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
            if peer != name and self._is_voter(peer, config):
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

    def _become_leader(self, name: str, config: dict) -> None:
        st = self.state[name]
        st.role = ROLE_LEADER
        st.known_leader = name
        st.votes = set()
        st.timeout_gen += 1  # leaders have no election timeout
        next_index = st.last_log_index() + 1
        # Replication progress is tracked for every other node: voters count
        # towards quorums, learners receive the log but never count.
        st.next_index = {peer: next_index for peer in self.node_names if peer != name}
        st.match_index = {peer: 0 for peer in self.node_names if peer != name}
        self._record_state_change(name, "majority")
        leaders = self.leaders_by_term.setdefault(st.term, [])
        if name not in leaders:
            leaders.append(name)
        self._send_heartbeats(name)
        self._resume_membership_change(name)

    def _resume_membership_change(self, name: str) -> None:
        """Finish a change a previous leader left open. If the newest config
        entry present is a joint entry (with no stable entry following it),
        append the stable entry now: winning this term's election required
        majorities of both joint sets, so the joint entry is known to be on
        those majorities even before commitIndex advanced to it."""
        st = self.state[name]
        if st.latest_config(self.initial_config).get("type") == CONFIG_JOINT:
            joint_entry = None
            # Newest joint entry in the uncompacted log, otherwise the newest
            # one folded into the snapshot's applied history.
            for log_entry in reversed(st.log):
                if log_entry.get("kind") == KIND_CONFIG and log_entry["entryType"] == CONFIG_JOINT:
                    joint_entry = log_entry
                    break
            if joint_entry is None:
                for applied in reversed(st.applied):
                    if (
                        applied.get("kind") == KIND_CONFIG
                        and applied["entryType"] == CONFIG_JOINT
                    ):
                        joint_entry = applied
                        break
            if joint_entry is not None:
                _old, new_voters = self._config_groups(joint_entry["config"])
                stable = self._stable_config(new_voters)
                self._append_config_entry(
                    name,
                    joint_entry["id"],
                    CONFIG_STABLE,
                    stable,
                    joint_entry["action"],
                    joint_entry["member"],
                )
                for peer in self.node_names:
                    if peer != name:
                        self._replicate_to(name, peer)
        if self.pending_catchup is not None:
            if self.pending_catchup["leader"] != name:
                # The old leader was replaced before it appended the joint
                # entry; the unfinished catch-up is abandoned.
                self.pending_catchup = None
            else:
                self._progress_catchup(name)

    # -- messages -----------------------------------------------------------

    def _send(self, src: str, dst: str, msg: dict) -> None:
        self._record({"type": "messageSend", "node": src, "peer": dst, "message": msg["kind"], "term": msg["term"]})
        self.send_counter += 1
        envelope = {**msg, "src": src, "dst": dst}
        # A messageFaults rule matches the occurrence-th actual send of its
        # (from, to, message) selector. The fault is recorded right after the
        # send; a drop replaces delivery with a dropped/messageFault result at
        # the originally scheduled arrival, a delay shifts the arrival itself.
        count_key = (src, dst, msg["kind"])
        occurrence = self.message_fault_counts.get(count_key, 0) + 1
        self.message_fault_counts[count_key] = occurrence
        rule = self.message_faults.get((src, dst, msg["kind"], occurrence))
        extra_delay = 0
        if rule is not None:
            scheduled = self.now + self.delay
            fault_entry = {
                "type": "messageFault",
                "rule": rule["index"],
                "from": src,
                "to": dst,
                "message": msg["kind"],
                "occurrence": occurrence,
                "action": rule["action"],
                "scheduledTime": scheduled,
            }
            if rule["action"] == "drop":
                envelope["faultDrop"] = True
            else:
                extra_delay = rule["delay"]
                fault_entry["arrivalTime"] = scheduled + extra_delay
            self._record(fault_entry)
        effective_delay = self.delay + extra_delay
        # Zero-delay replication triggered while draining a client-command
        # batch shares the batch's timestamp and must follow the remaining
        # client commands; a positive delay lands strictly in the future and
        # queues as a normal message arrival.
        in_phase = self.reaction_phase and effective_delay == 0
        kind = _KIND_REACTION if in_phase else _KIND_MESSAGE
        heapq.heappush(self.queue, (self.now + effective_delay, kind, self.send_counter, envelope))

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
        if msg.get("faultDrop"):
            # A messageFaults drop rule: the message never reaches the
            # receiver, reported at its originally scheduled arrival time.
            self._record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": msg["kind"],
                "term": msg["term"],
                "result": "dropped",
                "reason": "messageFault",
            })
            return
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
        # Learners and removed members never grant a vote; candidates only
        # solicit voters, so stale RVs reaching a demoted node are denied.
        eligible_voter = self._can_vote(dst)
        granted = (
            eligible_voter
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
        election_config = st.latest_config(self.initial_config)
        if msg["term"] > st.term:
            detail = "higherTerm"
        elif st.role == ROLE_CANDIDATE and msg["term"] == st.term and msg["granted"]:
            st.votes.add(src)
            detail = "voteCounted"
            elected = self._has_vote_majorities(dst, st.votes, election_config)
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
            self._become_leader(dst, election_config)

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

        # Latest configuration decides voting/campaigning as soon as an entry
        # is appended, so start or stop the election timer when the batch
        # changes this node's membership. Compute eligibility before any
        # truncation or append.
        eligible_before = self._can_vote(dst)

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
        for offset, entry in enumerate(msg["entries"]):
            index = prev_index + 1 + offset
            if index > st.last_log_index():
                if entry.get("kind") == KIND_CONFIG:
                    stored = {
                        "index": index,
                        "term": entry["term"],
                        "kind": KIND_CONFIG,
                        "id": entry["id"],
                        "entryType": entry["entryType"],
                        "config": entry["config"],
                        "action": entry["action"],
                        "member": entry["member"],
                    }
                else:
                    stored = self._command_entry(
                        index, entry["term"], entry["id"], entry["command"]
                    )
                st.log.append(stored)
                appended += 1

        self._reconcile_election_timer(dst, eligible_before)

        config_entries = [
            {
                "index": entry["index"],
                "id": entry["id"],
                "entryType": entry["entryType"],
            }
            for entry in msg["entries"]
            if entry.get("kind") == KIND_CONFIG
        ]
        result_entry = {
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
        if config_entries:
            # Configuration replication is reported separately from ordinary
            # client-command replication.
            result_entry["configEntries"] = config_entries
        self._record(result_entry)

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
        eligible_before = self._can_vote(dst)
        keep = (
            st.has_entry(included_index)
            and st.term_at(included_index) == included_term
        )
        snapshot_entries = [dict(entry) for entry in msg["entries"]]
        # Recover the configuration in force at the snapshot position from
        # the applied entries bundled into the snapshot.
        snap_config = None
        for bundled in snapshot_entries:
            if bundled.get("kind") == KIND_CONFIG:
                snap_config = bundled["config"]
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
        st.snapshot_config = snap_config
        if st.commit_index < included_index:
            st.commit_index = included_index
        st.last_applied = len(st.applied)

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
        # Configuration and membership arrive as already-restored state, just
        # like commands: no configurationApplied events are emitted for them.
        self._restore_membership_from_snapshot(dst, snapshot_entries, snap_config, eligible_before)
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

    def _restore_membership_from_snapshot(
        self,
        name: str,
        bundled_entries: list[dict],
        snap_config: dict | None,
        eligible_before: bool,
    ) -> None:
        """Restore voting membership and change outcomes from the applied
        entries folded into a received snapshot without re-applying them."""
        if snap_config is not None:
            eligible_after = self._is_voter(name, snap_config)
            if eligible_after and not eligible_before:
                self._reset_timeout(name)
            elif eligible_before and not eligible_after:
                self._invalidate_timeout(name)
        for bundled in bundled_entries:
            if bundled.get("kind") != KIND_CONFIG:
                continue
            change_id = bundled["id"]
            result = self.change_results.get(change_id)
            if result is None:
                continue
            if bundled["entryType"] == CONFIG_STABLE:
                if result.get("outcome") != "committed":
                    result["outcome"] = "committed"
                    result["index"] = bundled["index"]
                    result["term"] = bundled["term"]
                result.pop("jointPhase", None)
            else:
                old, new = self._config_groups(bundled["config"])
                result["jointPhase"] = {"id": change_id, "old": sorted(old), "new": sorted(new)}

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
            self._progress_catchup(dst)
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
            # A follower with a longer divergent suffix can acknowledge a
            # batch whose shared positions already match; the leader must not
            # treat positions beyond its own log end as replicated.
            match = min(msg["matchIndex"], st.last_log_index())
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
            self._progress_catchup(dst)
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
        st.log.append(self._command_entry(index, st.term, command_id, command["command"]))
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

    # -- membership changes --------------------------------------------------

    def _change_in_progress(self, st: _Node, leader: str) -> bool:
        pending = self.pending_catchup
        if pending is not None and pending["leader"] == leader and st.role == ROLE_LEADER:
            return True
        # The change runs until its stable entry commits: a committed joint
        # config, or any not-yet-committed config entry (the appended stable
        # entry), both keep the cluster busy.
        if st.committed_config(self.initial_config).get("type") == CONFIG_JOINT:
            return True
        return any(
            entry.get("kind") == KIND_CONFIG and entry["index"] > st.commit_index
            for entry in st.log
        )

    def _record_membership_rejected(self, change: dict, reason: str) -> None:
        change_id = change["id"]
        self.change_results[change_id] = {
            "node": change["node"],
            "action": change["action"],
            "member": change["member"],
            "outcome": "rejected",
            "reason": reason,
        }
        self._record({
            "type": "membershipResult",
            "node": change["node"],
            "id": change_id,
            "action": change["action"],
            "member": change["member"],
            "result": "rejected",
            "reason": reason,
        })

    def _on_membership_change(self, change: dict) -> None:
        node = change["node"]
        change_id = change["id"]
        action = change["action"]
        member = change["member"]
        st = self.state[node]
        # Rejection precedence: receiver down -> not the leader -> another
        # change already running -> member state not applicable -> removing
        # would leave fewer than three voters.
        if not st.online:
            self._record_membership_rejected(change, "nodeDown")
            return
        if st.role != ROLE_LEADER:
            self._record_membership_rejected(change, "notLeader")
            return
        if self._change_in_progress(st, node):
            self._record_membership_rejected(change, "changeInProgress")
            return
        _, voters = self._config_groups(st.committed_config(self.initial_config))
        voters = frozenset(voters)
        if action == "add":
            if member in voters:
                self._record_membership_rejected(change, "alreadyMember")
                return
            new_voters = voters | {member}
        else:
            if member not in voters:
                self._record_membership_rejected(change, "notMember")
                return
            new_voters = voters - {member}
        if len(new_voters) < 3:
            self._record_membership_rejected(change, "minimumClusterSize")
            return

        self.change_results[change_id] = {
            "node": node,
            "action": action,
            "member": member,
            "outcome": "pending",
        }
        if action == "add":
            # The learner must first receive the log (or a snapshot) until it
            # matches the leader; only then is the joint config entry
            # appended. Heartbeats already stream entries to every node, but
            # replicate immediately so catch-up starts at once.
            self.pending_catchup = {
                "id": change_id,
                "member": member,
                "action": action,
                "leader": node,
            }
            self._record({
                "type": "membershipResult",
                "node": node,
                "id": change_id,
                "action": action,
                "member": member,
                "result": "accepted",
                "phase": "catchingUp",
            })
            self._replicate_to(node, member)
            self._progress_catchup(node)
        else:
            self._record({
                "type": "membershipResult",
                "node": node,
                "id": change_id,
                "action": action,
                "member": member,
                "result": "accepted",
            })
            self._append_joint_entry(node, change_id, action, member, voters)

    def _append_config_entry(
        self, name: str, change_id: str, entry_type: str, config: dict, action: str, member: str
    ) -> dict:
        st = self.state[name]
        entry = {
            "index": st.last_log_index() + 1,
            "term": st.term,
            "kind": KIND_CONFIG,
            "id": change_id,
            "entryType": entry_type,
            "config": self._store_config(config),
            "action": action,
            "member": member,
        }
        st.log.append(entry)
        return entry

    def _append_joint_entry(
        self, name: str, change_id: str, action: str, member: str, voters: frozenset[str]
    ) -> None:
        joint = self._joint_config(voters, action, member)
        entry = self._append_config_entry(name, change_id, CONFIG_JOINT, joint, action, member)
        for peer in self.node_names:
            if peer != name:
                self._replicate_to(name, peer)

    def _progress_catchup(self, name: str) -> None:
        """Append the joint entry once the added learner has caught the
        leader's log end (via log replication or an installed snapshot)."""
        pending = self.pending_catchup
        if pending is None:
            return
        st = self.state[name]
        if st.role != ROLE_LEADER or pending["leader"] != name:
            return
        member = pending["member"]
        latest = st.latest_config(self.initial_config)
        if latest.get("type") == CONFIG_JOINT:
            # Another change already moved the cluster into a joint phase.
            return
        _, voters = self._config_groups(st.committed_config(self.initial_config))
        voters = frozenset(voters)
        if (pending["action"] == "add") == (member in voters):
            # The requested relationship no longer matches committed state.
            return
        if st.match_index.get(member, 0) < st.last_log_index():
            return
        self.pending_catchup = None
        self._append_joint_entry(name, pending["id"], pending["action"], member, voters)

    # -- commit and apply ---------------------------------------------------

    def _advance_leader_commit(self, name: str) -> None:
        st = self.state[name]
        replicated = sorted([st.last_log_index()] + list(st.match_index.values()), reverse=True)
        # Entries replicated while a joint configuration is active need
        # strict majorities of BOTH voter sets; entries before/after it use
        # the single stable set.
        for candidate in replicated:
            if candidate <= st.commit_index:
                break
            if st.term_at(candidate) != st.term:
                continue
            # A server uses the newest config in its log as soon as it is
            # appended, so the joint entry is committed by majorities of
            # BOTH sets; the stable entry is only appended after the joint
            # entry commits, so it and later entries use the new single set.
            config = st.config_at(candidate + 1, self.initial_config)
            if self._replicated_by_majorities(name, candidate, config):
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
            if entry.get("kind") == KIND_CONFIG:
                st.applied.append(self._applied_config_view(entry))
                self._apply_config_entry(name, entry)
            else:
                st.applied.append(
                    self._command_entry(
                        entry["index"], entry["term"], entry["id"], entry["command"]
                    )
                )
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

    @staticmethod
    def _applied_config_view(entry: dict) -> dict:
        return {
            "index": entry["index"],
            "term": entry["term"],
            "id": entry["id"],
            "kind": KIND_CONFIG,
            "entryType": entry["entryType"],
            "config": entry["config"],
            "action": entry["action"],
            "member": entry["member"],
        }

    @staticmethod
    def _has_following_stable(st: _Node, joint_entry: dict) -> bool:
        """Whether a stable entry for the same change already follows the
        joint entry in this node's log (e.g. appended when this leader first
        won the election)."""
        for log_entry in st.log:
            if (
                log_entry["index"] > joint_entry["index"]
                and log_entry.get("kind") == KIND_CONFIG
                and log_entry["entryType"] == CONFIG_STABLE
                and log_entry["id"] == joint_entry["id"]
            ):
                return True
        return False

    def _apply_config_entry(self, name: str, entry: dict) -> None:
        """React to a newly committed configuration: emit configurationApplied,
        start/stop election timers when membership changes, append the stable
        entry after a joint entry commits (on the leader), and make a removed
        leader step down once the stable configuration is committed."""
        st = self.state[name]
        config = entry["config"]
        old, new = self._config_groups(config)
        self._record({
            "type": "configurationApplied",
            "node": name,
            "index": entry["index"],
            "term": entry["term"],
            "id": entry["id"],
            "entryType": entry["entryType"],
            "config": config,
            "action": entry["action"],
            "member": entry["member"],
        })
        if entry["entryType"] == CONFIG_STABLE:
            # A stable entry keeps the new set in both halves, so the removed
            # members come from the preceding joint configuration (old set)
            # rather than from this entry itself.
            prior_old, _prior_new = self._config_groups(
                st.config_at(entry["index"], self.initial_config)
            )
            removed = prior_old - new
            result = self.change_results.get(entry["id"])
            if result is not None and result.get("outcome") != "committed":
                result["outcome"] = "committed"
                result["index"] = entry["index"]
                result["term"] = entry["term"]
                result.pop("jointPhase", None)
            # A leader removed by the change it proposed steps down at once;
            # its latest configuration no longer contains it, so it neither
            # campaigns nor votes afterwards.
            if name in removed and st.role == ROLE_LEADER:
                self._leader_removed(name, entry)
        else:
            result = self.change_results.get(entry["id"])
            if result is not None:
                result["jointPhase"] = {"id": entry["id"], "old": sorted(old), "new": sorted(new)}
            if st.role == ROLE_LEADER and not self._has_following_stable(st, entry):
                # The joint configuration is committed (or, for a leader that
                # won after the joint entry was already on both majorities, is
                # being caught up): append the stable configuration. A new
                # leader may already have appended one in _become_leader, in
                # which case it must not be duplicated.
                stable = self._stable_config(new)
                self._append_config_entry(
                    name, entry["id"], CONFIG_STABLE, stable, entry["action"], entry["member"]
                )
                for peer in self.node_names:
                    if peer != name:
                        self._replicate_to(name, peer)

    def _leader_removed(self, name: str, entry: dict) -> None:
        st = self.state[name]
        st.role = ROLE_FOLLOWER
        st.known_leader = None
        st.votes = set()
        st.next_index = {}
        st.match_index = {}
        # _reset_timeout cancels the timer for a now non-voting node.
        self._reset_timeout(name)
        self._record_state_change(name, "removedFromCluster")

    def _create_snapshot(self, name: str) -> None:
        """Fold every applied entry through last_applied into the snapshot and
        delete the compacted prefix. Global indices are preserved: the
        surviving log suffix keeps its indices and commitIndex/lastApplied are
        not renumbered. The configuration in force at the snapshot index is
        folded in as well, so membership survives compaction."""
        st = self.state[name]
        target = st.last_applied
        cut = target - st.snapshot_index
        included_term = st.log[cut - 1]["term"]
        config = st.snapshot_config if st.snapshot_config else self.initial_config
        for offset in range(cut):
            entry = st.log[offset]
            if entry.get("kind") == KIND_CONFIG:
                config = entry["config"]
        st.snapshot_index = target
        st.snapshot_term = included_term
        st.snapshot_config = self._store_config(config)
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
        if not self._can_vote(name):
            # A learner's or removed member's stray timer must never start an
            # election.
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
        """The (term, id, ...) a node holds for a global log index, looking
        through snapshot boundaries: applied entries cover the compacted
        prefix and the uncompacted log covers everything after it. Both
        client-command and configuration entries are covered."""
        if index <= 0 or index > st.last_log_index():
            return None
        if index <= st.last_applied:
            return st.applied[index - 1]
        if st.has_entry(index):
            return st.log[index - 1 - st.snapshot_index]
        return None

    def _log_matching_violations(self) -> list[dict]:
        """Same index and term, but different content (id/command for client
        commands, or the configuration payload) across nodes, including
        indices folded into snapshots or occupied by configuration entries."""
        violations = []
        max_len = max((st.last_log_index() for st in self.state.values()), default=0)
        for index in range(1, max_len + 1):
            by_term: dict[int, dict[tuple, dict]] = {}
            for name in self.node_names:
                st = self.state[name]
                entry = self._entry_view(st, index)
                if entry is None:
                    continue
                if entry.get("kind") == KIND_CONFIG:
                    marker = json.dumps(
                        [
                            entry["id"],
                            KIND_CONFIG,
                            entry["entryType"],
                            entry.get("config"),
                            entry.get("action"),
                            entry.get("member"),
                        ],
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    bucket_view = {
                        "id": entry["id"],
                        "kind": KIND_CONFIG,
                        "entryType": entry["entryType"],
                        "config": entry.get("config"),
                        "nodes": [],
                    }
                else:
                    marker = json.dumps([entry["id"], entry["command"]], ensure_ascii=False, sort_keys=True)
                    bucket_view = {"id": entry["id"], "command": entry["command"], "nodes": []}
                bucket = by_term.setdefault(entry["term"], {}).setdefault(marker, bucket_view)
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
        Configuration entries participate in index alignment alongside client
        commands, and applied histories recovered from snapshots are covered."""
        violations = []
        max_applied = max((st.last_applied for st in self.state.values()), default=0)
        for index in range(1, max_applied + 1):
            variants: dict[str, dict] = {}
            for name in self.node_names:
                st = self.state[name]
                if index > st.last_applied:
                    continue
                entry = st.applied[index - 1]
                if entry.get("kind") == KIND_CONFIG:
                    marker = json.dumps(
                        [
                            entry["term"],
                            entry["id"],
                            KIND_CONFIG,
                            entry["entryType"],
                            entry.get("config"),
                        ],
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    bucket = variants.setdefault(
                        marker,
                        {
                            "term": entry["term"],
                            "id": entry["id"],
                            "kind": KIND_CONFIG,
                            "entryType": entry["entryType"],
                            "config": entry.get("config"),
                            "nodes": [],
                        },
                    )
                else:
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
            # Voter/learner according to the node's newest configuration,
            # which governs voting and campaigning even before it commits.
            report["membershipRole"] = (
                "voter" if self._is_voter(name, st.latest_config(self.initial_config)) else "learner"
            )
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

    def _membership_report(self) -> dict:
        # The reference node deterministically has the greatest committed
        # position, then the greatest log length (declared node order breaks
        # ties): committed truth dominates, so a deposed leader's stale
        # uncommitted suffix cannot describe a phase the cluster left.
        ref_name = max(
            self.node_names,
            key=lambda name: (
                self.state[name].commit_index,
                self.state[name].last_log_index(),
                -self.index[name],
            ),
        )
        ref = self.state[ref_name]
        committed = ref.committed_config(self.initial_config)
        latest = ref.latest_config(self.initial_config)

        if latest.get("type") == CONFIG_JOINT:
            old, new = self._config_groups(latest)
            current_stable = sorted(old)
            joint = {"id": self._latest_joint_id(ref),
                     "old": sorted(old), "new": sorted(new)}
        elif committed.get("type") == CONFIG_JOINT:
            old, new = self._config_groups(committed)
            current_stable = sorted(old)
            joint = {"id": self._latest_joint_id(ref),
                     "old": sorted(old), "new": sorted(new)}
        else:
            _, stable_voters = self._config_groups(committed)
            current_stable = sorted(stable_voters)
            joint = None

        joint_ids = set()
        for st in self.state.values():
            for applied in st.applied:
                if applied.get("kind") == KIND_CONFIG and applied["entryType"] == CONFIG_JOINT:
                    joint_ids.add(applied["id"])
            for log_entry in st.log:
                if log_entry.get("kind") == KIND_CONFIG and log_entry["entryType"] == CONFIG_JOINT:
                    joint_ids.add(log_entry["id"])

        changes = []
        for change in self.changes:
            change_id = change["id"]
            result = self.change_results.get(change_id)
            entry = {
                "id": change_id,
                "node": change["node"],
                "action": change["action"],
                "member": change["member"],
            }
            if result is None or result.get("outcome") == "pending":
                entry["outcome"] = "pending"
                if self.pending_catchup is not None and self.pending_catchup["id"] == change_id:
                    entry["phase"] = "catchingUp"
                elif change_id in joint_ids:
                    phase_info = result.get("jointPhase") if result else None
                    entry["phase"] = "joint"
                    if phase_info is not None:
                        entry["joint"] = {
                            "old": sorted(phase_info["old"]),
                            "new": sorted(phase_info["new"]),
                        }
                else:
                    entry["phase"] = "catchingUp"
            elif result.get("outcome") == "rejected":
                entry["outcome"] = "rejected"
                entry["reason"] = result["reason"]
            else:
                entry["outcome"] = "committed"
                entry["index"] = result["index"]
                entry["term"] = result["term"]
            changes.append(entry)

        return {
            "initial": sorted(self.initial_config["new"]),
            "current": current_stable,
            "joint": joint,
            "changes": changes,
        }

    def _latest_config_entry_id(self, st: _Node, entry_type: str) -> str | None:
        """The id of the newest committed configuration entry of a type,
        looking through the snapshot into the applied history."""
        found = None
        for entry in st.applied:
            if entry.get("kind") == KIND_CONFIG and entry["entryType"] == entry_type:
                found = entry["id"]
        upper = min(st.commit_index, st.last_log_index())
        for i in range(st.snapshot_index + 1, upper + 1):
            entry = st.log[i - 1 - st.snapshot_index]
            if entry.get("kind") == KIND_CONFIG and entry["entryType"] == entry_type:
                found = entry["id"]
        return found

    def _latest_joint_id(self, st: _Node) -> str | None:
        """The id of the newest joint configuration entry the reference node
        holds, including one appended but not yet committed."""
        found = self._latest_config_entry_id(st, CONFIG_JOINT)
        for entry in st.log:
            if entry.get("kind") == KIND_CONFIG and entry["entryType"] == CONFIG_JOINT:
                found = entry["id"]
        return found

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
