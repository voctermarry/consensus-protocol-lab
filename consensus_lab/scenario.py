"""Scenario validation and normalization.

A decoded JSON object goes in; either a :class:`ScenarioError` describes the
first violated constraint or a normalized config dict comes out. All
structural and cross-reference checks run here, before any event is
scheduled, so the simulation core only ever sees a legal scenario.
"""

from __future__ import annotations

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
    "readQueries",
    "livenessChecks",
    "preVote",
}
_REQUIRED_FIELDS = _TOP_LEVEL_FIELDS - {
    "faults",
    "clientCommands",
    "nodeEvents",
    "snapshotThreshold",
    "initialMembers",
    "membershipChanges",
    "messageFaults",
    "readQueries",
    "livenessChecks",
    "preVote",
}
_FAULT_FIELDS = {"time", "action", "groups"}
_MESSAGE_FAULT_FIELDS = {"from", "to", "message", "occurrence", "action", "delay"}
_MESSAGE_FAULT_REQUIRED = {"from", "to", "message", "occurrence", "action"}
_MESSAGE_KINDS = (
    "requestVote",
    "voteReply",
    "preVote",
    "preVoteReply",
    "heartbeat",
    "appendEntries",
    "appendReply",
    "installSnapshot",
    "installSnapshotReply",
    "readProbe",
    "readReply",
)
# Pre-vote traffic exists only while the optional phase is enabled; a rule
# referencing either kind in a preVote-less scenario makes the scenario illegal.
_PRE_VOTE_MESSAGE_KINDS = ("preVote", "preVoteReply")
_CLIENT_COMMAND_FIELDS = {"time", "node", "id", "command"}
_NODE_EVENT_FIELDS = {"time", "node", "action"}
_MEMBERSHIP_CHANGE_FIELDS = {"time", "node", "id", "action", "member"}
_READ_QUERY_FIELDS = {"time", "node", "id"}
_LIVENESS_CHECK_FIELDS = {"id", "type", "startTime", "deadline", "target"}
_LIVENESS_TYPES = (
    "leaderElected",
    "clientCommitted",
    "readCompleted",
    "membershipCommitted",
)
_LIVENESS_TARGET_TYPES = {
    "clientCommitted": "clientCommands",
    "readCompleted": "readQueries",
    "membershipCommitted": "membershipChanges",
}

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

    # The optional pre-vote phase is opt-in: omitted or false keeps every
    # legacy scenario's behavior (and output) byte for byte, and the
    # pre-vote message kinds become illegal.
    pre_vote = raw.get("preVote", False)
    if not isinstance(pre_vote, bool):
        raise ScenarioError("preVote must be a boolean")

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
        if kind in _PRE_VOTE_MESSAGE_KINDS and not pre_vote:
            raise ScenarioError(
                f"{label}.message {kind!r} is only valid when preVote is enabled"
            )
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
    seen_change_ids: set[str] = set()
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

    read_queries_provided = "readQueries" in raw
    read_queries = raw.get("readQueries", [])
    if not isinstance(read_queries, list):
        raise ScenarioError("readQueries must be a list")
    normalized_read_queries = []
    seen_read_ids: set[str] = set()
    for index, query in enumerate(read_queries):
        label = f"readQueries[{index}]"
        if not isinstance(query, dict):
            raise ScenarioError(f"{label} must be an object")
        unknown = sorted(set(query) - _READ_QUERY_FIELDS)
        if unknown:
            raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
        missing = sorted(_READ_QUERY_FIELDS - set(query))
        if missing:
            raise ScenarioError(f"{label} missing field(s): {', '.join(missing)}")
        time = _require_int(query["time"], f"{label}.time", 0)
        if time > duration:
            raise ScenarioError(f"{label}.time is beyond the simulation duration")
        node = query["node"]
        if not isinstance(node, str) or not node:
            raise ScenarioError(f"{label}.node must be a non-empty string")
        if node not in node_set:
            raise ScenarioError(f"{label}.node references unknown node: {node!r}")
        query_id = query["id"]
        if not isinstance(query_id, str) or not query_id:
            raise ScenarioError(f"{label}.id must be a non-empty string")
        if query_id in seen_read_ids:
            raise ScenarioError(f"{label}.id duplicates a previous id: {query_id!r}")
        if query_id in seen_ids:
            raise ScenarioError(
                f"{label}.id duplicates a clientCommands id: {query_id!r}"
            )
        if query_id in seen_change_ids:
            raise ScenarioError(
                f"{label}.id duplicates a membershipChanges id: {query_id!r}"
            )
        seen_read_ids.add(query_id)
        normalized_read_queries.append({"time": time, "node": node, "id": query_id})

    liveness_provided = "livenessChecks" in raw
    liveness_checks = raw.get("livenessChecks", [])
    if not isinstance(liveness_checks, list):
        raise ScenarioError("livenessChecks must be a list")
    normalized_liveness_checks = []
    seen_check_ids: set[str] = set()
    for index, check in enumerate(liveness_checks):
        label = f"livenessChecks[{index}]"
        if not isinstance(check, dict):
            raise ScenarioError(f"{label} must be an object")
        unknown = sorted(set(check) - _LIVENESS_CHECK_FIELDS)
        if unknown:
            raise ScenarioError(f"{label} has unknown field(s): {', '.join(unknown)}")
        missing = sorted(_LIVENESS_CHECK_FIELDS - {"target"} - set(check))
        if missing:
            raise ScenarioError(f"{label} missing field(s): {', '.join(missing)}")
        check_id = check["id"]
        if not isinstance(check_id, str) or not check_id:
            raise ScenarioError(f"{label}.id must be a non-empty string")
        if check_id in seen_check_ids:
            raise ScenarioError(f"{label}.id duplicates a previous id: {check_id!r}")
        seen_check_ids.add(check_id)
        check_type = check["type"]
        if check_type not in _LIVENESS_TYPES:
            raise ScenarioError(
                f"{label}.type must be one of: {', '.join(_LIVENESS_TYPES)}"
            )
        start_time = _require_int(check["startTime"], f"{label}.startTime", 0)
        if start_time > duration:
            raise ScenarioError(f"{label}.startTime is beyond the simulation duration")
        deadline = _require_int(check["deadline"], f"{label}.deadline", 0)
        if deadline > duration:
            raise ScenarioError(f"{label}.deadline is beyond the simulation duration")
        if start_time > deadline:
            raise ScenarioError(f"{label}.startTime must not be greater than deadline")
        if check_type == "leaderElected":
            if "target" in check:
                raise ScenarioError(f"{label} leaderElected must not have a target")
            target = None
        else:
            if "target" not in check:
                raise ScenarioError(f"{label} {check_type} requires a target")
            target = check["target"]
            if not isinstance(target, str) or not target:
                raise ScenarioError(f"{label}.target must be a non-empty string")
            container = _LIVENESS_TARGET_TYPES[check_type]
            if container == "clientCommands" and target not in seen_ids:
                raise ScenarioError(
                    f"{label}.target references an unknown clientCommands id: {target!r}"
                )
            if container == "readQueries" and target not in seen_read_ids:
                raise ScenarioError(
                    f"{label}.target references an unknown readQueries id: {target!r}"
                )
            if container == "membershipChanges" and target not in seen_change_ids:
                raise ScenarioError(
                    f"{label}.target references an unknown membershipChanges id: {target!r}"
                )
        normalized_liveness_checks.append(
            {
                "id": check_id,
                "type": check_type,
                "startTime": start_time,
                "deadline": deadline,
                "target": target,
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
        "readQueries": normalized_read_queries,
        "readQueriesProvided": read_queries_provided,
        "livenessChecks": normalized_liveness_checks,
        "livenessChecksProvided": liveness_provided,
        "preVote": pre_vote,
    }
