"""Deterministic Raft leader-election and log-replication simulation.

The simulation advances virtual time only: it never reads the wall clock and
never uses randomness, so identical input produces byte-identical output.

The core execution path is split by responsibility: scenario normalization
(:mod:`.scenario`), the deterministic event queue (:mod:`.events`), network
delivery and fault determination (:mod:`.network`), node state transitions
(:mod:`.protocol`, itself decomposed into per-domain mixins) and final
report generation (:mod:`.report`). The
``simulate``, ``explore`` and ``replay`` entry points all run through the
single :class:`_Simulator` core below.
"""

from __future__ import annotations

from .events import (
    _KIND_CLIENT,
    _KIND_FAULT,
    _KIND_HEARTBEAT,
    _KIND_LIVENESS,
    _KIND_MEMBERSHIP,
    _KIND_MESSAGE,
    _KIND_NODE_EVENT,
    _KIND_REACTION,
    _KIND_READ,
    _KIND_TIMEOUT,
    EventQueue,
)
from .network import Network
from .node import (
    CONFIG_JOINT,
    CONFIG_STABLE,
    KIND_COMMAND,
    KIND_CONFIG,
    ROLE_CANDIDATE,
    ROLE_FOLLOWER,
    ROLE_LEADER,
    ROLE_PRECANDIDATE,
    _Node,
)
from .protocol import _Protocol
from .report import build_report
from .scenario import ScenarioError, _is_int, _require_int, parse_scenario


class _Simulator(_Protocol):
    """The single core execution path: wires the event queue, the network
    layer and the protocol transitions together and advances virtual time."""

    def __init__(self, config: dict) -> None:
        self.node_names: list[str] = config["nodes"]
        self.duration: int = config["duration"]
        self.timeouts: dict[str, int] = config["electionTimeouts"]
        self.heartbeat_interval: int = config["heartbeatInterval"]
        self.faults: list[dict] = config["faults"]
        self.commands: list[dict] = config["clientCommands"]
        self.node_events: list[dict] = config["nodeEvents"]
        self.node_events_provided: bool = config["nodeEventsProvided"]
        self.pre_vote_enabled: bool = config["preVote"]
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
        # The deterministic event queue and the network layer (delay model,
        # partitions, per-message faults) form the transport; the clock and
        # drain phase stay on the simulator and are read through callbacks.
        self.queue = EventQueue()
        self.now = 0
        # While draining client commands (and the replication cascade they
        # trigger), new messages land in the REACTION phase instead of the
        # regular MESSAGE phase.
        self.reaction_phase = False
        self.network = Network(
            config["messageDelay"],
            config["messageFaults"],
            self.queue,
            self._record,
            lambda: self.now,
            lambda: self.reaction_phase,
        )
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
        # Read-only query bookkeeping.
        self.read_queries: list[dict] = config["readQueries"]
        self.read_queries_provided: bool = config["readQueriesProvided"]
        # query id -> {"node", "term", "readIndex", "config", "acks",
        #              "required"} for accepted queries still awaiting a
        # same-term majority confirmation under the recorded configuration.
        self.pending_reads: dict[str, dict] = {}
        # query id -> final outcome for queries resolved during the run.
        self.read_results: dict[str, dict] = {}
        # query id -> client-command ids already committed when the query was
        # accepted; a completed read omitting any of them is a stale read.
        self.read_required: dict[str, set[str]] = {}
        # Client-command ids committed so far (leader-side commit advances).
        self.committed_command_ids: set[str] = set()
        # Client-command ids accepted (appended to a leader's log) at least
        # once; together with the rejected map and current log presence this
        # distinguishes "not submitted yet / pending" from "superseded".
        self.accepted_command_ids: set[str] = set()
        # Optional liveness checks. Each one is evaluated at every virtual
        # timestamp in its [startTime, deadline] window, but only after all
        # pre-existing events sharing that timestamp (and their zero-delay
        # reaction cascades) have drained; checks at the same timestamp run in
        # input order.
        self.liveness_checks: list[dict] = config["livenessChecks"]
        self.liveness_provided: bool = config["livenessChecksProvided"]
        # check id -> finished result entry (status satisfied/failed).
        self.liveness_results: dict[str, dict] = {}
        # Whether an online leader exists at the current evaluation point.
        self.leader_online = False
        # change id -> virtual time its stable configuration entry was first
        # applied on any node (the moment the change is committed).
        self.stable_commit_times: dict[str, int] = {}

    def run(self) -> dict:
        for order, fault in enumerate(self.faults):
            self.queue.push((fault["time"], _KIND_FAULT, order, fault))
        for order, event in enumerate(self.node_events):
            self.queue.push((event["time"], _KIND_NODE_EVENT, order, event))
        for order, command in enumerate(self.commands):
            self.queue.push((command["time"], _KIND_CLIENT, order, command))
        for order, change in enumerate(self.changes):
            self.queue.push((change["time"], _KIND_MEMBERSHIP, order, change))
        for order, query in enumerate(self.read_queries):
            self.queue.push((query["time"], _KIND_READ, order, query))
        for name in self.node_names:
            if self._is_voter(name, self.initial_config):
                self.queue.push((self.timeouts[name], _KIND_TIMEOUT, self.index[name], (name, 0)))
        if self.liveness_provided:
            # Anchors at every window edge guarantee evaluation also runs at
            # timestamps carrying no pre-existing events; all other timestamps
            # are reached via the drain hook in the loop below.
            anchors = sorted({
                edge
                for check in self.liveness_checks
                for edge in (check["startTime"], check["deadline"])
            })
            for order, edge in enumerate(anchors):
                self.queue.push((edge, _KIND_LIVENESS, order, None))
        while self.queue:
            time, kind, _order, payload = self.queue.pop()
            if time > self.duration:
                break
            self.now = time
            self.reaction_phase = kind in (_KIND_CLIENT, _KIND_MEMBERSHIP, _KIND_READ, _KIND_REACTION)
            if kind == _KIND_FAULT:
                self.network.apply_fault(payload)
            elif kind == _KIND_NODE_EVENT:
                self._on_node_event(payload)
            elif kind in (_KIND_MESSAGE, _KIND_REACTION):
                self._deliver(payload)
            elif kind == _KIND_CLIENT:
                self._on_client_command(payload)
            elif kind == _KIND_MEMBERSHIP:
                self._on_membership_change(payload)
            elif kind == _KIND_READ:
                self._on_read_query(payload)
            elif kind == _KIND_TIMEOUT:
                self._on_timeout(*payload)
            elif kind == _KIND_HEARTBEAT:
                self._on_heartbeat(*payload)
            # The liveness anchor itself is a no-op: it only marks a window
            # edge that carries no other events. Either way, once no entry
            # remains for the current timestamp (its zero-delay reactions
            # included), the timestamp has fully drained and the checks run.
            if (
                self.liveness_provided
                and self.liveness_checks
                and (not self.queue or self.queue.next_time > self.now)
            ):
                self._evaluate_liveness(self.now)
        return build_report(self)


def run_simulation(raw: object) -> dict:
    """Validate a decoded JSON scenario and run the deterministic simulation."""
    return _Simulator(parse_scenario(raw)).run()
