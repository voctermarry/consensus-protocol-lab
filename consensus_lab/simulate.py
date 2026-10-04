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

import copy

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
        # While a list is installed here, timeline entries are recorded into
        # the buffer instead of ``timeline`` (a speculative storage-fault
        # barrier); see _execute_with_barrier.
        self._record_buffer: list[dict] | None = None
        # Optional storage faults, keyed by (node, barrier occurrence) and
        # valued by the rule's restartDelay (None = stay offline). A
        # persistence barrier is one event handler that changes at least one
        # persisted field of its node; barriers are numbered per node from 1
        # across the whole run, continuously across automatic restarts.
        self.storage_fault_rules: dict[tuple[str, int], int | None] = {
            (rule["node"], rule["occurrence"]): rule["restartDelay"]
            for rule in config["storageFaults"]
        }
        # Whether any rule exists; when False the barrier machinery is fully
        # bypassed and legacy scenarios run (and report) exactly as before.
        self.storage_faults_active = bool(self.storage_fault_rules)
        # node -> number of persistence barriers committed so far.
        self.storage_counts: dict[str, int] = {}
        # Scheduling order for injected restart events (explicit node events
        # occupy 0..len-1; the two never coexist in one scenario).
        self._restart_order = len(self.node_events)
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
                self._deliver_guarded(payload)
            elif kind == _KIND_CLIENT:
                self._execute_with_barrier(payload["node"], self._on_client_command, payload)
            elif kind == _KIND_MEMBERSHIP:
                self._execute_with_barrier(payload["node"], self._on_membership_change, payload)
            elif kind == _KIND_READ:
                self._execute_with_barrier(payload["node"], self._on_read_query, payload)
            elif kind == _KIND_TIMEOUT:
                self._execute_with_barrier(payload[0], self._on_timeout, *payload)
            elif kind == _KIND_HEARTBEAT:
                self._execute_with_barrier(payload[0], self._on_heartbeat, *payload)
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

    # -- storage-fault barriers ---------------------------------------------
    #
    # Every event handler that targets one node is a candidate persistence
    # barrier: the persisted fields (term, votedFor, log, snapshot,
    # commitIndex, lastApplied, applied) are compared before and after the
    # handler, and a net change means the handler's effects were saved
    # atomically as the node's next barrier. When a storageFaults rule
    # matches that barrier's number, the handler runs speculatively — its
    # timeline entries and scheduled events are buffered, and the node
    # state, the simulator bookkeeping and the network send counters are
    # snapshotted — so a failed barrier can be suppressed completely: the
    # update never reaches the persistent image, nothing that depends on it
    # becomes visible, and the node crashes at the same instant.

    # Sentinel for "no rule matches the upcoming barrier" (distinct from a
    # rule whose restartDelay is omitted, which stores None).
    _NO_RULE = object()

    def _deliver_guarded(self, msg: dict) -> None:
        if not self.storage_faults_active:
            self._deliver(msg)
            return
        if not self.network.deliver(msg, self.state[msg["dst"]].online):
            return
        self._execute_with_barrier(msg["dst"], self._route_message, msg)

    def _execute_with_barrier(self, node: str, handler, *args) -> None:
        """Run one node-targeted handler, failing its persistence barrier
        when a storageFaults rule matches the barrier's number."""
        if not self.storage_faults_active:
            handler(*args)
            return
        st = self.state[node]
        before = (
            st.term,
            st.voted_for,
            list(st.log),
            st.snapshot_index,
            st.snapshot_term,
            st.snapshot_config,
            st.commit_index,
            st.last_applied,
            list(st.applied),
        )
        occurrence = self.storage_counts.get(node, 0) + 1
        delay = self.storage_fault_rules.get((node, occurrence), self._NO_RULE)
        rollback = None
        if delay is not self._NO_RULE:
            # The barrier, if this handler crosses one, fails: run
            # speculatively so every effect can still be suppressed.
            rollback = self._capture_rollback(node)
            self._record_buffer = []
            self.queue.begin_staging()
        handler(*args)
        fields = self._changed_persistent_fields(st, before)
        if not fields:
            # No persistent write happened: no barrier was crossed, the rule
            # stays pending for the next one, and any speculative effects
            # commit normally.
            if rollback is not None:
                self._commit_effects()
            return
        self.storage_counts[node] = occurrence
        if rollback is None:
            # An unremarkable barrier: the update is persisted.
            return
        # The barrier fails. The update never reaches the persistent image
        # and every effect depending on it — messages, responses, state
        # changes, client results — is suppressed; messages sent earlier are
        # unaffected and still arrive as scheduled.
        self._record_buffer = None
        self.queue.discard_staging()
        self._restore_rollback(node, rollback)
        self._record({
            "type": "storageFault",
            "node": node,
            "occurrence": occurrence,
            "fields": fields,
        })
        # The node crashes at the same instant, with the usual crash
        # semantics (offline, timers cancelled, pending reads abandoned).
        self._on_node_event({"node": node, "action": "crash"})
        if delay is not None:
            # Automatic restart with the existing restart semantics; a
            # restart time beyond the duration is simply never processed,
            # and a zero delay is still recorded (crash, then restart)
            # before the remaining events at this instant.
            restart_time = self.now + delay
            self._restart_order += 1
            self.queue.push((
                restart_time,
                _KIND_NODE_EVENT,
                self._restart_order,
                {"time": restart_time, "node": node, "action": "restart"},
            ))

    @staticmethod
    def _changed_persistent_fields(st: _Node, before: tuple) -> list[str]:
        """The persisted fields a handler net-changed, in the fixed
        storageFault reporting order."""
        fields = []
        if st.term != before[0]:
            fields.append("term")
        if st.voted_for != before[1]:
            fields.append("votedFor")
        if st.log != before[2]:
            fields.append("log")
        if (st.snapshot_index, st.snapshot_term, st.snapshot_config) != before[3:6]:
            fields.append("snapshot")
        if st.commit_index != before[6]:
            fields.append("commitIndex")
        if st.last_applied != before[7]:
            fields.append("lastApplied")
        if st.applied != before[8]:
            fields.append("applied")
        return fields

    def _capture_rollback(self, node: str) -> tuple:
        """Snapshot everything a failed barrier must restore: the node's
        full state, the simulator bookkeeping a handler can touch, and the
        network send counters (a suppressed send must not consume a
        messageFaults occurrence)."""
        st = self.state[node]
        node_state = {slot: copy.deepcopy(getattr(st, slot)) for slot in _Node.__slots__}
        bookkeeping = {
            name: copy.deepcopy(getattr(self, name))
            for name in (
                "rejected",
                "change_results",
                "pending_catchup",
                "pending_reads",
                "read_results",
                "read_required",
                "committed_command_ids",
                "accepted_command_ids",
                "leaders_by_term",
                "stable_commit_times",
            )
        }
        network_counters = (
            self.network.send_counter,
            copy.deepcopy(self.network.message_fault_counts),
        )
        return node_state, bookkeeping, network_counters

    def _restore_rollback(self, node: str, rollback: tuple) -> None:
        node_state, bookkeeping, network_counters = rollback
        st = self.state[node]
        for slot, value in node_state.items():
            setattr(st, slot, value)
        for name, value in bookkeeping.items():
            setattr(self, name, value)
        self.network.send_counter = network_counters[0]
        self.network.message_fault_counts = network_counters[1]

    def _commit_effects(self) -> None:
        """Settle a speculative region whose handler crossed no barrier:
        buffered timeline entries and scheduled events become visible."""
        buffered, self._record_buffer = self._record_buffer, None
        self.timeline.extend(buffered)
        self.queue.commit_staging()


def run_simulation(raw: object) -> dict:
    """Validate a decoded JSON scenario and run the deterministic simulation."""
    return _Simulator(parse_scenario(raw)).run()
