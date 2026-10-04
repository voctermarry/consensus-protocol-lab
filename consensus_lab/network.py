"""Network delivery and fault determination.

Owns the partition state, the per-message fault rules and their send
occurrence counters, and the whole send/deliver path: every message is
stamped with a deterministic send sequence number, matched against
``messageFaults`` rules by send occurrence, and enqueued for arrival at send
time + messageDelay (+ rule delay). On arrival the network decides —
deterministically — whether the envelope is dropped (messageFault drop,
destination down, partition) or handed to the protocol layer for dispatch.
"""

from __future__ import annotations

from .events import _KIND_MESSAGE, _KIND_REACTION, EventQueue


def _query_id_field(msg: dict) -> dict:
    """Read-confirmation messages (readProbe/readReply) carry the query id
    into their timeline entries; no other message kind has one."""
    return {"id": msg["id"]} if "id" in msg else {}

def _pre_vote_fields(msg: dict) -> dict:
    """Pre-vote messages carry their round and prospective term into every
    timeline entry; no other message kind has either."""
    fields: dict = {}
    if "round" in msg:
        fields["round"] = msg["round"]
    if "prospectiveTerm" in msg:
        fields["prospectiveTerm"] = msg["prospectiveTerm"]
    return fields


class Network:
    """The deterministic transport: delay model, partitions, message faults.

    ``queue`` is the simulation's event queue, ``record`` appends a timeline
    entry, and ``clock`` / ``in_reaction_phase`` read the core loop's current
    virtual time and drain phase so zero-delay reactions are scheduled into
    the right event kind.
    """

    def __init__(
        self,
        delay: int,
        message_fault_rules: list[dict],
        queue: EventQueue,
        record,
        clock,
        in_reaction_phase,
    ) -> None:
        self.delay = delay
        # Per-message fault rules, keyed by the full selector
        # (from, to, message, occurrence); the per-(from, to, message) send
        # counter below numbers actual sends from the start of the run.
        self.message_faults: dict[tuple, dict] = {
            (rule["from"], rule["to"], rule["message"], rule["occurrence"]): rule
            for rule in message_fault_rules
        }
        self.message_fault_counts: dict[tuple, int] = {}
        self.partition: list[frozenset[str]] | None = None
        self.send_counter = 0
        self.queue = queue
        self.record = record
        self._clock = clock
        self._in_reaction_phase = in_reaction_phase

    def apply_fault(self, fault: dict) -> None:
        if fault["action"] == "partition":
            self.partition = [frozenset(group) for group in fault["groups"]]
            self.record({"type": "fault", "action": "partition", "groups": fault["groups"]})
        else:
            self.partition = None
            self.record({"type": "fault", "action": "heal"})

    def connected(self, a: str, b: str) -> bool:
        if self.partition is None:
            return True
        return any(a in group and b in group for group in self.partition)

    def send(self, src: str, dst: str, msg: dict) -> None:
        self.record({
            "type": "messageSend",
            "node": src,
            "peer": dst,
            "message": msg["kind"],
            "term": msg["term"],
            **_query_id_field(msg),
            **_pre_vote_fields(msg),
        })
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
            scheduled = self._clock() + self.delay
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
            self.record(fault_entry)
        effective_delay = self.delay + extra_delay
        # Zero-delay replication triggered while draining a client-command
        # batch shares the batch's timestamp and must follow the remaining
        # client commands; a positive delay lands strictly in the future and
        # queues as a normal message arrival.
        in_phase = self._in_reaction_phase() and effective_delay == 0
        kind = _KIND_REACTION if in_phase else _KIND_MESSAGE
        self.queue.push((self._clock() + effective_delay, kind, self.send_counter, envelope))

    def deliver(self, msg: dict, dst_online: bool) -> bool:
        """Decide the fate of an arriving envelope. A drop (messageFault
        rule, destination down, partition) is recorded here and reported as
        False; True means the message must be dispatched to its handler."""
        src, dst = msg["src"], msg["dst"]
        if msg.get("faultDrop"):
            # A messageFaults drop rule: the message never reaches the
            # receiver, reported at its originally scheduled arrival time.
            self.record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": msg["kind"],
                "term": msg["term"],
                **_query_id_field(msg),
                **_pre_vote_fields(msg),
                "result": "dropped",
                "reason": "messageFault",
            })
            return False
        if not dst_online:
            # The destination crashed after this message was sent; it is
            # dropped on arrival. Messages the crashed node itself sent
            # earlier are unaffected and still land at their original time.
            self.record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": msg["kind"],
                "term": msg["term"],
                **_query_id_field(msg),
                **_pre_vote_fields(msg),
                "result": "dropped",
                "reason": "nodeDown",
            })
            return False
        if not self.connected(src, dst):
            self.record({
                "type": "messageResult",
                "node": dst,
                "peer": src,
                "message": msg["kind"],
                "term": msg["term"],
                **_query_id_field(msg),
                **_pre_vote_fields(msg),
                "result": "dropped",
                "reason": "partition",
            })
            return False
        return True
