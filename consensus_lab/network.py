"""Network delivery, per-occurrence message faults and connectivity.

This module owns everything that decides *whether* and *when* a sent message
arrives:

* the uniform message delay;
* ``messageFaults`` rules, keyed by the full selector
  ``(from, to, message, occurrence)`` and matched against actual sends
  numbered from the start of the run;
* the current network partition and the reachability test.

It never interprets a delivered message and never records timeline entries
itself: the engine records the send and the rule hit, asks the fabric for the
effective delay and drop verdict, and performs protocol handling only for
messages the fabric does not reject.
"""

from __future__ import annotations

from dataclasses import dataclass


def query_id_field(msg: dict) -> dict:
    """Read-confirmation messages (readProbe/readReply) carry the query id
    into their timeline entries; no other message kind has one."""
    return {"id": msg["id"]} if "id" in msg else {}


def pre_vote_fields(msg: dict) -> dict:
    """Pre-vote messages carry their round and prospective term into every
    timeline entry; no other message kind has either."""
    fields: dict = {}
    if "round" in msg:
        fields["round"] = msg["round"]
    if "prospectiveTerm" in msg:
        fields["prospectiveTerm"] = msg["prospectiveTerm"]
    return fields


def message_extra_fields(msg: dict) -> dict:
    """The fields a send record or a drop result repeats from the message:
    the read-query id and the pre-vote round/prospective term."""
    return {**query_id_field(msg), **pre_vote_fields(msg)}


@dataclass(frozen=True)
class SendOutcome:
    """The network verdict for one actual send."""

    occurrence: int
    action: str | None
    scheduled_time: int
    arrival_time: int
    drop: bool

    @property
    def delay(self) -> int:
        return self.arrival_time - self.scheduled_time


class NetworkFabric:
    def __init__(self, rules: list[dict], delay: int) -> None:
        # Per-message fault rules, keyed by the full selector
        # (from, to, message, occurrence); the per-(from, to, message) send
        # counter below numbers actual sends from the start of the run.
        self.rules: dict[tuple, dict] = {
            (rule["from"], rule["to"], rule["message"], rule["occurrence"]): rule
            for rule in rules
        }
        self.send_counts: dict[tuple, int] = {}
        self.delay = delay
        # None means full connectivity; otherwise a list of two groups and
        # only same-group pairs can communicate.
        self.partition: list[frozenset[str]] | None = None

    def on_send(self, now: int, src: str, dst: str, kind: str) -> SendOutcome:
        """Account for one actual send and decide its delivery fate.

        A drop rule marks the message dropped but keeps its original arrival
        slot; a delay rule shifts the arrival by the rule's own extra delay.
        """
        count_key = (src, dst, kind)
        occurrence = self.send_counts.get(count_key, 0) + 1
        self.send_counts[count_key] = occurrence
        rule = self.rules.get((src, dst, kind, occurrence))
        scheduled = now + self.delay
        if rule is None:
            return SendOutcome(occurrence, None, scheduled, scheduled, False)
        if rule["action"] == "drop":
            return SendOutcome(occurrence, "drop", scheduled, scheduled, True)
        extra_delay = rule["delay"]
        return SendOutcome(
            occurrence, "delay", scheduled, scheduled + extra_delay, False
        )

    def connected(self, a: str, b: str) -> bool:
        if self.partition is None:
            return True
        return any(a in group and b in group for group in self.partition)

    def set_partition(self, groups: list[list[str]] | None) -> None:
        """Install a partition (two groups) or restore full connectivity."""
        self.partition = None if groups is None else [frozenset(group) for group in groups]

    def drop_reason(self, msg: dict, dst_online: bool) -> str | None:
        """Why a message must not be delivered at its arrival time, or None
        when protocol handling should run. Checks run in their fixed order:
        an explicit messageFaults drop, then a crashed destination, then the
        partition."""
        if msg.get("faultDrop"):
            # A messageFaults drop rule: the message never reaches the
            # receiver, reported at its originally scheduled arrival time.
            return "messageFault"
        if not dst_online:
            # The destination crashed after this message was sent; it is
            # dropped on arrival. Messages the crashed node itself sent
            # earlier are unaffected and still land at their original time.
            return "nodeDown"
        if not self.connected(msg["src"], msg["dst"]):
            return "partition"
        return None
