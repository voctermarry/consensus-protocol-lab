"""Deterministic virtual-time event queue for the simulation.

Every event is a heap entry ``(time, kind, order, payload)``. Virtual time is
the primary key; the event kind is the secondary key and gives the fixed
same-timestamp priority (faults first, liveness last); the order field breaks
remaining ties, either by input position (scheduled inputs) or by the global
send sequence number (messages). The heap never reads the wall clock and
never uses randomness, so the processing order is fully determined by the
normalized scenario.
"""

from __future__ import annotations

import heapq

# Event kinds, processed in this order when they share a timestamp.
KIND_FAULT = 0
KIND_NODE_EVENT = 1
KIND_MESSAGE = 2
KIND_CLIENT = 3
# Replication traffic triggered by client commands (and its replies) lands
# here so that, even with zero message delay, all client commands sharing a
# timestamp are handled in input order before their reactions drain.
KIND_REACTION = 4
# Membership requests sit behind every pending reaction: the heap keeps
# (t, REACTION, ...) entries ahead of (t, MEMBERSHIP, ...), so a request's own
# zero-delay cascade likewise drains before the next timed event.
KIND_MEMBERSHIP = 5
# Read-only queries are accepted after faults, node events, arrived messages,
# client commands (and their zero-delay reactions) and membership requests,
# and before timeouts and heartbeats; a query's own zero-delay probe cascade
# likewise drains before the next timed event.
KIND_READ = 6
KIND_TIMEOUT = 7
KIND_HEARTBEAT = 8
# Liveness checks are evaluated after every pre-existing event (and its
# zero-delay reaction cascade) sharing the timestamp has drained, so this is
# the largest event kind.
KIND_LIVENESS = 9

# Processing one of these inputs opens the zero-delay reaction window: new
# zero-delay traffic it triggers is queued as REACTION entries sharing the
# timestamp, ahead of the remaining same-time inputs.
_REACTION_TRIGGERS = frozenset(
    {KIND_CLIENT, KIND_MEMBERSHIP, KIND_READ, KIND_REACTION}
)


class EventQueue:
    """Min-heap of ``(time, kind, order, payload)`` simulation events."""

    __slots__ = ("_heap",)

    def __init__(self) -> None:
        self._heap: list[tuple] = []

    def push(self, time: int, kind: int, order: int, payload: object) -> None:
        heapq.heappush(self._heap, (time, kind, order, payload))

    def pop(self) -> tuple:
        return heapq.heappop(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def next_time(self) -> int:
        """The timestamp of the earliest pending event."""
        return self._heap[0][0]

    @staticmethod
    def is_reaction_trigger(kind: int) -> bool:
        """Whether handling this kind keeps newly sent zero-delay messages in
        the same-timestamp reaction cascade."""
        return kind in _REACTION_TRIGGERS
