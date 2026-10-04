"""Deterministic virtual-time event queue.

Events are ordered by (virtual time, kind, scheduling order): all events
sharing a timestamp drain in kind order, and same-kind events drain in the
order they were scheduled. The queue only ever holds virtual timestamps; it
never reads the wall clock.
"""

from __future__ import annotations

import heapq

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
# Read-only queries are accepted after faults, node events, arrived messages,
# client commands (and their zero-delay reactions) and membership requests,
# and before timeouts and heartbeats; a query's own zero-delay probe cascade
# likewise drains before the next timed event.
_KIND_READ = 6
_KIND_TIMEOUT = 7
_KIND_HEARTBEAT = 8
# Liveness checks are evaluated after every pre-existing event (and its
# zero-delay reaction cascade) sharing the timestamp has drained, so this is
# the largest event kind.
_KIND_LIVENESS = 9


class EventQueue:
    """A deterministic priority queue of ``(time, kind, order, payload)``
    events. ``order`` is a monotonically assigned sequence number per
    scheduling site, so events comparing equal on ``(time, kind)`` drain in
    scheduling order and the payload never takes part in comparisons."""

    __slots__ = ("_heap",)

    def __init__(self) -> None:
        self._heap: list[tuple] = []

    def push(self, item: tuple) -> None:
        heapq.heappush(self._heap, item)

    def pop(self) -> tuple:
        return heapq.heappop(self._heap)

    @property
    def next_time(self) -> int:
        """Virtual time of the earliest queued event (queue must be non-empty)."""
        return self._heap[0][0]

    def __bool__(self) -> bool:
        return bool(self._heap)
