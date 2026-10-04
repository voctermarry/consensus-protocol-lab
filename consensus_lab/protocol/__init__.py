"""Node state transitions: the protocol core of the simulation.

The protocol kernel is split by responsibility into one mixin per domain,
composed here into the single :class:`_Protocol` class:

- :mod:`.kernel` — timeline recording and message dispatch;
- :mod:`.config` — configuration views and quorum arithmetic;
- :mod:`.elections` — elections and the optional pre-vote phase;
- :mod:`.replication` — log replication, commit/apply and snapshots;
- :mod:`.membership` — joint-consensus membership changes;
- :mod:`.reads` — read-only queries with leader-confirmation probes;
- :mod:`.lifecycle` — crash/restart;
- :mod:`.liveness` — liveness evaluation.

Each domain module documents the hooks it provides to and requires from its
siblings; cross-domain flows (a higher-term step-down cancelling the
pre-vote round and the pending reads, a committed configuration taking
effect on elections/replication/reads, a snapshot restoring membership)
meet only at those named hooks. The composing simulator provides the single
shared run-time state: ``state`` (node name -> ``_Node``), ``queue``
(``EventQueue``), ``network`` (``Network``), the timeline recorder and the
bookkeeping dictionaries. Every method only mutates that state, in the
established deterministic order — no domain keeps a private copy of any of
it.
"""

from __future__ import annotations

from .config import _ConfigView
from .elections import _Elections
from .kernel import _Kernel
from .lifecycle import _Lifecycle
from .liveness import _Liveness
from .membership import _Membership
from .reads import _Reads
from .replication import _Replication


class _Protocol(
    _Kernel,
    _ConfigView,
    _Elections,
    _Replication,
    _Membership,
    _Reads,
    _Lifecycle,
    _Liveness,
):
    """The composed protocol kernel: every domain mixin over the one shared
    simulator state. There are no name collisions between the mixins, so the
    method-resolution order never selects between competing definitions."""
