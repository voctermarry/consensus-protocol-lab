"""Protocol event-handling kernel, split by responsibility.

The kernel is a set of stateless mixins that are assembled into one class:

* :class:`_CommonKernel` — the single timeline recorder and configuration /
  quorum helpers shared by every responsibility;
* :class:`_ElectionKernel` — elections, the optional pre-vote phase and the
  election timer;
* :class:`_ReplicationKernel` — heartbeats, log replication, snapshots and
  commit/apply;
* :class:`_MembershipKernel` — joint-consensus membership changes;
* :class:`_ReadsKernel` — read-only queries and their confirmation;
* :class:`_LifecycleKernel` — crash/restart and the heartbeat timer event;
* :class:`_LivenessKernel` — liveness checks;
* :class:`_DispatchKernel` — routing of delivered envelopes.

The mixins hold no state of their own and are never instantiated separately:
the composing simulator contributes the one node-state map, virtual clock,
event queue, network layer, timeline and bookkeeping ledgers, and every
cross-domain call stays a plain method call on that single object. Splitting
the handlers therefore neither copies state nor adds any synchronization —
it only separates the transition rules into independently understandable
boundaries.
"""

from __future__ import annotations

from .common import _CommonKernel
from .dispatch import _DispatchKernel
from .election import _ElectionKernel
from .lifecycle import _LifecycleKernel
from .liveness import _LivenessKernel
from .membership import _MembershipKernel
from .reads import _ReadsKernel
from .replication import _ReplicationKernel


class _Protocol(
    _DispatchKernel,
    _LivenessKernel,
    _LifecycleKernel,
    _ReadsKernel,
    _MembershipKernel,
    _ReplicationKernel,
    _ElectionKernel,
    _CommonKernel,
):
    """The assembled protocol kernel.

    The base order is cosmetic — the mixins define disjoint method sets — but
    puts the cross-domain coordination mixins ahead of the domains they call
    into and the shared helpers last, mirroring how a delivered event flows
    from dispatch through one responsibility to the common primitives.
    """
