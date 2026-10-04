"""Deterministic Raft simulation — public entry point and compatibility façade.

The simulation kernel now lives in focused modules:

* :mod:`consensus_lab.scenario` — structural and cross-reference validation,
  producing the normalized config;
* :mod:`consensus_lab.events` — the deterministic virtual-time event queue and
  the same-timestamp event priorities;
* :mod:`consensus_lab.network` — message delivery, occurrence-based message
  faults and partition reachability;
* :mod:`consensus_lab.node` — per-node persisted and volatile state;
* :mod:`consensus_lab.engine` — protocol state transitions and the one
  execution path shared by simulate, explore and replay;
* :mod:`consensus_lab.report` — the final, read-only report assembly.

This module keeps every historical name (including the private ones used by
the bundled tests and by ``explore``) so existing callers are unaffected.
"""

from __future__ import annotations

from .engine import SimulationEngine, execute_scenario
from .node import (
    CONFIG_JOINT,
    CONFIG_STABLE,
    KIND_COMMAND,
    KIND_CONFIG,
    ROLE_CANDIDATE,
    ROLE_FOLLOWER,
    ROLE_LEADER,
    ROLE_PRECANDIDATE,
)
from .scenario import (
    ScenarioError,
    _require_int,
    parse_scenario,
)

# Historical class name: explore and the test suite construct the kernel
# directly and read its state/timeline. It is the same SimulationEngine that
# run_simulation drives.
_Simulator = SimulationEngine

__all__ = [
    "CONFIG_JOINT",
    "CONFIG_STABLE",
    "KIND_COMMAND",
    "KIND_CONFIG",
    "ROLE_CANDIDATE",
    "ROLE_FOLLOWER",
    "ROLE_LEADER",
    "ROLE_PRECANDIDATE",
    "ScenarioError",
    "parse_scenario",
    "run_simulation",
]


def run_simulation(raw: object) -> dict:
    """Validate a decoded JSON scenario and run the deterministic simulation."""
    return execute_scenario(parse_scenario(raw))
