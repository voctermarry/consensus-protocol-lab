"""Per-node persistent and volatile state.

``_Node`` holds one server's full state — persisted fields (term, vote, log,
snapshot, commit/apply progress) and volatile ones (role, replication
progress, pre-vote round, leader-contact clock) — together with the log and
configuration views over them. It contains no simulation logic of its own.
"""

from __future__ import annotations

ROLE_FOLLOWER = "follower"
ROLE_CANDIDATE = "candidate"
ROLE_LEADER = "leader"
# Optional pre-vote phase: a preCandidate probes the cluster with a
# prospective term (current term + 1) without touching its persisted term or
# votedFor, so a node rejoining after an isolation gap cannot disrupt a
# stable leader with a meaningless high term.
ROLE_PRECANDIDATE = "preCandidate"

# Log-entry kinds: ordinary client commands versus replicated configuration
# entries created by joint-consensus membership changes.
KIND_COMMAND = "command"
KIND_CONFIG = "config"
CONFIG_JOINT = "joint"
CONFIG_STABLE = "stable"


class _Node:
    __slots__ = (
        "role",
        "term",
        "voted_for",
        "known_leader",
        "votes",
        "timeout_gen",
        "log",
        "snapshot_index",
        "snapshot_term",
        "snapshot_config",
        "commit_index",
        "last_applied",
        "applied",
        "next_index",
        "match_index",
        "online",
        "restart_count",
        "pre_round",
        "pre_term",
        "pre_votes",
        "pre_config",
        "leader_contact",
    )

    def __init__(self) -> None:
        self.role = ROLE_FOLLOWER
        self.term = 0
        self.voted_for: str | None = None
        self.known_leader: str | None = None
        self.votes: set[str] = set()
        self.timeout_gen = 0
        # Log entries keep their global indices: self.log holds only the
        # uncompacted suffix, so entry for global index i is self.log[i - 1 -
        # self.snapshot_index]. snapshot_index/snapshot_term describe the last
        # entry folded into the (modeled) snapshot; 0 means no snapshot yet.
        self.log: list[dict] = []
        self.snapshot_index = 0
        self.snapshot_term = 0
        # Configuration in effect at snapshot_index (None before any
        # snapshot). Config entries can be compacted into snapshots like any
        # committed entry, so membership survives compaction.
        self.snapshot_config: dict | None = None
        self.commit_index = 0
        self.last_applied = 0
        self.applied: list[dict] = []
        # Leader-only replication progress, keyed by peer name.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        # Crash/restart lifecycle. term, voted_for, log, snapshot_index,
        # snapshot_term, snapshot_config, commit_index, last_applied, applied
        # and the configuration encoded in the log all model persisted state:
        # every change to them is made (synchronously) before the response that
        # depends on it, so they survive a crash and are simply kept on
        # restart. Everything else is volatile and is reset on restart.
        self.online = True
        self.restart_count = 0
        # Optional pre-vote phase state (volatile; never persisted). A
        # preCandidate probes with prospective term pre_term = term + 1 and
        # round pre_round without changing term or voted_for; pre_votes holds
        # the peers that granted the active round and pre_config the electorate
        # that round is decided under. All four are reset on restart.
        self.pre_round = 0
        self.pre_term = 0
        self.pre_votes: set[str] = set()
        self.pre_config: dict | None = None
        # Virtual time of the most recent heartbeat/appendEntries/
        # installSnapshot accepted from a current-term leader, or -1 when no
        # such contact has occurred since the term began (or since startup or
        # restart). A pre-vote is granted only when no contact is recorded or
        # the local election timeout has elapsed since the latest contact.
        # Purely internal: it is never serialized.
        self.leader_contact = -1

    def last_log_index(self) -> int:
        return self.snapshot_index + len(self.log)

    def last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else self.snapshot_term

    def term_at(self, index: int) -> int:
        if index <= 0:
            return 0
        if index == self.snapshot_index:
            return self.snapshot_term
        if index < self.snapshot_index:
            raise IndexError(f"index {index} is covered by the snapshot")
        return self.log[index - 1 - self.snapshot_index]["term"]

    def has_entry(self, index: int) -> bool:
        return self.snapshot_index < index <= self.last_log_index()

    def config_at(self, index: int, initial_config: dict) -> dict | None:
        """The configuration in effect immediately *before* the entry at
        ``index`` is applied: the latest config entry with a smaller index, or
        the snapshot config / initial configuration. Returns None when no
        configuration entry precedes ``index`` (the caller substitutes the
        initial configuration)."""
        config = self.snapshot_config if self.snapshot_config else initial_config
        upper = min(index - 1, self.last_log_index())
        for i in range(self.snapshot_index + 1, upper + 1):
            entry = self.log[i - 1 - self.snapshot_index]
            if entry.get("kind") == KIND_CONFIG:
                config = entry["config"]
        return config

    def latest_config(self, initial_config: dict) -> dict:
        """The newest configuration present in this node's log (or its
        snapshot), whether or not it is committed or applied."""
        result = self.snapshot_config if self.snapshot_config else initial_config
        for entry in self.log:
            if entry.get("kind") == KIND_CONFIG:
                result = entry["config"]
        return result

    def committed_config(self, initial_config: dict) -> dict:
        """The configuration established by the newest committed config
        entry (or the snapshot config, which only covers committed state)."""
        config = self.snapshot_config if self.snapshot_config else initial_config
        upper = min(self.commit_index, self.last_log_index())
        for i in range(self.snapshot_index + 1, upper + 1):
            entry = self.log[i - 1 - self.snapshot_index]
            if entry.get("kind") == KIND_CONFIG:
                config = entry["config"]
        return config
