"""Shared timeline and configuration helpers for the protocol kernel.

Every kernel mixin is assembled into one simulator object, so the helpers here
operate on that object's single node-state map, timeline and bookkeeping; no
mixin holds state of its own.
"""

from __future__ import annotations

from ..node import CONFIG_JOINT, CONFIG_STABLE, KIND_COMMAND


class _CommonKernel:
    # -- timeline helpers -------------------------------------------------

    def _record(self, entry: dict) -> None:
        self.timeline.append({"seq": len(self.timeline) + 1, "time": self.now, **entry})

    def _record_state_change(self, name: str, reason: str) -> None:
        st = self.state[name]
        self._record({"type": "stateChange", "node": name, "term": st.term, "role": st.role, "reason": reason})

    # -- configuration -------------------------------------------------------

    def _command_entry(self, index: int, term: int, command_id: str, command: object) -> dict:
        """A client-command log/applied entry. When membership changes are
        enabled it carries ``kind: "command"`` so configuration entries
        (``kind: "config"``) are unambiguously distinguishable; without the
        feature the historical, untyped shape is preserved byte for byte."""
        entry = {"index": index, "term": term}
        if self.membership_enabled:
            entry["kind"] = KIND_COMMAND
        entry["id"] = command_id
        entry["command"] = command
        return entry

    @staticmethod
    def _config_groups(config: dict) -> tuple[frozenset[str], frozenset[str]]:
        """The (old, new) voter sets of a configuration. Stable configurations
        carry the same set twice; joint configurations carry both."""
        return frozenset(config["old"]), frozenset(config["new"])

    def _config_quorums(self, config: dict) -> list[frozenset[str]]:
        old, new = self._config_groups(config)
        if config.get("type") == CONFIG_JOINT:
            return [old, new]
        return [new]

    def _is_voter(self, name: str, config: dict) -> bool:
        old, new = self._config_groups(config)
        return name in old or name in new

    def _can_vote(self, name: str) -> bool:
        """Current voting/election eligibility from the node's newest
        configuration: learners and nodes not present in the latest config
        neither vote nor campaign. A removed node that learns a configuration
        adding it back regains eligibility."""
        st = self.state[name]
        return self._is_voter(name, st.latest_config(self.initial_config))

    @staticmethod
    def _store_config(config: dict) -> dict:
        old, new = _CommonKernel._config_groups(config)
        return {
            "type": config.get("type", CONFIG_STABLE),
            "old": sorted(old),
            "new": sorted(new),
        }

    def _joint_config(self, voters: frozenset[str], action: str, member: str) -> dict:
        if action == "add":
            new_voters = voters | {member}
        else:
            new_voters = voters - {member}
        return {"type": CONFIG_JOINT, "old": sorted(voters), "new": sorted(new_voters)}

    @staticmethod
    def _stable_config(voters: frozenset[str]) -> dict:
        ordered = sorted(voters)
        return {"type": CONFIG_STABLE, "old": ordered, "new": ordered}

    def _has_vote_majorities(self, candidate: str, votes: set[str], config: dict) -> bool:
        for group in self._config_quorums(config):
            needed = len(group) // 2 + 1
            if len([v for v in votes if v in group]) < needed:
                return False
        return True

    def _replicated_by_majorities(self, leader: str, index: int, config: dict) -> bool:
        """Whether ``index`` is present on a strict majority of every quorum
        group (the leader itself counts in any group it belongs to)."""
        st = self.state[leader]
        for group in self._config_quorums(config):
            needed = len(group) // 2 + 1
            points = []
            for peer in group:
                if peer == leader:
                    points.append(st.last_log_index())
                else:
                    points.append(st.match_index.get(peer, 0))
            if len([point for point in points if point >= index]) < needed:
                return False
        return True
