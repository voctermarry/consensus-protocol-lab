"""Liveness evaluation.

``_Liveness`` owns the liveness checks: at every fully drained timestamp
inside a check's window it evaluates whether the target condition holds
(leader online, command committed, read completed, membership change
committed) and, at the deadline, why it failed. It only reads the shared
bookkeeping the other domains have already written — it never mutates node
state, sends messages or schedules events.

Provides:
    _evaluate_liveness — run every open check whose window contains the
        drained timestamp (simulator loop)

Requires (from the composing simulator):
    ``state``, ``liveness_checks``, ``liveness_results``,
    ``committed_command_ids``, ``accepted_command_ids``, ``rejected``,
    ``read_results``, ``change_results``, ``stable_commit_times`` and the
    ``leader_online`` flag it maintains; plus kernel._record.
"""

from __future__ import annotations

from ..node import ROLE_LEADER


class _Liveness:
    # -- liveness checks -------------------------------------------------------

    def _has_online_leader(self) -> bool:
        return any(
            st.online and st.role == ROLE_LEADER for st in self.state.values()
        )

    def _command_present_anywhere(self, command_id: str) -> bool:
        """Whether an accepted command still survives in some node's applied
        history or uncompacted log suffix."""
        for st in self.state.values():
            if any(entry.get("id") == command_id for entry in st.applied):
                return True
            if any(entry.get("id") == command_id for entry in st.log):
                return True
        return False

    def _liveness_satisfied(self, check: dict) -> bool:
        check_type = check["type"]
        target = check["target"]
        if check_type == "leaderElected":
            return self.leader_online
        if check_type == "clientCommitted":
            return target in self.committed_command_ids
        if check_type == "readCompleted":
            result = self.read_results.get(target)
            return result is not None and result.get("outcome") == "completed"
        # membershipCommitted: the stable configuration entry has been applied
        # (the change committed).
        return target in self.stable_commit_times

    def _liveness_failure_reason(self, check: dict) -> str | None:
        """A terminal reason once the target can never satisfy the check;
        None while satisfaction is still possible."""
        check_type = check["type"]
        target = check["target"]
        if check_type == "clientCommitted":
            if target in self.rejected:
                return "targetRejected"
            if (
                target in self.accepted_command_ids
                and target not in self.committed_command_ids
                and not self._command_present_anywhere(target)
            ):
                # Accepted earlier, but its uncommitted entry was truncated by
                # a higher-term leader: it can never commit anymore.
                return "targetSuperseded"
            return None
        if check_type == "readCompleted":
            result = self.read_results.get(target)
            if result is not None and result.get("outcome") == "rejected":
                return "targetRejected"
            return None
        if check_type == "membershipCommitted":
            result = self.change_results.get(target)
            if result is not None and result.get("outcome") == "rejected":
                return "targetRejected"
            return None
        return None

    def _evaluate_liveness(self, time: int) -> None:
        """Run every open check whose window contains this timestamp, in
        input order, against the fully drained state at ``time``."""
        self.leader_online = self._has_online_leader()
        for check in self.liveness_checks:
            check_id = check["id"]
            if check_id in self.liveness_results:
                continue
            if time < check["startTime"] or time > check["deadline"]:
                continue
            result = {"id": check_id, "checkType": check["type"]}
            if check["target"] is not None:
                result["target"] = check["target"]
            if not self._liveness_satisfied(check):
                if time < check["deadline"]:
                    # The first satisfying moment may still arrive later in
                    # the window; a terminal target failure likewise surfaces
                    # as the reason at the deadline.
                    continue
                result["status"] = "failed"
                result["time"] = time
                result["reason"] = self._liveness_failure_reason(check) or "deadlineExceeded"
            else:
                result["status"] = "satisfied"
                result["time"] = time
            self.liveness_results[check_id] = result
            entry = {"type": "livenessResult", "id": check_id, "checkType": result["checkType"]}
            if check["target"] is not None:
                entry["target"] = check["target"]
            entry["status"] = result["status"]
            if result["status"] == "failed":
                entry["reason"] = result["reason"]
            self._record(entry)
