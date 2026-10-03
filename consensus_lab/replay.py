"""Read-only replay verification of a saved ``simulate`` result.

``replay`` reruns the deterministic simulation on the original scenario and
compares the freshly computed result against the saved result field by field.
It never reads the wall clock, never uses randomness and never creates or
modifies files, so identical input produces byte-identical output.
"""

from __future__ import annotations

from .simulate import run_simulation


def _kind(value: object) -> str:
    """A JSON-level type tag. Booleans get their own tag so that ``true`` is
    never equal to the number 1, while ints and floats share ``number`` so
    that numbers compare by numeric value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def _escape_token(token: str) -> str:
    """Escape one RFC 6901 reference token ('~' before '/')."""
    return token.replace("~", "~0").replace("/", "~1")


def _pointer(tokens: list[str]) -> str:
    if not tokens:
        return ""
    return "".join("/" + token for token in tokens)


def _first_difference(expected: object, actual: object, tokens: list[str]) -> str | None:
    """Return the RFC 6901 pointer of the first structural difference, or
    None when the two JSON values are equal.

    Arrays are compared by ascending index; objects recurse over the union of
    both sides' keys in ascending Unicode code-point order. A type or scalar
    difference ends the comparison at the current position; when an array
    prefix matches but the lengths differ, the pointer names the first
    missing index.
    """
    expected_kind = _kind(expected)
    actual_kind = _kind(actual)
    if expected_kind != actual_kind:
        return _pointer(tokens)

    if expected_kind in ("null", "bool", "number", "string"):
        if expected != actual:
            return _pointer(tokens)
        return None

    if expected_kind == "array":
        shared = min(len(expected), len(actual))
        for index in range(shared):
            found = _first_difference(expected[index], actual[index], tokens + [str(index)])
            if found is not None:
                return found
        if len(expected) != len(actual):
            return _pointer(tokens + [str(shared)])
        return None

    # Object: member order is irrelevant; visit the key union by Unicode code
    # point. A key present on only one side is itself the first difference.
    for key in sorted(set(expected) | set(actual)):
        token = _escape_token(key)
        if key not in expected or key not in actual:
            return _pointer(tokens + [token])
        found = _first_difference(expected[key], actual[key], tokens + [token])
        if found is not None:
            return found
    return None


def run_replay(scenario_raw: object, result_raw: object) -> dict:
    """Recompute the simulation for ``scenario_raw`` and compare it against
    the saved ``result_raw``.

    Returns ``{"status": "matched"}`` on equality, otherwise an object with
    ``status: "mismatched"``, the RFC 6901 pointer of the first difference,
    presence flags for both sides and the present value(s). Scenario
    validation errors propagate as :class:`ScenarioError` exactly as in
    ``simulate``.
    """
    expected = run_simulation(scenario_raw)

    path = _first_difference(expected, result_raw, [])
    if path is None:
        return {"status": "matched"}

    mismatch = {
        "status": "mismatched",
        "path": path,
        "expectedPresent": False,
        "actualPresent": False,
    }
    expected_value, actual_value = _value_at(expected, path), _value_at(result_raw, path)
    if expected_value is not _MISSING:
        mismatch["expectedPresent"] = True
        mismatch["expected"] = expected_value
    if actual_value is not _MISSING:
        mismatch["actualPresent"] = True
        mismatch["actual"] = actual_value
    return mismatch


class _Missing:
    """Sentinel for a pointer position absent on one side (distinct from a
    real JSON null, which is a present value)."""


_MISSING = _Missing()


def _value_at(root: object, path: str) -> object:
    """Resolve an RFC 6901 pointer produced by the comparison against one
    side. Returns the ``_MISSING`` sentinel when an object member or array
    index is absent."""
    if path == "":
        return root
    current = root
    for raw_token in path.split("/")[1:]:
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            if token in current:
                current = current[token]
            else:
                return _MISSING
        elif isinstance(current, list) and token.isascii() and token.isdigit():
            index = int(token)
            if 0 <= index < len(current):
                current = current[index]
            else:
                return _MISSING
        else:
            return _MISSING
    return current
