"""Read-only replay verification.

``replay`` recomputes a simulation from the original scenario with the same
validation and simulation semantics as ``simulate``, then compares the
recomputed result with a saved result as JSON values: object member order is
ignored, array order is significant, strings are equal by their Unicode
content, numbers compare by value and booleans never equal numbers.

Like the rest of the lab it never reads the wall clock, never uses
randomness and never creates or rewrites files, so identical input produces
byte-identical output.
"""

from __future__ import annotations

from typing import Any

from .simulate import ScenarioError, run_simulation

# Sentinel for the root location in an RFC 6901 JSON Pointer.
_ROOT = ""


def _escape_token(token: str) -> str:
    """Escape one RFC 6901 reference token (``~`` before ``/``)."""
    return token.replace("~", "~0").replace("/", "~1")


def _unescape_token(raw_token: str) -> str:
    """Reverse RFC 6901 reference-token escaping (``~1`` before ``~0``)."""
    return raw_token.replace("~1", "/").replace("~0", "~")


def _pointer(tokens: list[str]) -> str:
    if not tokens:
        return _ROOT
    return "/" + "/".join(_escape_token(token) for token in tokens)


def _json_type(value: Any) -> str:
    """The JSON-level type used to tell values apart: bool is distinct from
    number. Both decoded JSON input and the recomputed result hold only
    JSON-compatible Python values, so the isinstance checks are faithful."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if value is None:
        return "null"
    return "object"


def _diff(expected: Any, actual: Any, tokens: list[str]) -> str | None:
    """Return an RFC 6901 pointer to the first difference, or None when the
    two JSON values are structurally equal under the replay rules.

    Arrays are compared by ascending index, objects over the Unicode code
    point ordering of the union of their keys; a type or scalar mismatch ends
    the comparison at the current location, and a shared prefix followed by
    unequal array lengths points at the first missing index."""
    te = _json_type(expected)
    ta = _json_type(actual)
    if te != ta:
        return _pointer(tokens)
    kind = te
    if kind == "object":
        for key in sorted(set(expected) | set(actual)):
            if (key in expected) != (key in actual):
                return _pointer(tokens + [key])
            pointer = _diff(expected[key], actual[key], tokens + [key])
            if pointer is not None:
                return pointer
        return None
    if kind == "array":
        length = min(len(expected), len(actual))
        for index in range(length):
            pointer = _diff(expected[index], actual[index], tokens + [str(index)])
            if pointer is not None:
                return pointer
        if len(expected) != len(actual):
            return _pointer(tokens + [str(length)])
        return None
    if expected != actual:
        return _pointer(tokens)
    return None


def _resolve(doc: Any, pointer: str) -> tuple[bool, Any]:
    """Look an RFC 6901 pointer up in a document, returning
    ``(present, value)``; present is False for a missing key/index."""
    if pointer == _ROOT:
        return True, doc
    current: Any = doc
    for raw_token in pointer.split("/")[1:]:
        token = _unescape_token(raw_token)
        if isinstance(current, dict):
            if token not in current:
                return False, None
            current = current[token]
        elif isinstance(current, list):
            # Pointers produced by _diff only index existing positions or the
            # first one-past-the-end position, both of which are plain digits.
            if not token.isdigit() or int(token) >= len(current):
                return False, None
            current = current[int(token)]
        else:
            return False, None
    return True, current


def _canonical(value: Any) -> Any:
    """Rebuild a JSON value with every object's members in Unicode code
    point order, leaving arrays and scalars untouched. Embedded mismatch
    values are canonicalized so the verdict is byte-identical regardless of
    the source file's (equality-irrelevant) member order."""
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def run_replay(scenario_raw: Any, result_raw: Any) -> dict:
    """Validate the scenario, recompute its simulation and return a JSON-
    serializable verdict for the saved result."""
    expected_result = run_simulation(scenario_raw)
    if not isinstance(result_raw, dict):
        raise ScenarioError("result must be a JSON object")
    pointer = _diff(expected_result, result_raw, [])
    if pointer is None:
        return {"status": "matched"}

    present_e, value_e = _resolve(expected_result, pointer)
    present_a, value_a = _resolve(result_raw, pointer)
    verdict = {
        "status": "mismatched",
        "path": pointer,
        "expectedPresent": present_e,
        "actualPresent": present_a,
    }
    if present_e:
        verdict["expected"] = _canonical(value_e)
    if present_a:
        verdict["actual"] = _canonical(value_a)
    return verdict
