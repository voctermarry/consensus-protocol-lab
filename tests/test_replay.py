"""Tests for the read-only replay subcommand."""

from __future__ import annotations

import json

from consensus_lab.cli import main
from consensus_lab.replay import _first_difference


def _base_scenario(**overrides):
    scenario = {
        "nodes": ["a", "b", "c"],
        "duration": 500,
        "electionTimeouts": {"a": 100, "b": 150, "c": 200},
        "heartbeatInterval": 50,
        "messageDelay": 10,
    }
    scenario.update(overrides)
    return scenario


def _write(tmp_path, name, value, *, raw=None):
    path = tmp_path / name
    if raw is not None:
        path.write_bytes(raw)
    else:
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _simulate_to_file(tmp_path, capsys, scenario, result_name="result.json", mutate=None):
    scenario_path = _write(tmp_path, "scenario.json", scenario)
    code = main(["simulate", scenario_path])
    assert code == 0
    out, err = capsys.readouterr()
    assert err == ""
    result = json.loads(out)
    if mutate is not None:
        mutate(result)
    result_path = _write(tmp_path, result_name, result)
    return scenario_path, result_path, result


def _replay(capsys, scenario_path, result_path):
    code = main(["replay", scenario_path, result_path])
    out, err = capsys.readouterr()
    return code, out, err


# -- structural comparison unit rules ---------------------------------------


def test_numbers_compare_numerically():
    assert _first_difference(1, 1.0, []) is None
    assert _first_difference(1.5, 1.5, []) is None
    assert _first_difference({"k": 1}, {"k": 1.0}, []) is None


def test_boolean_is_not_a_number():
    assert _first_difference(True, 1, []) == ""
    assert _first_difference(False, 0.0, []) == ""
    assert _first_difference([True], [1], []) == "/0"
    assert _first_difference(True, True, []) is None


def test_scalars_of_different_json_types_differ():
    assert _first_difference("1", 1, []) == ""
    assert _first_difference(None, 0, []) == ""
    assert _first_difference(None, False, []) == ""
    assert _first_difference([], {}, []) == ""
    assert _first_difference(None, None, []) is None
    assert _first_difference("x", "x", []) is None


def test_strings_compare_by_exact_unicode_content():
    nfc = "caf\u00e9"        # é as a single code point (NFC)
    nfd = "cafe\u0301"       # e + combining acute accent (NFD)
    assert nfc != nfd
    assert _first_difference(nfc, nfc, []) is None
    assert _first_difference(nfc, nfd, []) == ""
    assert _first_difference({"\u540d": 1}, {"\u540d": 1}, []) is None


def test_object_member_order_is_irrelevant():
    assert _first_difference({"a": 1, "b": 2}, {"b": 2, "a": 1}, []) is None


def test_object_keys_visited_in_code_point_order():
    # Both members differ; the smallest code point wins the first-difference.
    assert _first_difference({"a": 1, "b": 2}, {"a": 9, "b": 8}, []) == "/a"
    assert _first_difference({"b": 2, "a": 1}, {"b": 8, "a": 9}, []) == "/a"
    # The key present on only one side is itself the difference.
    assert _first_difference({"a": 1}, {"b": 1}, []) == "/a"


def test_arrays_compare_by_ascending_index():
    assert _first_difference([1, 2, 3], [1, 9, 3], []) == "/1"
    assert _first_difference([1, 2], [1, 2, 3], []) == "/2"
    assert _first_difference([1, 2, 3], [1, 2], []) == "/2"


def test_rfc6901_escaping_of_reference_tokens():
    assert _first_difference({"a/b": 1}, {"a/b": 2}, []) == "/a~1b"
    assert _first_difference({"a~b": 1}, {"a~b": 2}, []) == "/a~0b"
    assert _first_difference({"~1": 1}, {"~1": 2}, []) == "/~01"


def test_nested_pointer_combines_tokens():
    expected = {"o": {"list": [{"k": 1}]}}
    actual = {"o": {"list": [{"k": 2}]}}
    assert _first_difference(expected, actual, []) == "/o/list/0/k"


# -- end-to-end matched case ------------------------------------------------


def test_replay_matches_saved_simulation(tmp_path, capsys):
    scenario_path, result_path, _ = _simulate_to_file(
        tmp_path, capsys, _base_scenario()
    )
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 0
    assert err == ""
    assert out == '{"status":"matched"}\n'


def test_replay_matches_full_featured_scenario(tmp_path, capsys):
    scenario = _base_scenario(
        duration=900,
        snapshotThreshold=3,
        initialMembers=["a", "b", "c"],
        faults=[
            {"time": 120, "action": "partition", "groups": [["a"], ["b", "c"]]},
            {"time": 300, "action": "heal"},
        ],
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": {"k": "v"}},
            {"time": 210, "node": "a", "id": "x2", "command": "two"},
            {"time": 220, "node": "a", "id": "x3", "command": "three"},
        ],
        nodeEvents=[{"time": 400, "node": "c", "action": "crash"}],
        membershipChanges=[
            {"time": 250, "node": "a", "id": "m1", "action": "remove", "member": "c"}
        ],
        readQueries=[{"time": 350, "node": "a", "id": "r1"}],
        livenessChecks=[
            {"id": "l1", "type": "leaderElected", "startTime": 0, "deadline": 500}
        ],
    )
    scenario_path, result_path, _ = _simulate_to_file(tmp_path, capsys, scenario)
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 0
    assert out == '{"status":"matched"}\n'
    assert err == ""


def test_replay_matches_despite_shuffled_object_member_order(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _write(tmp_path, "scenario.json", scenario)
    code = main(["simulate", scenario_path])
    assert code == 0
    out, _ = capsys.readouterr()
    result = json.loads(out)
    shuffled = {key: result[key] for key in reversed(list(result))}
    result_path = _write(tmp_path, "result.json", shuffled)
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 0
    assert out == '{"status":"matched"}\n'
    assert err == ""


def test_replay_matches_when_number_text_differs_but_value_equal(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", _base_scenario())
    code = main(["simulate", scenario_path])
    assert code == 0
    out, _ = capsys.readouterr()
    # Rewrite an integer field as a decimal with equal numeric value; the
    # compared JSON values must still be equal.
    text = out.replace('"term": 1', '"term": 1.0', 1)
    result_path = tmp_path / "result.json"
    result_path.write_text(text, encoding="utf-8")
    code, out, err = _replay(capsys, scenario_path, str(result_path))
    assert code == 0
    assert out == '{"status":"matched"}\n'
    assert err == ""


# -- end-to-end mismatches --------------------------------------------------


def test_replay_reports_changed_scalar_with_path_and_values(tmp_path, capsys):
    def mutate(result):
        result["nodes"]["a"]["role"] = "follower"

    scenario_path, result_path, result = _simulate_to_file(
        tmp_path, capsys, _base_scenario(), mutate=mutate
    )
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    assert err == ""
    report = json.loads(out)
    assert report["status"] == "mismatched"
    assert report["path"] == "/nodes/a/role"
    assert report["expectedPresent"] is True
    assert report["actualPresent"] is True
    assert report["expected"] == "leader"
    assert report["actual"] == "follower"


def test_replay_reports_missing_top_level_field(tmp_path, capsys):
    def mutate(result):
        del result["timeline"]

    scenario_path, result_path, _ = _simulate_to_file(
        tmp_path, capsys, _base_scenario(), mutate=mutate
    )
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    report = json.loads(out)
    assert report["status"] == "mismatched"
    assert report["path"] == "/timeline"
    assert report["expectedPresent"] is True
    assert report["actualPresent"] is False
    assert "actual" not in report
    assert isinstance(report["expected"], list)


def test_replay_reports_unknown_top_level_field(tmp_path, capsys):
    def mutate(result):
        result["extra"] = {"nested": [1, 2]}

    scenario_path, result_path, _ = _simulate_to_file(
        tmp_path, capsys, _base_scenario(), mutate=mutate
    )
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    report = json.loads(out)
    assert report["status"] == "mismatched"
    assert report["path"] == "/extra"
    assert report["expectedPresent"] is False
    assert report["actualPresent"] is True
    assert report["actual"] == {"nested": [1, 2]}
    assert "expected" not in report


def test_replay_reports_truncated_array_at_first_missing_index(tmp_path, capsys):
    def mutate(result):
        result["timeline"].pop()

    scenario_path, result_path, result = _simulate_to_file(
        tmp_path, capsys, _base_scenario(), mutate=mutate
    )
    expected_length = len(result["timeline"]) + 1
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    report = json.loads(out)
    assert report["path"] == f"/timeline/{expected_length - 1}"
    assert report["expectedPresent"] is True
    assert report["actualPresent"] is False
    assert "actual" not in report


def test_replay_reports_extra_array_element(tmp_path, capsys):
    def mutate(result):
        result["timeline"].append(dict(result["timeline"][-1]))

    scenario_path, result_path, result = _simulate_to_file(
        tmp_path, capsys, _base_scenario(), mutate=mutate
    )
    extra_index = len(result["timeline"]) - 1
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    report = json.loads(out)
    assert report["path"] == f"/timeline/{extra_index}"
    assert report["expectedPresent"] is False
    assert report["actualPresent"] is True
    assert "expected" not in report
    assert report["actual"] == result["timeline"][-1]


def test_replay_reports_null_value_when_present(tmp_path, capsys):
    def mutate(result):
        result["nodes"]["b"]["knownLeader"] = "someone"

    scenario_path, result_path, _ = _simulate_to_file(
        tmp_path, capsys, _base_scenario(duration=10), mutate=mutate
    )
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    report = json.loads(out)
    assert report["path"] == "/nodes/b/knownLeader"
    assert report["expectedPresent"] is True
    assert report["actualPresent"] is True
    assert report["expected"] is None
    assert report["actual"] == "someone"


def test_replay_reports_boolean_vs_number_type_mismatch(tmp_path, capsys):
    scenario = _base_scenario(nodeEvents=[])

    def mutate(result):
        result["nodes"]["a"]["online"] = 1

    scenario_path, result_path, _ = _simulate_to_file(
        tmp_path, capsys, scenario, mutate=mutate
    )
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    report = json.loads(out)
    assert report["path"] == "/nodes/a/online"
    assert report["expected"] is True
    assert report["actual"] == 1


def test_replay_escapes_slash_in_node_name_key(tmp_path, capsys):
    scenario = _base_scenario(
        nodes=["a/b", "c", "d"],
        electionTimeouts={"a/b": 100, "c": 150, "d": 200},
    )

    def mutate(result):
        result["nodes"]["a/b"]["term"] = 99

    scenario_path, result_path, _ = _simulate_to_file(
        tmp_path, capsys, scenario, mutate=mutate
    )
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    report = json.loads(out)
    assert report["path"] == "/nodes/a~1b/term"
    assert report["expected"] == 1
    assert report["actual"] == 99


def test_replay_detects_result_from_a_different_scenario(tmp_path, capsys):
    # Save the result of a rich scenario ...
    rich = _base_scenario(
        duration=400,
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": "one"}
        ],
    )
    _, result_path, saved = _simulate_to_file(tmp_path, capsys, rich, result_name="rich.json")
    # ... then replay it against a different scenario.
    other_path = _write(tmp_path, "other.json", _base_scenario(duration=50))
    code, out, err = _replay(capsys, other_path, result_path)
    assert code == 1
    assert err == ""
    report = json.loads(out)
    assert report["status"] == "mismatched"
    assert report["path"]  # some concrete first difference
    assert saved  # sanity: the saved result really did contain events
    assert saved["timeline"]


def test_replay_empty_object_result_is_mismatched_not_an_error(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", _base_scenario())
    result_path = _write(tmp_path, "result.json", {})
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 1
    assert err == ""
    report = json.loads(out)
    assert report["status"] == "mismatched"
    # Top-level keys are visited in code-point order: clients comes first.
    assert report["path"] == "/clients"
    assert report["expectedPresent"] is True
    assert report["actualPresent"] is False


# -- determinism -------------------------------------------------------------


def test_replay_output_is_byte_identical_across_runs(tmp_path, capsys):
    scenario_path, result_path, _ = _simulate_to_file(
        tmp_path, capsys, _base_scenario(duration=800)
    )
    code1, out1, err1 = _replay(capsys, scenario_path, result_path)
    code2, out2, err2 = _replay(capsys, scenario_path, result_path)
    assert (code1, out1, err1) == (code2, out2, err2)
    assert out1 == '{"status":"matched"}\n'

    def mutate(result):
        result["duration_anchor"] = True
        result.pop("logMatching")

    scenario_path2, result_path2, _ = _simulate_to_file(
        tmp_path, capsys, _base_scenario(duration=800), result_name="r2.json", mutate=mutate
    )
    _, out3, _ = _replay(capsys, scenario_path2, result_path2)
    _, out4, _ = _replay(capsys, scenario_path2, result_path2)
    assert out3 == out4
    assert json.loads(out3)["status"] == "mismatched"


# -- error handling: exit code 2 ---------------------------------------------


def test_replay_missing_scenario_file(capsys, tmp_path):
    result_path = _write(tmp_path, "result.json", {})
    code = main(["replay", "/nonexistent/scenario.json", result_path])
    out, err = capsys.readouterr()
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_replay_missing_result_file(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", _base_scenario())
    code, out, err = _replay(capsys, scenario_path, "/nonexistent/result.json")
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_replay_non_utf8_scenario(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", None, raw=b"\xff\xfe{}")
    result_path = _write(tmp_path, "result.json", {})
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_replay_non_utf8_result(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", _base_scenario())
    result_path = _write(tmp_path, "result.json", None, raw=b"{\xff}")
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_replay_syntax_error_in_scenario(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", None, raw=b"{not json")
    result_path = _write(tmp_path, "result.json", {})
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_replay_syntax_error_in_result(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", _base_scenario())
    result_path = _write(tmp_path, "result.json", None, raw=b"[1, 2")
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_replay_invalid_scenario_constraints(tmp_path, capsys):
    bad = _base_scenario(nodes=["a", "b"])
    scenario_path = _write(tmp_path, "scenario.json", bad)
    result_path = _write(tmp_path, "result.json", {})
    code, out, err = _replay(capsys, scenario_path, result_path)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_replay_result_top_level_scalar_is_an_error(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", _base_scenario())
    for literal in (b"[]", b"5", b'"hello"', b"null", b"true"):
        result_path = _write(tmp_path, "result.json", None, raw=literal)
        code, out, err = _replay(capsys, scenario_path, result_path)
        assert code == 2, literal
        assert out == "", literal
        assert err.startswith("error: "), literal
        assert err.count("\n") == 1, literal


def test_replay_rejects_nonstandard_json_numbers(tmp_path, capsys):
    scenario_path = _write(tmp_path, "scenario.json", _base_scenario())
    for literal in (b'{"x": NaN}', b'{"x": Infinity}', b'{"x": -Infinity}'):
        result_path = _write(tmp_path, "result.json", None, raw=literal)
        code, out, err = _replay(capsys, scenario_path, result_path)
        assert code == 2, literal
        assert out == "", literal
        assert err.startswith("error: "), literal
        assert err.count("\n") == 1, literal


def test_replay_does_not_mutate_files(tmp_path, capsys):
    scenario_path, result_path, _ = _simulate_to_file(
        tmp_path, capsys, _base_scenario()
    )
    before_s = (tmp_path / "scenario.json").read_bytes()
    before_r = (tmp_path / "result.json").read_bytes()
    code, _, _ = _replay(capsys, scenario_path, result_path)
    assert code == 0
    assert (tmp_path / "scenario.json").read_bytes() == before_s
    assert (tmp_path / "result.json").read_bytes() == before_r
