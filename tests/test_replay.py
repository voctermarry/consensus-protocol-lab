"""Tests for the read-only replay subcommand."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main
from consensus_lab.replay import _diff, run_replay
from consensus_lab.simulate import ScenarioError


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
        path.write_text(raw, encoding="utf-8")
    else:
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _simulate(tmp_path, scenario, name="scenario.json"):
    path = _write(tmp_path, name, scenario)
    return path


def _replay(tmp_path, capsys, scenario_path, result_path):
    code = main(["replay", str(scenario_path), str(result_path)])
    out, err = capsys.readouterr()
    return code, out, err


def _saved_result(tmp_path, scenario, *, name="result.json", mutate=None, raw=None):
    """Run the simulator for a scenario and persist its result."""
    result = run_simulation_result(scenario)
    if mutate is not None:
        mutate(result)
    return _write(tmp_path, name, result, raw=raw)


def run_simulation_result(scenario):
    from consensus_lab.simulate import run_simulation

    return run_simulation(scenario)


# -- happy path -------------------------------------------------------------


def test_replay_matches_saved_simulation(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)
    result_path = _saved_result(tmp_path, scenario)
    code, out, err = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 0
    assert err == ""
    assert out == '{"status":"matched"}\n'


def test_replay_output_is_byte_identical(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": "café"},
        ]
    )
    scenario_path = _simulate(tmp_path, scenario)
    result_path = _saved_result(tmp_path, scenario)
    _, out1, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    _, out2, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert out1 == out2


def test_mismatch_output_is_byte_identical_despite_member_order(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)
    result = run_simulation_result(scenario)
    result["nodes"]["a"]["extra"] = {
        "zz": 1,
        "aa": {"b": [1, 2], "a": 0},
        "中": [{"y": 1, "x": 2}],
    }

    def reverse_order(value):
        if isinstance(value, dict):
            return {k: reverse_order(value[k]) for k in reversed(list(value))}
        if isinstance(value, list):
            return [reverse_order(item) for item in value]
        return value

    path1 = _write(tmp_path, "r1.json", reverse_order(result))
    path2 = _write(tmp_path, "r2.json", result)
    _, out1, _ = _replay(tmp_path, capsys, scenario_path, path1)
    _, out2, _ = _replay(tmp_path, capsys, scenario_path, path2)
    assert out1 == out2
    verdict = json.loads(out1)
    assert verdict["path"] == "/nodes/a/extra"
    assert verdict["actual"] == result["nodes"]["a"]["extra"]


def test_replay_matches_full_feature_scenario(tmp_path, capsys):
    scenario = _base_scenario(
        nodes=["a", "b", "c", "d"],
        duration=900,
        electionTimeouts={"a": 100, "b": 150, "c": 200, "d": 250},
        snapshotThreshold=3,
        initialMembers=["a", "b", "c"],
        membershipChanges=[
            {"time": 200, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        clientCommands=[
            {"time": 150, "node": "a", "id": "x1", "command": "one"},
            {"time": 160, "node": "a", "id": "x2", "command": "two"},
        ],
        readQueries=[{"time": 300, "node": "a", "id": "r1"}],
        livenessChecks=[
            {"id": "l1", "type": "leaderElected", "startTime": 0, "deadline": 200},
            {"id": "l2", "type": "clientCommitted", "startTime": 0, "deadline": 400, "target": "x1"},
            {"id": "l3", "type": "readCompleted", "startTime": 0, "deadline": 500, "target": "r1"},
            {"id": "l4", "type": "membershipCommitted", "startTime": 0, "deadline": 700, "target": "m1"},
        ],
    )
    scenario_path = _simulate(tmp_path, scenario)
    result_path = _saved_result(tmp_path, scenario)
    code, out, err = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 0, err
    assert out == '{"status":"matched"}\n'


# -- mismatch verdicts ------------------------------------------------------


def test_missing_top_level_field(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)

    def drop_clients(result):
        del result["clients"]

    result_path = _saved_result(tmp_path, scenario, mutate=drop_clients)
    code, out, err = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    assert err == ""
    verdict = json.loads(out)
    assert verdict["status"] == "mismatched"
    assert verdict["path"] == "/clients"
    assert verdict["expectedPresent"] is True
    assert verdict["actualPresent"] is False
    assert "expected" in verdict
    assert "actual" not in verdict


def test_unknown_top_level_field(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)

    def add_extra(result):
        result["extra"] = 1

    result_path = _saved_result(tmp_path, scenario, mutate=add_extra)
    code, out, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    verdict = json.loads(out)
    assert verdict["path"] == "/extra"
    assert verdict["expectedPresent"] is False
    assert verdict["actualPresent"] is True
    assert verdict["actual"] == 1
    assert "expected" not in verdict


def test_different_scenario_reports_mismatched_not_error(tmp_path, capsys):
    scenario_a = _base_scenario(duration=500)
    scenario_b = _base_scenario(duration=200)
    scenario_path = _simulate(tmp_path, scenario_b, name="scenario_b.json")
    result_path = _saved_result(tmp_path, scenario_a)
    code, out, err = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    assert err == ""
    verdict = json.loads(out)
    assert verdict["status"] == "mismatched"
    assert verdict["path"].startswith("/timeline/")


def test_timeline_scalar_change_locates_array_index(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)

    def tweak(result):
        result["timeline"][3]["node"] = "zzz"

    result_path = _saved_result(tmp_path, scenario, mutate=tweak)
    code, out, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    verdict = json.loads(out)
    assert verdict["path"] == "/timeline/3/node"
    assert verdict["expected"] == "a" or verdict["expected"] in {"b", "c"}
    assert verdict["actual"] == "zzz"


def test_truncated_timeline_points_at_first_missing_index(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)
    full = run_simulation_result(scenario)

    def truncate(result):
        result["timeline"] = result["timeline"][:2]

    result_path = _saved_result(tmp_path, scenario, mutate=truncate)
    code, out, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    verdict = json.loads(out)
    assert verdict["path"] == "/timeline/2"
    assert verdict["expectedPresent"] is True
    assert verdict["actualPresent"] is False
    assert verdict["expected"] == full["timeline"][2]
    assert "actual" not in verdict


def test_array_element_order_matters(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)

    def swap(result):
        result["timeline"][0], result["timeline"][1] = (
            result["timeline"][1],
            result["timeline"][0],
        )

    result_path = _saved_result(tmp_path, scenario, mutate=swap)
    code, out, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    assert json.loads(out)["path"].startswith("/timeline/0")


def test_object_member_order_is_insignificant(tmp_path, capsys):
    scenario = _base_scenario(
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": {"k": "v"}}
        ]
    )
    scenario_path = _simulate(tmp_path, scenario)
    result = run_simulation_result(scenario)

    def reverse_order(value):
        if isinstance(value, dict):
            return {k: reverse_order(value[k]) for k in reversed(list(value))}
        if isinstance(value, list):
            return [reverse_order(item) for item in value]
        return value

    result_path = _write(tmp_path, "result.json", reverse_order(result))
    code, out, err = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 0
    assert out == '{"status":"matched"}\n'
    assert err == ""


def test_numbers_compare_by_value(tmp_path, capsys):
    # Node a wins term 1; writing the term as 1.0 must still match.
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)

    def float_term(result):
        result["nodes"]["a"]["term"] = 1.0

    result_path = _saved_result(tmp_path, scenario, mutate=float_term)
    code, out, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 0
    assert json.loads(out)["status"] == "matched"


def test_boolean_does_not_equal_number(tmp_path, capsys):
    scenario = _base_scenario(
        nodeEvents=[{"time": 400, "node": "c", "action": "crash"}]
    )
    scenario_path = _simulate(tmp_path, scenario)

    def numeric_bool(result):
        result["nodes"]["a"]["online"] = 1

    result_path = _saved_result(tmp_path, scenario, mutate=numeric_bool)
    code, out, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    verdict = json.loads(out)
    assert verdict["path"] == "/nodes/a/online"
    assert verdict["expected"] is True
    assert verdict["actual"] == 1


def test_pointer_escapes_slash_and_tilde_in_node_names(tmp_path, capsys):
    scenario = _base_scenario(
        nodes=["a/b", "b", "c~d"],
        electionTimeouts={"a/b": 100, "b": 150, "c~d": 200},
    )
    scenario_path = _simulate(tmp_path, scenario)

    def extra_field(result):
        result["nodes"]["a/b"]["zzz"] = 1

    result_path = _saved_result(tmp_path, scenario, mutate=extra_field)
    code, out, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    assert json.loads(out)["path"] == "/nodes/a~1b/zzz"


def test_unicode_strings_compare_by_content(tmp_path, capsys):
    # U+00E9 versus the NFD decomposition e + U+0301 are different content.
    nfc = "caf\u00e9"
    nfd = "cafe\u0301"
    scenario = _base_scenario(
        clientCommands=[
            {"time": 200, "node": "a", "id": "x1", "command": nfc}
        ]
    )
    scenario_path = _simulate(tmp_path, scenario)

    def nfd_command(result):
        for entry in result["nodes"]["a"]["log"]:
            if entry["id"] == "x1":
                entry["command"] = nfd

    result_path = _saved_result(tmp_path, scenario, mutate=nfd_command)
    code, out, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 1
    verdict = json.loads(out)
    assert verdict["path"].endswith("/command")
    assert verdict["expected"] == nfc
    assert verdict["actual"] == nfd
    assert verdict["expected"] != verdict["actual"]


# -- direct comparison unit tests ------------------------------------------


def test_diff_root_scalar_difference_uses_empty_pointer():
    assert _diff(1, True, []) == ""
    assert _diff([], {}, []) == ""
    assert _diff(None, 0, []) == ""


def test_diff_objects_iterate_union_by_unicode_codepoint():
    # "a" sorts before "é" (U+00E9), so the mismatch under "a" wins.
    assert _diff({"a": 1, "é": 0}, {"a": 2, "é": 0}, []) == "/a"
    assert _diff({"a": 1, "é": 0}, {"a": 1, "é": 2}, []) == "/é"
    # Missing and extra keys are located regardless of which side holds them.
    assert _diff({"a": 1}, {"a": 1, "b": 2}, []) == "/b"
    assert _diff({"a": 1, "b": 2}, {"a": 1}, []) == "/b"


def test_diff_arrays_compare_by_ascending_index():
    assert _diff([1, 2], [1, 2, 3], []) == "/2"
    assert _diff([1, 2, 3], [1, 2], []) == "/2"
    assert _diff([1, 2, 3], [1, 9, 3], []) == "/1"
    assert _diff([[1]], [[2]], []) == "/0/0"


def test_diff_numeric_and_escaping_rules():
    assert _diff(1, 1.0, []) is None
    assert _diff(1.5, 1.5, []) is None
    assert _diff(True, 1, []) == ""
    assert _diff({"a/b": 1}, {"a/b": 2}, []) == "/a~1b"
    assert _diff({"~": 1}, {"~": 2}, []) == "/~0"
    assert _diff("é", "é", []) is None
    assert _diff("\u00e9", "e\u0301", []) == ""


def test_run_replay_requires_object_result():
    scenario = _base_scenario()
    with pytest.raises(ScenarioError):
        run_replay(scenario, [])


# -- error handling ---------------------------------------------------------


def test_replay_matched_does_not_modify_inputs(tmp_path, capsys):
    scenario = _base_scenario()
    scenario_path = _simulate(tmp_path, scenario)
    result_path = _saved_result(tmp_path, scenario)
    before = (tmp_path / "scenario.json").read_bytes(), (tmp_path / "result.json").read_bytes()
    code, _, _ = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 0
    after = (tmp_path / "scenario.json").read_bytes(), (tmp_path / "result.json").read_bytes()
    assert before == after


@pytest.mark.parametrize("result_value", [[], "x", 5, None, True])
def test_non_object_result_is_an_error(tmp_path, capsys, result_value):
    scenario_path = _simulate(tmp_path, _base_scenario())
    result_path = _write(tmp_path, "result.json", result_value)
    code, out, err = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_missing_scenario_file(tmp_path, capsys):
    result_path = _saved_result(tmp_path, _base_scenario())
    code, out, err = _replay(tmp_path, capsys, tmp_path / "missing.json", result_path)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_missing_result_file(tmp_path, capsys):
    scenario_path = _simulate(tmp_path, _base_scenario())
    code, out, err = _replay(tmp_path, capsys, scenario_path, tmp_path / "missing.json")
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


@pytest.mark.parametrize("which", ["scenario", "result"])
def test_non_utf8_files(tmp_path, capsys, which):
    scenario_path = _simulate(tmp_path, _base_scenario())
    result_path = _saved_result(tmp_path, _base_scenario())
    bad = tmp_path / f"bad-{which}.json"
    bad.write_bytes(b"\xff\xfe{}")
    args = (bad, result_path) if which == "scenario" else (scenario_path, bad)
    code, out, err = _replay(tmp_path, capsys, *args)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


@pytest.mark.parametrize("which", ["scenario", "result"])
def test_invalid_json_files(tmp_path, capsys, which):
    scenario_path = _simulate(tmp_path, _base_scenario())
    result_path = _saved_result(tmp_path, _base_scenario())
    bad = tmp_path / f"bad-{which}.json"
    bad.write_text("{not json", encoding="utf-8")
    args = (bad, result_path) if which == "scenario" else (scenario_path, bad)
    code, out, err = _replay(tmp_path, capsys, *args)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_invalid_scenario_constraints(tmp_path, capsys):
    bad_scenario = _base_scenario(nodes=["a", "b"])
    scenario_path = _simulate(tmp_path, bad_scenario, name="bad-scenario.json")
    result_path = _saved_result(tmp_path, _base_scenario())
    code, out, err = _replay(tmp_path, capsys, scenario_path, result_path)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_existing_commands_unchanged(tmp_path, capsys):
    # version/simulate/explore keep their exit codes and output conventions.
    assert main(["version"]) == 0
    out, _ = capsys.readouterr()
    assert out.strip()
    scenario_path = _simulate(tmp_path, _base_scenario())
    assert main(["simulate", scenario_path]) == 0
    out, err = capsys.readouterr()
    assert err == ""
    assert json.loads(out)["electionSafety"]["violations"] == []
