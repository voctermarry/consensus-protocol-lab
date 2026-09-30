"""Tests for the consensus-protocol-lab command line interface."""

from __future__ import annotations

import json

from consensus_lab.cli import main

from .test_simulator import scenario


def write_scenario(path, raw):
    path.write_text(json.dumps(raw), encoding="utf-8")


def test_version(capsys):
    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == "0.1.0"


def test_no_args_prints_help(capsys):
    assert main([]) == 0
    assert "usage" in capsys.readouterr().out


def test_simulate_success(tmp_path, capsys):
    path = tmp_path / "scenario.json"
    write_scenario(path, scenario())

    assert main(["simulate", str(path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["nodes"]["n1"]["role"] == "leader"
    assert result["electionSafety"]["violations"] == []


def test_simulate_invalid_json(tmp_path, capsys):
    path = tmp_path / "scenario.json"
    path.write_text("{not json", encoding="utf-8")

    assert main(["simulate", str(path)]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("error: ")


def test_simulate_invalid_scenario(tmp_path, capsys):
    path = tmp_path / "scenario.json"
    write_scenario(path, scenario(nodes=["only", "two"]))

    assert main(["simulate", str(path)]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("error: ")
    assert captured.err.count("\n") == 1


def test_simulate_missing_file(capsys):
    assert main(["simulate", "/nonexistent/consensus-lab-scenario.json"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("error: ")


def test_simulate_does_not_create_files(tmp_path):
    path = tmp_path / "scenario.json"
    write_scenario(path, scenario())
    before = set(tmp_path.iterdir())

    assert main(["simulate", str(path)]) == 0
    assert set(tmp_path.iterdir()) == before
