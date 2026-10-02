"""Tests for the optional messageFaults per-message drop/delay rules."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main


def _write_scenario(tmp_path, scenario):
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(scenario), encoding="utf-8")
    return str(path)


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


def _run(tmp_path, capsys, scenario):
    path = _write_scenario(tmp_path, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _by_type(result, event_type):
    return [e for e in result["timeline"] if e["type"] == event_type]


def test_omitted_empty_and_unmatched_are_byte_identical(tmp_path, capsys):
    plain = _base_scenario()
    _, out_plain, _ = _run(tmp_path, capsys, plain)

    empty = _base_scenario(messageFaults=[])
    code, out_empty, err = _run(tmp_path, capsys, empty)
    assert code == 0 and err == ""
    assert out_empty == out_plain

    unmatched = _base_scenario(messageFaults=[
        {"from": "a", "to": "b", "message": "installSnapshot", "occurrence": 3, "action": "drop"},
        {"from": "c", "to": "a", "message": "heartbeat", "occurrence": 99, "action": "delay", "delay": 5},
    ])
    code, out_unmatched, err = _run(tmp_path, capsys, unmatched)
    assert code == 0 and err == ""
    assert out_unmatched == out_plain


def test_drop_vote_replies_changes_election(tmp_path, capsys):
    scenario = _base_scenario(messageFaults=[
        {"from": "b", "to": "a", "message": "voteReply", "occurrence": 1, "action": "drop"},
        {"from": "c", "to": "a", "message": "voteReply", "occurrence": 1, "action": "drop"},
    ])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)

    faults = _by_type(result, "messageFault")
    assert faults == [
        {"seq": faults[0]["seq"], "time": 110, "type": "messageFault", "rule": 0,
         "from": "b", "to": "a", "message": "voteReply", "occurrence": 1,
         "action": "drop", "scheduledTime": 120},
        {"seq": faults[1]["seq"], "time": 110, "type": "messageFault", "rule": 1,
         "from": "c", "to": "a", "message": "voteReply", "occurrence": 1,
         "action": "drop", "scheduledTime": 120},
    ]
    assert all("arrivalTime" not in f for f in faults)

    dropped = [
        e for e in _by_type(result, "messageResult")
        if e.get("result") == "dropped"
    ]
    assert len(dropped) == 2
    for entry in dropped:
        assert entry["reason"] == "messageFault"
        assert entry["message"] == "voteReply"
        assert entry["node"] == "a"
        assert entry["time"] == 120  # the originally scheduled arrival

    # Both replies were dropped, so "a" never won its term-1 election; it
    # times out again at 200 and wins term 2 instead.
    assert result["electionSafety"]["leadersByTerm"] == {"2": ["a"]}
    assert result["electionSafety"]["violations"] == []
    seqs = [entry["seq"] for entry in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def test_delay_shifts_arrival_and_reorders(tmp_path, capsys):
    scenario = _base_scenario(messageFaults=[
        {"from": "a", "to": "c", "message": "heartbeat", "occurrence": 1,
         "action": "delay", "delay": 60},
    ])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)

    faults = _by_type(result, "messageFault")
    assert len(faults) == 1
    fault = faults[0]
    assert fault["from"] == "a" and fault["to"] == "c"
    assert fault["message"] == "heartbeat" and fault["occurrence"] == 1
    assert fault["action"] == "delay"
    # Sent at 120 (when "a" becomes leader): scheduled 120 + 10, actual + 60.
    assert fault["scheduledTime"] == 130
    assert fault["arrivalTime"] == 190

    sends = [
        e for e in _by_type(result, "messageSend")
        if e["node"] == "a" and e["peer"] == "c" and e["message"] == "heartbeat"
    ]
    assert [e["time"] for e in sends][:2] == [120, 170]
    arrivals = [
        e for e in _by_type(result, "messageResult")
        if e["node"] == "c" and e["message"] == "heartbeat"
    ]
    # The delayed first heartbeat (190) lands after the second one (180):
    # the delay produced out-of-order delivery.
    assert [e["time"] for e in arrivals][:2] == [180, 190]
    assert all(e.get("result") == "delivered" for e in arrivals)


def test_delay_checked_against_node_down_at_arrival(tmp_path, capsys):
    scenario = _base_scenario(
        nodeEvents=[{"time": 150, "node": "c", "action": "crash"}],
        messageFaults=[
            {"from": "a", "to": "c", "message": "heartbeat", "occurrence": 1,
             "action": "delay", "delay": 60},
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    dropped = [
        e for e in _by_type(result, "messageResult")
        if e.get("result") == "dropped"
    ]
    # The heartbeat was sent while "c" was up but arrives after the crash.
    assert any(
        e["node"] == "c" and e["message"] == "heartbeat"
        and e["reason"] == "nodeDown" and e["time"] == 190
        for e in dropped
    )


def test_delay_checked_against_partition_at_arrival(tmp_path, capsys):
    scenario = _base_scenario(
        faults=[{"time": 140, "action": "partition", "groups": [["a", "b"], ["c"]]}],
        messageFaults=[
            {"from": "a", "to": "c", "message": "heartbeat", "occurrence": 1,
             "action": "delay", "delay": 60},
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    dropped = [
        e for e in _by_type(result, "messageResult")
        if e.get("result") == "dropped"
    ]
    assert any(
        e["node"] == "c" and e["message"] == "heartbeat"
        and e["reason"] == "partition" and e["time"] == 190
        for e in dropped
    )


def test_arrival_beyond_duration_keeps_only_send_and_fault(tmp_path, capsys):
    scenario = _base_scenario(
        duration=150,
        messageFaults=[
            {"from": "a", "to": "c", "message": "heartbeat", "occurrence": 1,
             "action": "delay", "delay": 60},
        ],
    )
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    faults = _by_type(result, "messageFault")
    assert len(faults) == 1
    assert faults[0]["scheduledTime"] == 130
    assert faults[0]["arrivalTime"] == 190
    assert not [
        e for e in _by_type(result, "messageResult")
        if e["node"] == "c" and e["message"] == "heartbeat"
    ]


def test_occurrence_counts_actual_sends_per_selector(tmp_path, capsys):
    scenario = _base_scenario(messageFaults=[
        {"from": "a", "to": "c", "message": "heartbeat", "occurrence": 2,
         "action": "drop"},
    ])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    faults = _by_type(result, "messageFault")
    assert len(faults) == 1
    assert faults[0]["occurrence"] == 2
    # The first heartbeat (sent at 120) is delivered; the second (sent at
    # 170, scheduled 180) is dropped at its scheduled arrival time.
    arrivals = [
        e for e in _by_type(result, "messageResult")
        if e["node"] == "c" and e["message"] == "heartbeat"
    ]
    first = arrivals[0]
    assert first["time"] == 130 and first["result"] == "delivered"
    dropped = [e for e in arrivals if e.get("result") == "dropped"]
    assert len(dropped) == 1
    assert dropped[0]["time"] == 180
    assert dropped[0]["reason"] == "messageFault"


def test_determinism_with_message_faults(tmp_path, capsys):
    scenario = _base_scenario(messageFaults=[
        {"from": "b", "to": "a", "message": "voteReply", "occurrence": 1, "action": "drop"},
        {"from": "a", "to": "c", "message": "heartbeat", "occurrence": 1,
         "action": "delay", "delay": 25},
    ])
    _, out1, _ = _run(tmp_path, capsys, scenario)
    _, out2, _ = _run(tmp_path, capsys, scenario)
    assert out1 == out2


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(messageFaults={}),
    lambda s: s.update(messageFaults=[1]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": 1, "action": "drop", "extra": 1}]),
    lambda s: s.update(messageFaults=[{"from": "a", "message": "heartbeat",
                                       "occurrence": 1, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "", "to": "b", "message": "heartbeat",
                                       "occurrence": 1, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "z", "message": "heartbeat",
                                       "occurrence": 1, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "z", "to": "b", "message": "heartbeat",
                                       "occurrence": 1, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "a", "message": "heartbeat",
                                       "occurrence": 1, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "ping",
                                       "occurrence": 1, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": 0, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": -1, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": 1.5, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": True, "action": "drop"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": 1, "action": "lose"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": 1, "action": "drop", "delay": 5}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": 1, "action": "delay"}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": 1, "action": "delay", "delay": -1}]),
    lambda s: s.update(messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                                       "occurrence": 1, "action": "delay", "delay": 1.5}]),
    lambda s: s.update(messageFaults=[
        {"from": "a", "to": "b", "message": "heartbeat", "occurrence": 1, "action": "drop"},
        {"from": "a", "to": "b", "message": "heartbeat", "occurrence": 1,
         "action": "delay", "delay": 5},
    ]),
])
def test_invalid_message_faults(tmp_path, capsys, mutate):
    scenario = _base_scenario()
    mutate(scenario)
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1
