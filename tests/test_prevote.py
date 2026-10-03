"""Tests for the optional preVote (pre-vote) election phase."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main


def _write_json(tmp_path, name, value):
    path = tmp_path / name
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


def _write_scenario(tmp_path, scenario):
    return _write_json(tmp_path, "scenario.json", scenario)


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


def _run_simulate(tmp_path, capsys, scenario):
    path = _write_scenario(tmp_path, scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _by_type(result, event_type):
    return [e for e in result["timeline"] if e["type"] == event_type]


def _message_events(result, kind):
    return [e for e in result["timeline"] if e.get("message") == kind]


# -- feature gating and backward compatibility ------------------------------


def test_omitted_and_false_pre_vote_are_byte_identical(tmp_path, capsys):
    _, out_plain, _ = _run_simulate(tmp_path, capsys, _base_scenario())
    code, out_false, err = _run_simulate(tmp_path, capsys, _base_scenario(preVote=False))
    assert code == 0 and err == ""
    assert out_false == out_plain


def test_pre_vote_scenario_is_deterministic(tmp_path, capsys):
    scenario = _base_scenario(
        preVote=True,
        faults=[{"time": 300, "action": "partition", "groups": [["a"], ["b", "c"]]},
                {"time": 400, "action": "heal"}],
    )
    _, out1, _ = _run_simulate(tmp_path, capsys, scenario)
    _, out2, _ = _run_simulate(tmp_path, capsys, scenario)
    assert out1 == out2


@pytest.mark.parametrize("value", [0, 1, "true", "false", None, [], {}])
def test_pre_vote_must_be_a_boolean(tmp_path, capsys, value):
    code, out, err = _run_simulate(tmp_path, capsys, _base_scenario(preVote=value))
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


@pytest.mark.parametrize("kind", ["preVote", "preVoteReply"])
@pytest.mark.parametrize("enabled", [None, False])
def test_pre_vote_message_faults_require_the_feature(tmp_path, capsys, kind, enabled):
    overrides = {}
    if enabled is not None:
        overrides["preVote"] = enabled
    scenario = _base_scenario(
        messageFaults=[{"from": "a", "to": "b", "message": kind,
                        "occurrence": 1, "action": "drop"}],
        **overrides,
    )
    code, out, err = _run_simulate(tmp_path, capsys, scenario)
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


# -- protocol flow ------------------------------------------------------------


def test_pre_vote_elects_leader_without_inflating_terms(tmp_path, capsys):
    code, out, err = _run_simulate(tmp_path, capsys, _base_scenario(preVote=True))
    assert code == 0 and err == ""
    result = json.loads(out)

    # "a" opens round 1 for prospectiveTerm 1 without touching its term.
    pre_candidate = [
        e for e in _by_type(result, "stateChange") if e["role"] == "preCandidate"
    ]
    assert pre_candidate == [
        {"seq": pre_candidate[0]["seq"], "time": 100, "type": "stateChange",
         "node": "a", "term": 0, "role": "preCandidate",
         "reason": "electionTimeout", "round": 1, "prospectiveTerm": 1}
    ]

    # Requests carry the current (unincremented) term, the prospective term
    # and the round; both peers grant.
    requests = [e for e in _message_events(result, "preVote") if e["type"] == "messageSend"]
    assert [(e["node"], e["peer"], e["term"], e["round"], e["prospectiveTerm"]) for e in requests] == [
        ("a", "b", 0, 1, 1),
        ("a", "c", 0, 1, 1),
    ]
    replies = _message_events(result, "preVoteReply")
    assert {e["detail"] for e in replies if e["type"] == "messageResult"} <= {
        "granted", "staleRound"
    }

    # The term only rises once the pre-vote majority holds.
    candidate = next(
        e for e in _by_type(result, "stateChange") if e["role"] == "candidate"
    )
    assert candidate["node"] == "a"
    assert candidate["term"] == 1
    assert candidate["reason"] == "preVoteMajority"

    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}
    assert result["nodes"]["a"]["role"] == "leader"
    assert result["nodes"]["a"]["term"] == 1
    assert result["nodes"]["b"]["knownLeader"] == "a"
    seqs = [entry["seq"] for entry in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def test_isolated_node_cannot_disrupt_a_stable_leader(tmp_path, capsys):
    scenario = _base_scenario(
        duration=1200,
        faults=[
            {"time": 300, "action": "partition", "groups": [["a", "b"], ["c"]]},
            {"time": 900, "action": "heal"},
        ],
    )
    _, out_baseline, _ = _run_simulate(tmp_path, capsys, scenario)
    baseline = json.loads(out_baseline)
    # Without pre-vote the isolated node keeps raising its term and, after
    # the heal, unseats the stable leader with its inflated term.
    assert "c" in {
        node for names in baseline["electionSafety"]["leadersByTerm"].values() for node in names
    }
    assert baseline["nodes"]["c"]["term"] > 1

    code, out, err = _run_simulate(tmp_path, capsys, {**scenario, "preVote": True})
    assert code == 0 and err == ""
    result = json.loads(out)
    # With pre-vote the isolated node's term never inflates (its pre-votes
    # are dropped by the partition), and after the heal it simply rejoins.
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}
    assert result["nodes"]["a"]["role"] == "leader"
    assert result["nodes"]["a"]["term"] == 1
    assert result["nodes"]["c"]["term"] == 1
    assert result["nodes"]["c"]["role"] == "follower"


def test_healthy_nodes_reject_pre_vote_before_their_deadline(tmp_path, capsys):
    # Heal just before "c" opens a new round and delay the first post-heal
    # heartbeat so the preVote reaches the healthy side while its leader
    # contact is still fresh.
    scenario = _base_scenario(
        duration=1400,
        electionTimeouts={"a": 100, "b": 150, "c": 250},
        preVote=True,
        faults=[
            {"time": 300, "action": "partition", "groups": [["a", "b"], ["c"]]},
            {"time": 995, "action": "heal"},
        ],
        messageFaults=[
            {"from": "a", "to": "c", "message": "heartbeat", "occurrence": 18,
             "action": "delay", "delay": 60},
        ],
    )
    code, out, err = _run_simulate(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)

    received = [
        e for e in _message_events(result, "preVote")
        if e["type"] == "messageResult" and e["result"] == "delivered" and e["time"] >= 990
    ]
    assert received, "expected the healed node's preVote to reach the majority side"
    assert {e["detail"] for e in received} == {"rejected"}
    # Rejections must not disturb the receivers' terms or the stable leader.
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}
    assert result["nodes"]["a"]["role"] == "leader"
    assert result["nodes"]["a"]["term"] == 1
    assert result["nodes"]["b"]["term"] == 1


def test_stale_round_reply_is_ignored(tmp_path, capsys):
    # Round 1 of "a" cannot succeed (c's reply is dropped, b's is delayed
    # past the start of a's next round); the late reply is stale.
    scenario = _base_scenario(
        duration=600,
        preVote=True,
        messageFaults=[
            {"from": "b", "to": "a", "message": "preVoteReply", "occurrence": 1,
             "action": "delay", "delay": 150},
            {"from": "c", "to": "a", "message": "preVoteReply", "occurrence": 1,
             "action": "drop"},
        ],
    )
    code, out, err = _run_simulate(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)

    replies = [
        e for e in _message_events(result, "preVoteReply")
        if e["type"] == "messageResult" and e["node"] == "a"
    ]
    stale = [e for e in replies if e.get("detail") == "staleRound"]
    assert stale, "expected the delayed round-1 reply to be reported staleRound"
    assert all(e["round"] == 1 for e in stale)
    # The delayed reply never counts: "a" is not the term-1 leader.
    assert result["electionSafety"]["leadersByTerm"] == {"1": ["b"]}
    assert result["electionSafety"]["violations"] == []


def test_dropped_pre_votes_prevent_any_election(tmp_path, capsys):
    # Round 1 of every node (and a's round 2 at 200) is dropped, so within
    # the duration no preVote is ever delivered.
    drops = [
        {"from": src, "to": dst, "message": "preVote", "occurrence": 1, "action": "drop"}
        for src in ("a", "b", "c")
        for dst in ("a", "b", "c")
        if src != dst
    ]
    drops += [
        {"from": "a", "to": dst, "message": "preVote", "occurrence": 2, "action": "drop"}
        for dst in ("b", "c")
    ]
    scenario = _base_scenario(duration=250, preVote=True, messageFaults=drops)
    code, out, err = _run_simulate(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    # Nobody ever collects a pre-vote majority, so nobody even becomes a
    # candidate and every term stays 0.
    assert result["electionSafety"] == {"leadersByTerm": {}, "violations": []}
    assert all(node["term"] == 0 for node in result["nodes"].values())
    assert all(node["role"] == "preCandidate" for node in result["nodes"].values())


def test_restart_clears_pre_vote_state(tmp_path, capsys):
    scenario = _base_scenario(
        duration=900,
        preVote=True,
        faults=[{"time": 250, "action": "partition", "groups": [["a", "b"], ["c"]]}],
        nodeEvents=[
            {"time": 350, "node": "c", "action": "crash"},
            {"time": 500, "node": "c", "action": "restart"},
        ],
    )
    code, out, err = _run_simulate(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)

    rounds = [
        e["round"]
        for e in _by_type(result, "stateChange")
        if e["node"] == "c" and e["role"] == "preCandidate"
    ]
    # Round numbering restarts at 1 after the restart: pre-vote state is
    # not persisted.
    assert rounds and rounds[0] == 1
    assert rounds == sorted(rounds)
    post_restart = [
        e for e in _by_type(result, "stateChange")
        if e["node"] == "c" and e["role"] == "preCandidate" and e["time"] > 500
    ]
    assert post_restart and post_restart[0]["round"] == 1
    # A preCandidate final state is legal and is not an elected leader.
    assert result["electionSafety"]["violations"] == []


def test_pre_candidate_is_not_a_leader_for_liveness(tmp_path, capsys):
    # No preVote is ever delivered, so every node stays a preCandidate; a
    # preCandidate must not count as an elected leader.
    drops = [
        {"from": src, "to": dst, "message": "preVote", "occurrence": 1, "action": "drop"}
        for src in ("a", "b", "c")
        for dst in ("a", "b", "c")
        if src != dst
    ]
    drops += [
        {"from": "a", "to": dst, "message": "preVote", "occurrence": 2, "action": "drop"}
        for dst in ("b", "c")
    ]
    scenario = _base_scenario(
        duration=250,
        preVote=True,
        messageFaults=drops,
        livenessChecks=[{"id": "chk", "type": "leaderElected", "startTime": 0, "deadline": 250}],
    )
    code, out, err = _run_simulate(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    assert result["liveness"]["checks"][0]["status"] == "failed"
    assert result["liveness"]["violations"]


def test_learner_neither_starts_nor_answers_pre_votes(tmp_path, capsys):
    scenario = _base_scenario(
        nodes=["a", "b", "c", "d"],
        electionTimeouts={"a": 100, "b": 150, "c": 200, "d": 120},
        initialMembers=["a", "b", "c"],
        membershipChanges=[],
        preVote=True,
    )
    code, out, err = _run_simulate(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    result = json.loads(out)
    pre_vote_traffic = _message_events(result, "preVote") + _message_events(result, "preVoteReply")
    assert pre_vote_traffic, "expected pre-vote traffic among the voters"
    assert all(e["node"] != "d" and e.get("peer") != "d" for e in pre_vote_traffic)
    assert result["nodes"]["d"]["role"] == "follower"
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}


# -- explore and replay integration -------------------------------------------


def test_explore_enumerates_pre_vote_faults(tmp_path, capsys):
    scenario = _base_scenario(preVote=True)
    plan = {
        "scenario": scenario,
        "candidates": [
            {"from": "b", "to": "a", "message": "preVoteReply", "occurrence": 1, "action": "drop"},
            {"from": "c", "to": "a", "message": "preVoteReply", "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 2,
        "maxCases": 10,
    }
    path = _write_json(tmp_path, "plan.json", plan)
    code = main(["explore", path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    result = json.loads(out)
    assert result["totalCases"] == 4
    assert result["passedCases"] + result["failedCases"] == 4
    # Dropping both of a's round-1 preVoteReply messages keeps "a" from
    # winning round 1; the run stays safe regardless.
    assert all(case["status"] == "passed" for case in result["cases"])


def test_explore_rejects_pre_vote_candidates_without_the_feature(tmp_path, capsys):
    plan = {
        "scenario": _base_scenario(),
        "candidates": [
            {"from": "a", "to": "b", "message": "preVote", "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 1,
        "maxCases": 5,
    }
    path = _write_json(tmp_path, "plan.json", plan)
    code = main(["explore", path])
    out, err = capsys.readouterr()
    assert code == 2
    assert out == ""
    assert err.startswith("error: ")
    assert err.count("\n") == 1


def test_replay_round_trip_with_pre_vote(tmp_path, capsys):
    scenario = _base_scenario(
        preVote=True,
        messageFaults=[
            {"from": "b", "to": "a", "message": "preVoteReply", "occurrence": 1,
             "action": "delay", "delay": 150},
        ],
    )
    scenario_path = _write_scenario(tmp_path, scenario)
    code = main(["simulate", scenario_path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    result_path = _write_json(tmp_path, "result.json", json.loads(out))

    code = main(["replay", scenario_path, result_path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    assert json.loads(out) == {"status": "matched"}
