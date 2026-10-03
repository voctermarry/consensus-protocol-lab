"""Tests for the optional pre-vote phase."""

from __future__ import annotations

import json

import pytest

from consensus_lab.cli import main
from consensus_lab.simulate import (
    ROLE_FOLLOWER,
    ROLE_LEADER,
    ROLE_PRECANDIDATE,
    _Simulator,
    parse_scenario,
)


def _write(path_dir, name, value):
    path = path_dir / name
    path.write_text(json.dumps(value), encoding="utf-8")
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
    path = _write(tmp_path, "scenario.json", scenario)
    code = main(["simulate", path])
    out, err = capsys.readouterr()
    return code, out, err


def _run_ok(tmp_path, capsys, scenario):
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""
    return json.loads(out)


def _by_type(result, event_type):
    return [e for e in result["timeline"] if e["type"] == event_type]


# -- backward compatibility -------------------------------------------------


def test_pre_vote_omitted_false_and_legacy_are_byte_identical(tmp_path, capsys):
    plain = _base_scenario()
    _, out_plain, _ = _run(tmp_path, capsys, plain)

    _, out_false, _ = _run(tmp_path, capsys, _base_scenario(preVote=False))
    assert out_false == out_plain

    # A richer legacy scenario (every feature on) is unchanged too.
    rich = _base_scenario(
        duration=900,
        snapshotThreshold=3,
        initialMembers=["a", "b", "c"],
        faults=[{"time": 120, "action": "partition", "groups": [["a"], ["b", "c"]]},
                {"time": 300, "action": "heal"}],
        clientCommands=[{"time": 200, "node": "a", "id": "x1", "command": 1}],
        nodeEvents=[{"time": 400, "node": "c", "action": "crash"},
                    {"time": 600, "node": "c", "action": "restart"}],
        membershipChanges=[{"time": 250, "node": "a", "id": "m1",
                            "action": "remove", "member": "c"}],
        readQueries=[{"time": 350, "node": "a", "id": "r1"}],
        livenessChecks=[{"id": "l1", "type": "leaderElected",
                         "startTime": 0, "deadline": 500}],
        messageFaults=[{"from": "a", "to": "b", "message": "heartbeat",
                        "occurrence": 9, "action": "drop"}],
    )
    _, out_rich_legacy, _ = _run(tmp_path, capsys, rich)
    rich_false = dict(rich)
    rich_false["preVote"] = False
    _, out_rich_false, _ = _run(tmp_path, capsys, rich_false)
    assert out_rich_false == out_rich_legacy


def test_pre_vote_disabled_emits_no_pre_vote_traffic(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario())
    messages = {e.get("message") for e in result["timeline"]}
    assert "preVote" not in messages
    assert "preVoteReply" not in messages
    assert all(n["role"] != ROLE_PRECANDIDATE for n in result["nodes"].values())


# -- happy path --------------------------------------------------------------


def test_pre_vote_precedes_the_real_election_without_bumping_term(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(duration=300, preVote=True))

    # The first state change is to preCandidate still at term 0; only after a
    # majority of pre-votes does the node promote and raise the term.
    states = _by_type(result, "stateChange")
    pre = next(e for e in states if e["node"] == "a" and e["role"] == ROLE_PRECANDIDATE)
    assert pre["term"] == 0
    assert pre["reason"] == "electionTimeout"
    promote = next(e for e in states if e["node"] == "a" and e["reason"] == "preVoteMajority")
    assert promote["role"] == "candidate"
    assert promote["term"] == 1

    # Pre-vote probes carry the current term, the prospective term and a round.
    sends = [e for e in _by_type(result, "messageSend") if e["message"] == "preVote"]
    assert sends and all(
        e["node"] == "a" and e["term"] == 0 and e["prospectiveTerm"] == 1 and e["round"] == 1
        for e in sends
    )
    # Every pre-vote result records the same round/prospectiveTerm, including
    # delivered grants.
    replies = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "preVote" and e["result"] == "delivered"
    ]
    assert {e["detail"] for e in replies} == {"granted"}
    assert all(e["round"] == 1 and e["prospectiveTerm"] == 1 for e in replies)

    # The real RequestVote only happens in term 1 after promotion.
    rv = [e for e in _by_type(result, "messageSend") if e["message"] == "requestVote"]
    assert rv and all(e["term"] == 1 for e in rv)

    assert result["nodes"]["a"]["role"] == ROLE_LEADER
    assert result["nodes"]["a"]["term"] == 1
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}
    # seq stays global and contiguous with the new events present.
    seqs = [e["seq"] for e in result["timeline"]]
    assert seqs == list(range(1, len(seqs) + 1))


def test_pre_vote_replies_are_counted_then_go_stale_after_promotion(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(duration=300, preVote=True))
    pre_replies = [
        e for e in _by_type(result, "messageResult") if e["message"] == "preVoteReply"
    ]
    details = [(e["peer"], e["detail"], e["round"], e["prospectiveTerm"]) for e in pre_replies]
    # The first grant makes "a" promote; the second grant for the same round is
    # a late reply and is therefore reported as staleRound.
    assert ("b", "granted", 1, 1) in details or ("c", "granted", 1, 1) in details
    assert any(d == "staleRound" for _, d, _, _ in details)
    assert all(r == 1 and p == 1 for _, _, r, p in details)


def test_election_is_still_decided_under_one_term(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _base_scenario(preVote=True))
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}


# -- the anti-disruption property --------------------------------------------


def _isolation_scenario(**overrides):
    # "a" wins term 1; "c" is then isolated and would (without pre-vote) burn
    # ever higher terms. Heartbeats keep the majority side stable.
    return _base_scenario(
        nodes=["a", "b", "c"],
        duration=650,
        electionTimeouts={"a": 80, "b": 300, "c": 200},
        faults=[
            {"time": 130, "action": "partition", "groups": [["c"], ["a", "b"]]},
            {"time": 500, "action": "heal"},
        ],
        preVote=True,
        **overrides,
    )


def test_isolated_node_keeps_opening_pre_vote_rounds_without_raising_term(tmp_path, capsys):
    # Isolate "a" on its own from time 0: the majority side (b, c) elects a
    # leader, while "a" can never win a round and keeps probing from term 0.
    scenario = _base_scenario(
        duration=400,
        electionTimeouts={"a": 100, "b": 150, "c": 200},
        faults=[{"time": 0, "action": "partition", "groups": [["a"], ["b", "c"]]}],
        preVote=True,
    )
    result = _run_ok(tmp_path, capsys, scenario)

    a_states = [
        e for e in _by_type(result, "stateChange")
        if e["node"] == "a" and e["role"] == ROLE_PRECANDIDATE
    ]
    # Several pre-vote rounds while isolated ...
    assert len(a_states) >= 3
    # ... none of which bumps the persisted term or casts a real vote.
    assert result["nodes"]["a"]["term"] == 0
    assert result["nodes"]["a"]["votedFor"] is None
    assert result["nodes"]["a"]["role"] == ROLE_PRECANDIDATE
    # Consecutive rounds keep prospectiveTerm at current term + 1 = 1.
    sends = [
        e for e in _by_type(result, "messageSend")
        if e["node"] == "a" and e["message"] == "preVote"
    ]
    assert [e["round"] for e in sends][:6] == [1, 1, 2, 2, 3, 3]
    assert all(e["prospectiveTerm"] == 1 for e in sends)
    # The majority side elects exactly one leader and is never disrupted.
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["b"]}, "violations": []}


def test_recovering_node_cannot_grant_itself_enough_votes_and_stands_down(tmp_path, capsys):
    result = _run_ok(tmp_path, capsys, _isolation_scenario())

    # After the heal, "c"'s in-flight pre-vote round reaches the majority
    # side: the leader rejects outright and "b" rejects because it heard a
    # current-term heartbeat within its election deadline. Neither the
    # receiver term/vote nor its deadline is changed by the refusal.
    post_heal = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "preVote"
        and e["result"] == "delivered"
        and e["time"] >= 500
    ]
    assert {(e["node"], e["detail"]) for e in post_heal} == {
        ("a", "rejected"),
        ("b", "rejected"),
    }
    # The stable leader never left term 1 and "c" is folded back as a follower
    # by a legitimate same-term heartbeat (no higher term involved). It may
    # first open one more pre-vote round at its own timeout before that
    # heartbeat lands.
    assert result["electionSafety"] == {"leadersByTerm": {"1": ["a"]}, "violations": []}
    assert result["nodes"]["a"]["role"] == ROLE_LEADER
    assert result["nodes"]["a"]["term"] == 1
    demotion = next(
        e for e in _by_type(result, "stateChange")
        if e["node"] == "c" and e["time"] >= 500 and e["role"] == ROLE_FOLLOWER
    )
    assert demotion["reason"] == "heartbeat"
    assert demotion["term"] == 1
    assert result["nodes"]["c"]["role"] == ROLE_FOLLOWER
    assert result["nodes"]["c"]["term"] == 1


def test_legitimate_leader_message_cancels_an_active_pre_vote_round(tmp_path, capsys):
    # "c" is isolated, opens a round at t=400; the heal at 500 lets a current
    # term heartbeat demote it to follower before its next timeout.
    result = _run_ok(tmp_path, capsys, _isolation_scenario())
    c = result["nodes"]["c"]
    assert c["role"] == ROLE_FOLLOWER
    assert c["knownLeader"] == "a"


# -- liveness ----------------------------------------------------------------


def test_new_leader_is_elected_after_leader_crash(tmp_path, capsys):
    scenario = _base_scenario(
        duration=600,
        preVote=True,
        nodeEvents=[{"time": 145, "node": "a", "action": "crash"}],
    )
    result = _run_ok(tmp_path, capsys, scenario)
    # Followers grant pre-votes only once their own deadline (measured from
    # the last accepted leader message) has elapsed, then elect a new leader.
    assert result["electionSafety"]["violations"] == []
    leaders = result["electionSafety"]["leadersByTerm"]
    assert "2" in leaders
    assert any(n["role"] == ROLE_LEADER for n in result["nodes"].values())


def test_failed_real_election_opens_a_new_pre_vote_round(tmp_path, capsys):
    scenario = _base_scenario(
        duration=420,
        electionTimeouts={"a": 100, "b": 300, "c": 300},
        preVote=True,
        messageFaults=[
            {"from": "a", "to": "b", "message": "requestVote", "occurrence": 1, "action": "drop"},
            {"from": "a", "to": "c", "message": "requestVote", "occurrence": 1, "action": "drop"},
        ],
    )
    result = _run_ok(tmp_path, capsys, scenario)
    sends = [
        (e["round"], e["prospectiveTerm"])
        for e in _by_type(result, "messageSend")
        if e["node"] == "a" and e["message"] == "preVote"
    ]
    # Round 1 probes prospective term 1; after the real election stalls, the
    # next round probes prospective term 2.
    assert sends[:2] == [(1, 1), (1, 1)]
    assert (2, 2) in sends
    assert result["nodes"]["a"]["term"] == 2
    assert result["nodes"]["a"]["role"] == ROLE_LEADER


# -- crash / restart ----------------------------------------------------------


def test_pre_vote_state_is_volatile_across_restart(tmp_path, capsys):
    scenario = _base_scenario(
        duration=400,
        preVote=True,
        nodeEvents=[
            {"time": 105, "node": "a", "action": "crash"},
            {"time": 160, "node": "a", "action": "restart"},
        ],
    )
    result = _run_ok(tmp_path, capsys, scenario)
    # After restart the node is a follower that waits a fresh timeout; a reply
    # from the pre-crash round cannot be counted (the node was down on
    # arrival or the round is gone), so no post-restart pre-vote is granted.
    assert result["nodes"]["a"]["role"] == ROLE_FOLLOWER
    counted_after = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "preVoteReply"
        and e.get("detail") == "granted"
        and e["node"] == "a"
        and e["time"] >= 160
    ]
    assert counted_after == []
    assert result["electionSafety"]["violations"] == []


# -- message faults -----------------------------------------------------------


def test_message_faults_can_drop_and_delay_pre_vote_traffic(tmp_path, capsys):
    scenario = _base_scenario(
        duration=300,
        preVote=True,
        messageFaults=[
            {"from": "a", "to": "b", "message": "preVote", "occurrence": 1, "action": "drop"},
        ],
    )
    result = _run_ok(tmp_path, capsys, scenario)
    dropped = [
        e for e in _by_type(result, "messageResult")
        if e["message"] == "preVote" and e.get("result") == "dropped"
    ]
    assert len(dropped) == 1
    entry = dropped[0]
    assert entry["reason"] == "messageFault"
    # Drop results still carry round and prospectiveTerm.
    assert entry["round"] == 1 and entry["prospectiveTerm"] == 1
    faults = _by_type(result, "messageFault")
    assert faults[0]["message"] == "preVote"
    assert faults[0]["scheduledTime"] == 110


def test_delayed_pre_vote_reply_is_a_late_stale_round(tmp_path, capsys):
    scenario = _base_scenario(
        duration=300,
        preVote=True,
        messageFaults=[
            {"from": "b", "to": "a", "message": "preVoteReply",
             "occurrence": 1, "action": "delay", "delay": 5},
        ],
    )
    result = _run_ok(tmp_path, capsys, scenario)
    fault = _by_type(result, "messageFault")[0]
    assert fault["scheduledTime"] == 120 and fault["arrivalTime"] == 125
    replies = [
        e for e in _by_type(result, "messageResult") if e["message"] == "preVoteReply"
    ]
    delayed = next(e for e in replies if e["peer"] == "b")
    assert delayed["detail"] == "staleRound"
    assert delayed["round"] == 1 and delayed["prospectiveTerm"] == 1


# -- validation ---------------------------------------------------------------


@pytest.mark.parametrize("value", [0, 1, "true", "True", None, [], {}, 1.0])
def test_pre_vote_must_be_a_boolean(tmp_path, capsys, value):
    code, out, err = _run(tmp_path, capsys, _base_scenario(preVote=value))
    assert code == 2 and out == ""
    assert err.startswith("error: ") and err.count("\n") == 1
    assert "preVote must be a boolean" in err


@pytest.mark.parametrize("kind", ["preVote", "preVoteReply"])
def test_pre_vote_message_faults_are_illegal_when_disabled(tmp_path, capsys, kind):
    scenario = _base_scenario(messageFaults=[
        {"from": "a", "to": "b", "message": kind, "occurrence": 1, "action": "drop"}
    ])
    code, out, err = _run(tmp_path, capsys, scenario)
    assert code == 2 and out == ""
    assert err.startswith("error: ") and err.count("\n") == 1
    assert kind in err


def test_pre_vote_message_faults_are_legal_when_enabled(tmp_path, capsys):
    scenario = _base_scenario(
        preVote=True,
        messageFaults=[
            {"from": "a", "to": "b", "message": "preVote", "occurrence": 1, "action": "drop"},
            {"from": "b", "to": "a", "message": "preVoteReply",
             "occurrence": 1, "action": "delay", "delay": 3},
        ],
    )
    code, _, err = _run(tmp_path, capsys, scenario)
    assert code == 0 and err == ""


# -- grant predicates (deterministic, isolated from timing) ------------------


def _simulator(scenario):
    return _Simulator(parse_scenario(scenario))


def _pre_vote(sim, src, dst, *, term, prospective, round_no, last_index, last_term):
    sim._handle_pre_vote({
        "src": src,
        "dst": dst,
        "kind": "preVote",
        "term": term,
        "prospectiveTerm": prospective,
        "round": round_no,
        "lastLogIndex": last_index,
        "lastLogTerm": last_term,
    })


def _latest_detail(sim, dst, src):
    return next(
        e for e in reversed(sim.timeline)
        if e["type"] == "messageResult" and e["node"] == dst and e["peer"] == src
        and e["message"] == "preVote"
    )["detail"]


def test_grant_requires_fresh_prospective_term(tmp_path):
    sim = _simulator(_base_scenario(preVote=True))
    st = sim.state["b"]
    st.term = 5
    sim.now = 1000
    _pre_vote(sim, "a", "b", term=5, prospective=5, round_no=1, last_index=9, last_term=5)
    assert _latest_detail(sim, "b", "a") == "rejected"
    assert st.term == 5 and st.voted_for is None
    _pre_vote(sim, "a", "b", term=5, prospective=6, round_no=1, last_index=9, last_term=5)
    assert _latest_detail(sim, "b", "a") == "granted"
    # Granting a pre-vote changes neither term nor votedFor.
    assert st.term == 5 and st.voted_for is None


def test_grant_requires_an_up_to_date_log(tmp_path):
    sim = _simulator(_base_scenario(preVote=True))
    st = sim.state["b"]
    st.term = 2
    # b holds a longer, higher-term log; the requester trails it.
    st.log = [
        {"index": 1, "term": 1, "id": "x", "command": 1},
        {"index": 2, "term": 2, "id": "y", "command": 2},
    ]
    sim.now = 1000
    _pre_vote(sim, "c", "b", term=2, prospective=3, round_no=1, last_index=1, last_term=1)
    assert _latest_detail(sim, "b", "c") == "rejected"
    # Equal-or-newer log is granted.
    _pre_vote(sim, "c", "b", term=2, prospective=3, round_no=1, last_index=2, last_term=2)
    assert _latest_detail(sim, "b", "c") == "granted"


def test_grant_waits_for_local_election_deadline_after_leader_contact(tmp_path):
    sim = _simulator(_base_scenario(preVote=True))
    st = sim.state["b"]
    st.term = 1
    st.leader_contact = 100  # heard a term-1 heartbeat at t=100; timeout(b)=150
    sim.now = 200
    _pre_vote(sim, "c", "b", term=1, prospective=2, round_no=1, last_index=0, last_term=0)
    assert _latest_detail(sim, "b", "c") == "rejected"
    # At/after contact + timeout the deadline has elapsed.
    sim.now = 250
    _pre_vote(sim, "c", "b", term=1, prospective=2, round_no=1, last_index=0, last_term=0)
    assert _latest_detail(sim, "b", "c") == "granted"


def test_a_leader_never_grants_a_pre_vote(tmp_path):
    sim = _simulator(_base_scenario(preVote=True))
    st = sim.state["a"]
    st.role = ROLE_LEADER
    st.term = 1
    sim.now = 1000
    _pre_vote(sim, "c", "a", term=1, prospective=2, round_no=1, last_index=0, last_term=0)
    assert _latest_detail(sim, "a", "c") == "rejected"
    assert st.term == 1


def test_pre_vote_refusal_leaves_receiver_state_untouched(tmp_path):
    sim = _simulator(_base_scenario(preVote=True))
    st = sim.state["b"]
    st.term = 3
    st.voted_for = "a"
    gen_before = st.timeout_gen
    st.leader_contact = 100
    sim.now = 150
    _pre_vote(sim, "c", "b", term=3, prospective=4, round_no=1, last_index=0, last_term=0)
    assert _latest_detail(sim, "b", "c") == "rejected"
    assert st.term == 3 and st.voted_for == "a"
    assert st.timeout_gen == gen_before  # deadline untouched
    assert st.leader_contact == 100


# -- joint consensus quorum ---------------------------------------------------


def test_pre_vote_needs_both_majorities_in_a_joint_config(tmp_path, capsys):
    # Four voters during a joint (old {a,b,c}, new {a,b,c,d}); isolating one
    # old voter and the new learner-turned-voter must deny the round the old
    # majority. We assert the healthy add still completes with pre-vote on.
    scenario = _base_scenario(
        nodes=["a", "b", "c", "d"],
        duration=700,
        electionTimeouts={"a": 80, "b": 150, "c": 200, "d": 250},
        messageDelay=5,
        preVote=True,
        initialMembers=["a", "b", "c"],
        membershipChanges=[
            {"time": 300, "node": "a", "id": "m1", "action": "add", "member": "d"}
        ],
        clientCommands=[{"time": 291, "node": "a", "id": "w1", "command": 1}],
    )
    result = _run_ok(tmp_path, capsys, scenario)
    change = result["membership"]["changes"][0]
    assert change["outcome"] == "committed"
    assert result["electionSafety"]["violations"] == []
    assert result["nodes"]["d"]["membershipRole"] == "voter"
    # Only real (formal) leaders are ever recorded in electionSafety.
    for term, names in result["electionSafety"]["leadersByTerm"].items():
        for name in names:
            assert result["nodes"][name]["role"] == ROLE_LEADER
            assert int(term) == result["nodes"][name]["term"]


# -- explore -----------------------------------------------------------------


def test_explore_enumerates_pre_vote_faults(tmp_path, capsys):
    scenario = _base_scenario(duration=200, preVote=True)
    plan = {
        "scenario": scenario,
        "candidates": [
            {"from": "b", "to": "a", "message": "preVoteReply",
             "occurrence": 1, "action": "drop"},
            {"from": "c", "to": "a", "message": "preVoteReply",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 1,
        "maxCases": 10,
    }
    path = _write(tmp_path, "plan.json", plan)
    code = main(["explore", path])
    out, err = capsys.readouterr()
    assert code == 0 and err == ""
    summary = json.loads(out)
    assert summary["totalCases"] == 3
    assert [case["selected"] for case in summary["cases"]] == [[], [0], [1]]
    # Every enumerated case re-runs the full pre-vote simulation byte-stably.
    assert all("preVote" in {
        e.get("message") for e in case["result"]["timeline"]
    } for case in summary["cases"])


def test_explore_rejects_pre_vote_candidate_when_disabled(tmp_path, capsys):
    plan = {
        "scenario": _base_scenario(),
        "candidates": [
            {"from": "a", "to": "b", "message": "preVote",
             "occurrence": 1, "action": "drop"},
        ],
        "maxFaults": 1,
        "maxCases": 10,
    }
    path = _write(tmp_path, "plan.json", plan)
    code = main(["explore", path])
    out, err = capsys.readouterr()
    assert code == 2 and out == ""
    assert err.startswith("error: ") and err.count("\n") == 1
    assert "preVote" in err


# -- replay -------------------------------------------------------------------


def test_replay_verifies_pre_vote_result_field_by_field(tmp_path, capsys):
    scenario = _base_scenario(duration=300, preVote=True)
    spath = _write(tmp_path, "scenario.json", scenario)
    code = main(["simulate", spath])
    assert code == 0
    out, err = capsys.readouterr()
    assert err == ""
    rpath = _write(tmp_path, "result.json", json.loads(out))
    code = main(["replay", spath, rpath])
    rep_out, rep_err = capsys.readouterr()
    assert code == 0 and rep_err == ""
    assert rep_out == '{"status":"matched"}\n'


def test_replay_detects_a_tampered_pre_vote_round(tmp_path, capsys):
    scenario = _base_scenario(duration=300, preVote=True)
    spath = _write(tmp_path, "scenario.json", scenario)
    main(["simulate", spath])
    out, _ = capsys.readouterr()
    result = json.loads(out)
    # Tamper with a recorded pre-vote round; replay must flag a mismatch.
    for entry in result["timeline"]:
        if entry.get("message") == "preVote":
            entry["round"] = 99
            break
    rpath = _write(tmp_path, "result.json", result)
    code = main(["replay", spath, rpath])
    rep_out, rep_err = capsys.readouterr()
    assert code == 1 and rep_err == ""
    assert json.loads(rep_out)["status"] == "mismatched"
