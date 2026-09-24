import json

import pytest

from dbguard.harness import chaos
from dbguard.harness.chaos import (
    Opts,
    agent_port,
    analyze_acks,
    checker_fields,
    expand_cost,
    gtid_count,
    gtid_equal,
    haproxy_port,
    last_json,
    match_failover,
    mysql_port,
    parse_args,
    parse_gtid,
    set_nodes,
    spare_node,
)

U1 = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
U2 = "4e11fa47-71ca-11e1-9e33-c80aa9429562"


def test_ports_follow_contract():
    assert mysql_port("mysql-a1") == 13311 and mysql_port("mysql-a4") == 13314
    assert mysql_port("mysql-b3") == 13323
    assert agent_port("mysql-a1") == 18011 and agent_port("mysql-b4") == 18024
    assert haproxy_port("rs1") == 13306 and haproxy_port("rs2") == 13307
    assert set_nodes("rs2") == ["mysql-b1", "mysql-b2", "mysql-b3"]
    assert spare_node("rs1") == "mysql-a4"
    with pytest.raises(ValueError):
        mysql_port("haproxy")


def test_gtid_helpers():
    s = f"{U1}:1-5:7,\n{U2}:3"
    assert gtid_count(s) == 7
    assert gtid_equal(s, f"{U2}:3, {U1.upper()}:1-3:4-5:7")
    assert not gtid_equal(s, f"{U1}:1-5")
    assert parse_gtid("") == {} and gtid_count(None) == 0
    assert parse_gtid(f"{U1}:1-3:2-6") == {U1: [(1, 6)]}


def ok(t, lat=2.0, c=0, s=0):
    return {"client": c, "seq": s, "t_ok": t, "latency_ms": lat}


def err(t):
    return {"client": 0, "seq": 0, "error": "gone", "t_err": t}


def test_analyze_error_driven_failover():
    oks = [ok(100 + i * 0.1) for i in range(100)]          # 100.0 .. 109.9
    oks += [ok(117.5 + i * 0.1) for i in range(20)]         # resumes at 117.5
    errs = [err(110.5), err(111.0)]
    st = analyze_acks(oks, errs, inject_ts=110.0)
    assert st.first_error_ts == 110.5
    assert st.first_ok_after_ts == pytest.approx(117.5)
    assert st.failover_s == pytest.approx(7.5)
    assert st.stall_s == pytest.approx(117.5 - 109.9)
    assert st.acked == 120 and st.errors == 2 and st.errors_after_inject == 2
    assert st.commit_p50_ms == 2.0


def test_analyze_errorless_stall():
    oks = [ok(100 + i * 0.1) for i in range(100)] + [ok(125.0), ok(125.1)]
    st = analyze_acks(oks, [], inject_ts=110.0)
    assert st.first_error_ts is None
    assert st.failover_s == pytest.approx(15.0)
    assert st.stall_s == pytest.approx(125.0 - 109.9)


def test_analyze_no_stall_no_failover():
    oks = [ok(100 + i * 0.01, lat=float(i % 10)) for i in range(1000)]
    st = analyze_acks(oks, [], inject_ts=105.0)
    assert st.failover_s is None
    assert st.stall_s == pytest.approx(0.01, abs=1e-6)
    assert st.commit_p99_ms == 9.0
    assert st.writes_per_s == pytest.approx(1000 / 9.99, rel=1e-3)


def test_analyze_ignores_errors_before_injection():
    oks = [ok(100 + i * 0.1) for i in range(200)]
    st = analyze_acks(oks, [err(101.0)], inject_ts=110.0)
    assert st.first_error_ts is None and st.errors == 1 and st.errors_after_inject == 0


def test_checker_fields_variants():
    v = {"lost_acked_writes": 2, "lost_list": [[1, 2], [3, 4]], "phantom_writes": 0,
         "single_writer_violations": 0, "converged": True, "converge_s": 1.5}
    f = checker_fields(v)
    assert f["lost_acked_writes"] == 2 and f["lost_list"] == [[1, 2], [3, 4]] and f["converged"]
    f = checker_fields({"lost": [[1, 1]] * 30, "phantom": [], "convergence": {"ok": False}})
    assert f["lost_acked_writes"] == 30 and len(f["lost_list"]) == 20
    assert f["phantom_writes"] == 0 and f["converged"] is False


def test_last_json():
    assert last_json('noise\n{"a": 1}\n') == {"a": 1}
    assert last_json('{"a":\n 2}') == {"a": 2}
    assert last_json("") is None


def test_match_failover():
    evs = [{"type": "suspect"}, {"type": "failover", "old_primary": "mysql-a2"},
           {"type": "failover", "old_primary": "mysql-a1"}]
    assert match_failover(evs, "mysql-a1")["old_primary"] == "mysql-a1"
    assert match_failover(evs, None)["old_primary"] == "mysql-a2"
    assert match_failover([{"type": "healthy"}], "x") is None


def test_expand_cost():
    o = Opts(scenario="cost", mode="dbguard", cost_all=True)
    vs = expand_cost(o)
    assert [(v.semisync, v.netem) for v in vs] == [("on", 0), ("on", 2), ("on", 20),
                                                   ("off", 0), ("off", 2), ("off", 20)]
    assert len(expand_cost(Opts(scenario="cost", mode="naive", cost_all=True))) == 3
    assert expand_cost(Opts(scenario="cost")) == [Opts(scenario="cost")]


def test_parse_args():
    a = parse_args(["--scenario", "kill", "--runs", "2", "--mode", "naive", "--out", "x.jsonl"])
    assert a.scenario == "kill" and a.runs == 2 and a.mode == "naive"
    a = parse_args(["--all", "--mode", "orchestrator"])
    assert a.all and a.scenario is None
    with pytest.raises(SystemExit):
        parse_args(["--scenario", "nope"])


def test_row_has_every_contract_key(tmp_path):
    contract = ["run_id", "scenario", "mode", "rs", "ts", "host", "mysql_version", "docker_version",
                "detect_window_s", "probe_timeout_s", "clients", "workload_s", "inject_ts",
                "failover_s", "first_error_ts", "first_ok_after_ts", "acked_writes",
                "lost_acked_writes", "lost_list", "phantom_writes", "single_writer_violations",
                "converged", "converge_s", "false_failover", "stall_s", "writes_on_woken_primary",
                "rejoin", "event", "rs2_state_changes", "commit_p50_ms", "commit_p99_ms",
                "writes_per_s", "clone", "notes"]
    run = chaos.Run(opts=Opts(scenario="kill"), fleet=chaos.Fleet("rs1", "dbguard"),
                    run_id="r", dir=tmp_path)
    row = chaos.base_row(run, {"host": "h", "mysql_version": "8.4", "docker_version": "d",
                               "knobs": {"detect_window_s": 5, "probe_timeout_s": 1}})
    assert set(contract) <= set(row)
    json.dumps(row)


def test_disk_full_is_explicitly_blocked(tmp_path):
    run = chaos.Run(opts=Opts(scenario="disk-full"), fleet=chaos.Fleet("rs1", "dbguard"),
                    run_id="r", dir=tmp_path)
    with pytest.raises(NotImplementedError):
        chaos.sc_disk_full(run)


def test_rejoin_matches():
    from dbguard.harness.chaos import rejoin_matches
    e = {"type": "rejoin", "old_primary": "mysql-a1", "rejoin": {"branch": "repoint", "node": "mysql-a1"}}
    assert rejoin_matches(e, "mysql-a1")
    assert not rejoin_matches(e, "mysql-a2")
    assert not rejoin_matches({"type": "rejoin", "rejoin": {"branch": "none", "node": "mysql-a1"}}, "mysql-a1")


def test_failover_ignores_ok_of_a_write_sent_before_the_error():
    # review finding 1: a pre-crash write whose ack lands 1 ms after the first error
    oks = [ok(100 + i * 0.1) for i in range(100)]
    oks.append({"client": 3, "seq": 9, "t_ok": 110.003, "latency_ms": 5.0, "t_start": 109.998})
    oks += [{"client": 1, "seq": 50 + i, "t_ok": 118.0 + i * 0.1, "latency_ms": 2.0,
             "t_start": 117.998 + i * 0.1} for i in range(5)]
    st = analyze_acks(oks, [err(110.002)], inject_ts=110.0)
    assert st.first_ok_after_ts == pytest.approx(118.0)
    assert st.failover_s == pytest.approx(8.0)


def test_ok_start_falls_back_to_latency():
    from dbguard.harness.chaos import ok_start
    assert ok_start({"t_ok": 10.0, "latency_ms": 500}) == pytest.approx(9.5)
    assert ok_start({"t_ok": 10.0, "t_start": 9.9}) == 9.9
