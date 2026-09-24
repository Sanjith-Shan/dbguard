import json

from dbguard.gtid import GtidSet
from dbguard.harness import checker, workload
from dbguard.mysqlx import parse_replica_status

U1 = "3e11fa47-71ca-11e1-9e33-c80aa9429562"


# ---------------------------------------------------------------- ack log

def test_ack_log_roundtrip_and_torn_lines(tmp_path):
    p = tmp_path / "a.jsonl"
    log = workload.AckLog(open(p, "w"))
    log.write({"client": 1, "seq": 1, "t_ok": 1.0, "latency_ms": 2.0})
    log.write({"client": 1, "seq": 2, "error": "boom", "t_err": 2.0})
    log.close()
    with open(p, "a") as f:
        f.write('\n{"client": 2, "seq": 9, "t_o')  # torn last line (killed mid-write)
    oks, errs = workload.read_ack_log(p)
    assert [(r["client"], r["seq"]) for r in oks] == [(1, 1)]
    assert [(r["client"], r["seq"]) for r in errs] == [(1, 2)]
    log.write({"client": 3, "seq": 3, "t_ok": 3.0})  # after close: dropped, no crash


def test_load_acks_merges_files(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_text(json.dumps({"client": 1, "seq": 1, "t_ok": 1}) + "\n")
    b.write_text(json.dumps({"client": 2, "seq": 5, "error": "x", "t_err": 1}) + "\n")
    acks = checker.load_acks([str(a), str(b)])
    assert acks.acked == {(1, 1)} and acks.errored == {(2, 5)}


def test_percentile_nearest_rank():
    xs = list(range(1, 101))
    assert workload.percentile(xs, 50) == 50
    assert workload.percentile(xs, 99) == 99
    assert workload.percentile([], 50) is None
    assert workload.percentile([7.0], 99) == 7.0


# ---------------------------------------------------------------- checker logic

class FakeNode:
    def __init__(self, name, sro=True, ro=True, gtid="", rows=(), accept_insert=None):
        self.name = name
        self.sro, self.ro = sro, ro
        self.gtids = [gtid] if isinstance(gtid, str) else list(gtid)
        self._rows = set(rows)
        self.accept_insert = (not sro) if accept_insert is None else accept_insert

    def read_only(self):
        return self.sro, self.ro

    def probe_insert(self):
        return None if self.accept_insert else "1290 super-read-only"

    def gtid_executed(self):
        return self.gtids.pop(0) if len(self.gtids) > 1 else self.gtids[0]

    def rows(self, run_id):
        return set(self._rows)


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def acks(acked, errored=()):
    return checker.AckSets(set(acked), set(errored))


def healthy(rows, gtid=f"{U1}:1-10"):
    return [FakeNode("p", sro=False, ro=False, gtid=gtid, rows=rows),
            FakeNode("r1", gtid=gtid), FakeNode("r2", gtid=gtid)]


def run(a, nodes, **kw):
    clk = FakeClock()
    return checker.check("run", a, nodes, {}, converge_timeout=kw.pop("timeout", 5),
                         clock=clk, sleep=clk.sleep, **kw)


def test_all_pass():
    v = run(acks({(1, 1), (1, 2)}), healthy({(1, 1), (1, 2)}))
    assert v["pass"] and v["primary"] == "p"
    assert v["lost_acked_writes"] == 0 and v["phantom_writes"] == 0
    assert v["single_writer_violations"] == 0 and v["converged"]


def test_lost_acked_write_detected():
    v = run(acks({(1, 1), (1, 2), (2, 7)}), healthy({(1, 1)}))
    assert not v["pass"] and not v["properties"]["lossless"]["ok"]
    assert v["lost_acked_writes"] == 2 and v["lost_list"] == [[1, 2], [2, 7]]


def test_in_flight_error_row_is_neither_loss_nor_phantom():
    # seq 2 got an error: landing (client 1) or not landing (client 2) are both fine
    v = run(acks({(1, 1), (2, 1)}, errored={(1, 2), (2, 2)}), healthy({(1, 1), (1, 2), (2, 1)}))
    assert v["pass"]
    nop = v["properties"]["no_phantom"]
    assert nop["errored_landed"] == 1 and nop["errored_absent"] == 1


def test_phantom_detected():
    v = run(acks({(1, 1)}), healthy({(1, 1), (1, 2)}))
    assert not v["pass"] and v["phantom_writes"] == 1 and v["phantom_list"] == [[1, 2]]


def test_two_writers_is_a_violation():
    nodes = healthy({(1, 1)})
    nodes[1].sro = nodes[1].ro = False
    v = run(acks({(1, 1)}), nodes)
    assert not v["pass"] and v["primary"] is None
    assert v["single_writer_violations"] == 1


def test_no_writer_is_a_violation():
    nodes = healthy(set())
    nodes[0].sro = nodes[0].ro = True
    nodes[0].accept_insert = False
    v = run(acks(set()), nodes)
    assert not v["pass"] and v["single_writer_violations"] == 1


def test_read_only_node_that_accepts_insert_is_a_violation():
    # read_only=1 but super_read_only=0 lets a SUPER user write: not fenced
    nodes = healthy({(1, 1)})
    nodes[2].sro, nodes[2].ro, nodes[2].accept_insert = False, True, True
    v = run(acks({(1, 1)}), nodes)
    assert not v["pass"]
    assert v["properties"]["single_writer"]["insert_probe"]["r2"] == "accepted"


def test_convergence_waits_for_lagging_replica():
    nodes = healthy({(1, 1)})
    nodes[2].gtids = [f"{U1}:1-8", f"{U1}:1-9", f"{U1}:1-10"]
    v = run(acks({(1, 1)}), nodes)
    assert v["pass"] and v["converge_s"] == 1.0


def test_convergence_times_out_and_reports_extra():
    nodes = healthy({(1, 1)})
    other = "4e11fa47-71ca-11e1-9e33-c80aa9429562"
    nodes[1].gtids = [f"{U1}:1-10,{other}:1-2"]  # errant transaction on a replica
    v = run(acks({(1, 1)}), nodes, timeout=2)
    assert not v["pass"] and not v["converged"]
    conv = v["properties"]["convergence"]
    assert GtidSet.parse(conv["extra_on_replica"]["r1"]) == GtidSet.parse(f"{other}:1-2")


def test_gtid_compared_as_sets_not_strings():
    nodes = healthy({(1, 1)}, gtid=f"{U1}:1-5:6-10")
    nodes[1].gtids = [f"{U1}:1-10"]
    nodes[2].gtids = [f"{U1}:1-10"]
    assert run(acks({(1, 1)}), nodes)["converged"]


def test_parse_nodes():
    assert checker.parse_nodes("mysql-a1:13311, x@10.0.0.2:3306") == [
        ("mysql-a1", "127.0.0.1", 13311), ("x", "10.0.0.2", 3306)]


# ---------------------------------------------------------------- SHOW REPLICA STATUS

def test_parse_replica_status():
    row = {"Source_Host": "mysql-a1", "Replica_IO_Running": "Yes", "Replica_SQL_Running": "Yes",
           "Seconds_Behind_Source": 0, "Retrieved_Gtid_Set": f"{U1}:1-5,\n{U1[:-1]}3:1",
           "Executed_Gtid_Set": "", "Last_IO_Error": "", "Last_SQL_Error": ""}
    r = parse_replica_status(row)
    assert r["configured"] and r["io_running"] == "Yes" and r["seconds_behind_source"] == 0
    assert "\n" not in r["retrieved_gtid_set"]
    assert r["executed_gtid_set"] == "" and r["last_io_error"] is None
    assert parse_replica_status(None) is None
    row["Seconds_Behind_Source"] = None
    assert parse_replica_status(row)["seconds_behind_source"] is None


def test_bootstrap_host_ports():
    from dbguard.harness.bootstrap import host_port
    assert host_port("mysql-a1") == 13311 and host_port("mysql-b4") == 13324
