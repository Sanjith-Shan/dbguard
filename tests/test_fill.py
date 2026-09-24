import json
import subprocess
import sys
from pathlib import Path

from dbguard.harness import fill
from dbguard.harness.report import summarize

REPO = Path(__file__).resolve().parents[1]


def row(**kw):
    base = {"run_id": "r", "scenario": "kill", "mode": "dbguard", "rs": "rs1", "host": "lab host",
            "mysql_version": "8.4.6", "failover_s": 7.0, "lost_acked_writes": 0,
            "phantom_writes": 0, "single_writer_violations": 0, "false_failover": False,
            "converged": True, "rs2_state_changes": 0, "rejoin": None, "stall_s": None,
            "event": None}
    base.update(kw)
    return base


def ev(choose, promote):
    return {"type": "failover", "steps": {"choose": {"duration_s": choose},
                                          "promote": {"duration_s": promote},
                                          "repoint": {"duration_s": promote * 3}}}


KILL = [
    row(failover_s=7.0, rejoin={"branch": "repoint", "phantom_gtids": 0, "duration_s": 3.0},
        event=ev(0.1, 0.5)),
    row(failover_s=9.5, rejoin={"branch": "rebuild", "phantom_gtids": 4, "duration_s": 40.0},
        event=ev(0.3, 0.7)),
    row(failover_s=8.25, converged=False, rs2_state_changes=1,
        rejoin={"branch": "rebuild", "phantom_gtids": 2, "duration_s": 30.0}, event=ev(0.2, 0.6)),
]
COST = [row(scenario="cost", semisync=s, netem_ms=0.0, failover_s=None, commit_p50_ms=p50,
            commit_p99_ms=p99, writes_per_s=wps, lost_acked_writes=1 if not s else 0)
        for s, p50, p99, wps in [(True, 4.0, 20.0, 900.0), (True, 4.4, 22.0, 880.0),
                                 (False, 1.25, 5.0, 2000.4)]]

DOC = """# Doc

A tag of the form `[[N: what, source file]]` marks a number. Cells are `[[N: ...]]`.

| mode | runs | failover p50 s | converged |
|---|---|---|---|
| dbguard | `[[N: kill run count]]` | `[[N: kill failover p50, results/kill_dbguard.jsonl]]` | `[[N: kill dbguard converged runs, results/kill_dbguard.jsonl]]` |

| Scenario | Failover p99 |
|---|---|
| killed | `[[N: kill failover p99, results/kill_dbguard.jsonl]]` |

Median **`[[N: kill failover p50]]` s** and `[[N: choose step p99, results/kill_dbguard.jsonl]]` to choose, repoint `[[N: repoint step p50, results/kill_dbguard.jsonl]]`.
Cost `[[N: commit p50 semisync on vs off at 0 ms netem, results/cost_dbguard.jsonl]]`, p50 on was `[[N: cost p50 on 0ms]]` and wps `[[N: cost wps off 0ms]]`.
Split `[[N: rejoin repoint count vs rebuild count, results/kill_dbguard.jsonl]]`, rebuild p50 `[[N: rejoin rebuild duration p50, results/kill_dbguard.jsonl]]`.
Lost `[[N: dbguard lost acked writes total all scenarios except kill-two, results/*_dbguard.jsonl]]`, rs2 `[[N: rs2 state changes total across all rs1 injections, results/*.jsonl]]` over `[[N: total rs1 injections, results/*.jsonl]]`.
Env `[[N: host, Docker Desktop and MySQL version, results/SUMMARY.md]]`.
Memory `[[N: node mem idle MB, docker stats]]`, switchover `[[N: switchover stall p50]]`, odd `[[N: made up metric, results/x.jsonl]]`.
"""


def setup(tmp_path, with_summary=False):
    res = tmp_path / "results"
    res.mkdir()
    (res / "kill_dbguard.jsonl").write_text("".join(json.dumps(r) + "\n" for r in KILL))
    (res / "cost_dbguard.jsonl").write_text("".join(json.dumps(r) + "\n" for r in COST))
    (res / "kill_dbguard.errors.jsonl").write_text(json.dumps(
        {"scenario": "kill", "mode": "dbguard", "error": "x", "traceback": "t"}) + "\n")
    (res / "pilot").mkdir()
    (res / "pilot" / "kill_dbguard.jsonl").write_text(json.dumps(row(failover_s=99.0)) + "\n")
    if with_summary:
        (res / "summary.json").write_text(json.dumps(summarize(KILL + COST)))
    doc = tmp_path / "doc.md"
    doc.write_text(DOC)
    return res, doc


def by_what(res):
    out = {}
    for r in res:
        out.setdefault(r.tag.what, []).append(r)
    return out


def test_every_doc_tag_is_mapped():
    docs = [REPO / d for d in fill.DEFAULT_DOCS]
    tags = fill.scan(docs)
    assert len(tags) > 200
    assert not [t.body for t in tags if t.what not in fill.MAP]


def test_resolution_and_formatting(tmp_path):
    res_dir, doc = setup(tmp_path)
    ctx = fill.Ctx(res_dir, tmp_path)
    assert len(ctx.rows_all) == 6          # error rows and results/pilot/ ignored
    res = fill.resolve(fill.scan([doc]), ctx)
    w = by_what(res)
    assert "..." not in w and "what" not in w and "what, source file" not in w
    assert w["kill run count"][0].rendered == "3"
    # header "failover p50 s" carries the unit, the prose one is followed by "s"
    assert [r.rendered for r in w["kill failover p50"]] == ["8.25", "8.25"]
    # header "Failover p99" has no unit, so it is appended
    assert w["kill failover p99"][0].rendered == "9.50 s"
    assert w["kill dbguard converged runs"][0].rendered == "2/3"
    assert w["choose step p99"][0].rendered == "0.30 s"
    assert w["repoint step p50"][0].rendered == "1.80 s"
    assert w["commit p50 semisync on vs off at 0 ms netem"][0].rendered == \
        "+2.8 ms (4.0 ms on vs 1.2 ms off)"
    assert w["cost p50 on 0ms"][0].rendered == "4.0 ms"
    assert w["cost wps off 0ms"][0].rendered == "2000"
    assert w["rejoin repoint count vs rebuild count"][0].rendered == "1 repoint, 2 rebuild"
    assert w["rejoin rebuild duration p50"][0].rendered == "30.00 s"
    assert w["dbguard lost acked writes total all scenarios except kill-two"][0].rendered == "1"
    assert w["rs2 state changes total across all rs1 injections"][0].rendered == "1"
    assert w["total rs1 injections"][0].rendered == "6"
    assert w["host, Docker Desktop and MySQL version"][0].rendered == "lab host | MySQL 8.4.6"
    assert w["node mem idle MB"][0].status == "manual"
    assert w["switchover stall p50"][0].status == "pending"
    assert w["made up metric"][0].status == "unmapped"


def test_summary_json_used_when_it_matches(tmp_path):
    res_dir, _ = setup(tmp_path, with_summary=True)
    assert fill.Ctx(res_dir, tmp_path).summary_origin == "summary.json"
    (res_dir / "summary.json").write_text(json.dumps({"rows": 99}))
    assert fill.Ctx(res_dir, tmp_path).summary_origin == "rows"


def test_write_is_idempotent(tmp_path):
    res_dir, doc = setup(tmp_path)
    args = ["--write", "--results", str(res_dir), "--docs", str(doc), "--root", str(tmp_path)]
    assert fill.main(args) == 0
    once = doc.read_text()
    assert "| dbguard | 3 | 8.25 | 2/3 |" in once
    assert "| killed | 9.50 s |" in once
    assert "Median **8.25 s** and 0.30 s to choose, repoint 1.80 s." in once
    assert "`[[N: what, source file]]`" in once and "`[[N: ...]]`" in once
    assert "`[[N: node mem idle MB, docker stats]]`" in once
    assert "`[[N: switchover stall p50]]`" in once
    assert "`[[N: made up metric, results/x.jsonl]]`" in once
    assert fill.main(args) == 0
    assert doc.read_text() == once


def test_cli_check_changes_nothing(tmp_path):
    res_dir, doc = setup(tmp_path)
    cp = subprocess.run([sys.executable, str(REPO / "bin" / "fill-docs"), "--results", str(res_dir),
                         "--docs", str(doc), "--root", str(tmp_path)],
                        capture_output=True, text=True, check=True)
    assert "unmapped:" in cp.stdout and "made up metric" in cp.stdout
    assert doc.read_text() == DOC


def test_switchover_event_fields(tmp_path):
    res_dir, _ = setup(tmp_path)
    rows = [row(scenario="switchover", failover_s=None, stall_s=st,
                event={"type": "switchover", "steps": {"prepare": {"duration_s": pr}}})
            for st, pr in [(1.0, 0.4), (2.0, 0.6), (3.0, 0.5)]]
    (res_dir / "switchover_dbguard.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    ctx = fill.Ctx(res_dir, tmp_path)
    assert fill.MAP["switchover prepare p50"].fn(ctx) == 0.5
    assert fill.MAP["switchover prepare p99"].fn(ctx) == 0.6
    assert fill.MAP["switchover stall_s p50"].fn(ctx) == 2.0
    assert fill.MAP["switchover stall_s p99"].fn(ctx) == 3.0
