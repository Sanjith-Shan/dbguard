import json

from dbguard.harness.report import (
    load_rows, percentile, render_markdown, summarize,
)


def test_percentile_nearest_rank():
    xs = [5, 1, 4, 2, 3]
    assert percentile(xs, 50) == 3
    assert percentile(xs, 99) == 5
    assert percentile(xs, 1) == 1
    assert percentile(list(range(1, 101)), 99) == 99
    assert percentile([None, 2.0], 50) == 2.0
    assert percentile([], 50) is None


def row(**kw):
    base = {"scenario": "kill", "mode": "dbguard", "rs": "rs1", "host": "h", "mysql_version": "8.4.6",
            "failover_s": 7.0, "lost_acked_writes": 0, "phantom_writes": 0,
            "single_writer_violations": 0, "false_failover": False, "converged": True,
            "rs2_state_changes": 0, "rejoin": None, "stall_s": None}
    base.update(kw)
    return base


def test_summary_from_synthetic_rows(tmp_path):
    rows = [row(failover_s=float(i), rejoin={"branch": "repoint" if i % 2 else "rebuild",
                                             "phantom_gtids": 0 if i % 2 else i, "duration_s": 3.0})
            for i in range(1, 11)]
    rows += [row(mode="naive", failover_s=2.0, lost_acked_writes=4), row(mode="naive", failover_s=3.0)]
    rows += [row(scenario="partition-manager", failover_s=None),
             row(scenario="partition-manager", mode="naive", false_failover=True, failover_s=None)]
    rows += [row(scenario="cost", semisync=ss, netem_ms=n, commit_p50_ms=1.0 + n, commit_p99_ms=5.0 + n,
                 writes_per_s=1000.0 / (1 + n)) for ss in (True, False) for n in (0, 2, 20)]
    rows += [row(scenario="switchover", stall_s=0.4, errors=0), row(scenario="switchover", stall_s=None, errors=2)]
    rows += [row(scenario="replica-loss", stall_s=30.0, resume_s=1.2,
                 clone={"bytes": 200_000_000, "duration_s": 10.0, "mb_per_s": 20.0})]
    rows += [row(scenario="hang-container", writes_on_woken_primary=0)]
    p = tmp_path / "kill_dbguard.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n\nnot json\n")

    s = summarize(load_rows(tmp_path))
    fo = {(x["scenario"], x["mode"]): x for x in s["failover"]}
    k = fo[("kill", "dbguard")]
    assert k["runs"] == 10 and k["failover_p50_s"] == 5.0 and k["failover_p99_s"] == 10.0
    assert k["lost_acked_writes"] == 0 and k["converged_runs"] == 10
    n = fo[("kill", "naive")]
    assert n["lost_acked_writes"] == 4 and n["runs_with_loss"] == 1
    assert fo[("partition-manager", "naive")]["false_failovers"] == 1
    assert fo[("partition-manager", "dbguard")]["false_failovers"] == 0
    assert fo[("hang-container", "dbguard")]["writes_on_woken_primary"] == 0

    rj = s["rejoin"][0]
    assert (rj["repoint"], rj["rebuild"]) == (5, 5)
    assert rj["phantom_gtids_max"] == 10 and rj["phantom_gtids_mean"] == 3.0

    cost = s["cost"]
    assert len(cost) == 6
    assert cost[0]["semisync"] is True and cost[0]["netem_ms"] == 0
    assert cost[-1]["semisync"] is False and cost[-1]["netem_ms"] == 20

    sw = s["switchover"][0]
    assert sw["errors"] == 2 and sw["runs_with_errors"] == 1 and sw["stall_p99_s"] == 0.4
    rl = s["replica_loss"][0]
    assert rl["clone_mb_per_s_p50"] == 20.0 and rl["clone_mb_p50"] == 200.0

    md = render_markdown(s)
    assert "## Failover under injected faults" in md
    assert "| kill | naive | 2 |" in md
    assert "## Cost of losslessness" in md and "## Planned switchover" in md
    assert "## Manager partitioned from the primary" in md


def test_main_writes_files(tmp_path, capsys):
    from dbguard.harness.report import main
    (tmp_path / "kill_dbguard.jsonl").write_text(json.dumps(row()) + "\n")
    assert main(["--results", str(tmp_path)]) == 0
    assert (tmp_path / "SUMMARY.md").exists()
    assert json.loads((tmp_path / "summary.json").read_text())["rows"] == 1
