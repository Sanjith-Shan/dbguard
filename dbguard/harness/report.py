"""Turn results/*.jsonl chaos rows into the README tables (Markdown) and results/summary.json.

Percentiles are nearest-rank on the sorted list (no numpy): p(q) = sorted[ceil(q/100*n) - 1].
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

FAILOVER_SCENARIOS = ["kill", "hang-container", "hang-process", "partition-replicas", "disk-full",
                      "kill-two"]
MODES = ["dbguard", "naive", "orchestrator"]


def percentile(values: Iterable[float | None], q: float) -> float | None:
    xs = sorted(float(v) for v in values if v is not None)
    if not xs:
        return None
    rank = max(1, math.ceil(q / 100.0 * len(xs)))
    return xs[rank - 1]


def load_rows(results_dir: Path) -> list[dict]:
    rows = []
    for p in sorted(Path(results_dir).glob("*.jsonl")):
        if p.name.endswith(".errors.jsonl"):
            continue   # failed runs are not runs, see load_errors()
        for i, line in enumerate(p.read_text().splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            row.setdefault("_file", f"{p.name}:{i + 1}")
            rows.append(row)
    return rows


def load_errors(results_dir: Path) -> dict[str, int]:
    """Failed runs per scenario/mode, from results/<scenario>_<mode>.errors.jsonl."""
    out: dict[str, int] = defaultdict(int)
    for p in sorted(Path(results_dir).glob("*.errors.jsonl")):
        for line in p.read_text().splitlines():
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            out[f"{r.get('scenario', '?')}/{r.get('mode', '?')}"] += 1
    return dict(sorted(out.items()))


def _f(v, digits=2) -> str:
    if v is None:
        return "-"
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def md_table(header: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(_f(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def _group(rows: list[dict], scenarios: list[str]) -> dict[tuple[str, str], list[dict]]:
    g: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("scenario") in scenarios:
            g[(r["scenario"], r.get("mode", "?"))].append(r)
    order = {s: i for i, s in enumerate(scenarios)}
    morder = {m: i for i, m in enumerate(MODES)}
    return dict(sorted(g.items(), key=lambda kv: (order.get(kv[0][0], 99), morder.get(kv[0][1], 99))))


def _sum(rows, key) -> int:
    return sum(int(r.get(key) or 0) for r in rows)


def failover_summary(rows: list[dict]) -> list[dict]:
    out = []
    for (sc, mode), rs in _group(rows, FAILOVER_SCENARIOS + ["partition-manager"]).items():
        fo = [r.get("failover_s") for r in rs]
        woken = [r.get("writes_on_woken_primary") for r in rs if r.get("writes_on_woken_primary") is not None]
        out.append({
            "scenario": sc, "mode": mode, "runs": len(rs),
            "failover_p50_s": percentile(fo, 50), "failover_p99_s": percentile(fo, 99),
            "failed_over_runs": sum(1 for v in fo if v is not None),
            "lost_acked_writes": _sum(rs, "lost_acked_writes"),
            "runs_with_loss": sum(1 for r in rs if (r.get("lost_acked_writes") or 0) > 0),
            "phantom_writes": _sum(rs, "phantom_writes"),
            "single_writer_violations": _sum(rs, "single_writer_violations"),
            "false_failovers": sum(1 for r in rs if r.get("false_failover")),
            "converged_runs": sum(1 for r in rs if r.get("converged")),
            "writes_on_woken_primary": sum(int(v) for v in woken) if woken else None,
            "stall_p50_s": percentile([r.get("stall_s") for r in rs], 50),
            "stall_p99_s": percentile([r.get("stall_s") for r in rs], 99),
            "reconnect_gap_p50_s": percentile([r.get("reconnect_gap_p50_s") for r in rs], 50),
            "rs2_state_changes": _sum(rs, "rs2_state_changes"),
            "rs2_role_changes": _sum(rs, "rs2_role_changes"),
            "runs_with_errant_gtids": sum(1 for r in rs if r.get("errant_gtids")),
        })
    return out


def rejoin_summary(rows: list[dict]) -> list[dict]:
    out = []
    for (sc, mode), rs in _group(rows, FAILOVER_SCENARIOS).items():
        rj = [r["rejoin"] for r in rs if r.get("rejoin")]
        if not rj:
            continue
        branches = defaultdict(int)
        for j in rj:
            branches[j.get("branch") or "none"] += 1
        ph = [int(j.get("phantom_gtids") or 0) for j in rj]
        out.append({
            "scenario": sc, "mode": mode, "rejoins": len(rj),
            "repoint": branches.get("repoint", 0), "rebuild": branches.get("rebuild", 0),
            "other": sum(v for k, v in branches.items() if k not in ("repoint", "rebuild")),
            "phantom_gtids_mean": sum(ph) / len(ph) if ph else None,
            "phantom_gtids_max": max(ph) if ph else None,
            "rejoin_p50_s": percentile([j.get("duration_s") for j in rj], 50),
            "rejoin_p99_s": percentile([j.get("duration_s") for j in rj], 99),
        })
    return out


def cost_summary(rows: list[dict]) -> list[dict]:
    g: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("scenario") != "cost":
            continue
        g[(r.get("mode"), bool(r.get("semisync", True)), float(r.get("netem_ms") or 0))].append(r)
    out = []
    for (mode, ss, netem), rs in sorted(g.items(), key=lambda kv: (MODES.index(kv[0][0]) if kv[0][0] in MODES else 9,
                                                                    not kv[0][1], kv[0][2])):
        out.append({
            "mode": mode, "semisync": ss, "netem_ms": netem, "runs": len(rs),
            # the per-run p50/p99 are aggregated by their median across runs
            "commit_p50_ms": percentile([r.get("commit_p50_ms") for r in rs], 50),
            "commit_p99_ms": percentile([r.get("commit_p99_ms") for r in rs], 50),
            "writes_per_s": percentile([r.get("writes_per_s") for r in rs], 50),
            "semisync_avg_wait_us": percentile([r.get("semisync_avg_wait_us") for r in rs], 50),
        })
    return out


def replica_loss_summary(rows: list[dict]) -> list[dict]:
    out = []
    for (sc, mode), rs in _group(rows, ["replica-loss"]).items():
        clones = [r["clone"] for r in rs if r.get("clone")]
        out.append({
            "mode": mode, "runs": len(rs),
            "stall_p50_s": percentile([r.get("stall_s") for r in rs], 50),
            "resume_p50_s": percentile([r.get("resume_s") for r in rs], 50),
            "clones": len(clones),
            "clone_mb_p50": percentile([c.get("bytes", 0) / 1e6 for c in clones], 50),
            "clone_s_p50": percentile([c.get("duration_s") for c in clones], 50),
            "clone_mb_per_s_p50": percentile([c.get("mb_per_s") for c in clones], 50),
            "lost_acked_writes": _sum(rs, "lost_acked_writes"),
        })
    return out


def switchover_summary(rows: list[dict]) -> list[dict]:
    out = []
    for (sc, mode), rs in _group(rows, ["switchover"]).items():
        out.append({
            "mode": mode, "runs": len(rs),
            "stall_p50_s": percentile([r.get("stall_s") or 0.0 for r in rs], 50),
            "stall_p99_s": percentile([r.get("stall_s") or 0.0 for r in rs], 99),
            "errors": _sum(rs, "errors"),
            "runs_with_errors": sum(1 for r in rs if (r.get("errors") or 0) > 0),
            "lost_acked_writes": _sum(rs, "lost_acked_writes"),
        })
    return out


def summarize(rows: list[dict], errors: dict[str, int] | None = None) -> dict:
    return {
        "rows": len(rows),
        "failed_runs": errors or {},
        "failover": failover_summary(rows),
        "rejoin": rejoin_summary(rows),
        "cost": cost_summary(rows),
        "replica_loss": replica_loss_summary(rows),
        "switchover": switchover_summary(rows),
        "environment": sorted({(r.get("host") or "?") + " | MySQL " + (r.get("mysql_version") or "?")
                               for r in rows}),
    }


def render_markdown(s: dict) -> str:
    parts = ["# DBGuard chaos results", "",
             f"{s['rows']} runs. Generated by `bin/report` from `results/*.jsonl`. "
             "Percentiles are nearest-rank.", ""]
    for env in s["environment"]:
        parts.append(f"- {env}")
    parts.append("")
    if s.get("failed_runs"):
        parts.append("Runs that raised and wrote no row (results/*.errors.jsonl): " +
                     ", ".join(f"{k} {v}" for k, v in s["failed_runs"].items()) + ".")
        parts.append("")

    fo = [x for x in s["failover"] if x["scenario"] != "partition-manager"]
    if fo:
        parts += ["## Failover under injected faults", "", md_table(
            ["scenario", "mode", "runs", "failover p50 s", "failover p99 s", "lost acked writes",
             "runs with loss", "phantom writes", "single-writer violations", "false failovers",
             "converged", "writes on woken primary", "stall p50 s", "stall p99 s",
             "client reconnect gap p50 s", "runs with errant GTIDs", "rs2 changes"],
            [[x["scenario"], x["mode"], x["runs"], x["failover_p50_s"], x["failover_p99_s"],
              x["lost_acked_writes"], x["runs_with_loss"], x["phantom_writes"],
              x["single_writer_violations"], x["false_failovers"],
              f"{x['converged_runs']}/{x['runs']}", x["writes_on_woken_primary"],
              x["stall_p50_s"], x["stall_p99_s"], x["reconnect_gap_p50_s"],
              x["runs_with_errant_gtids"],
              x["rs2_state_changes"]] for x in fo]), ""]
    pm = [x for x in s["failover"] if x["scenario"] == "partition-manager"]
    if pm:
        parts += ["## Manager partitioned from the primary (correct action is none)", "", md_table(
            ["mode", "runs", "false failovers", "lost acked writes", "converged", "rs2 changes"],
            [[x["mode"], x["runs"], x["false_failovers"], x["lost_acked_writes"],
              f"{x['converged_runs']}/{x['runs']}", x["rs2_state_changes"]] for x in pm]), ""]
    if s["rejoin"]:
        parts += ["## Rejoin of the old primary", "", md_table(
            ["scenario", "mode", "rejoins", "repoint", "rebuild", "other", "phantom GTIDs mean",
             "phantom GTIDs max", "rejoin p50 s", "rejoin p99 s"],
            [[x["scenario"], x["mode"], x["rejoins"], x["repoint"], x["rebuild"], x["other"],
              x["phantom_gtids_mean"], x["phantom_gtids_max"], x["rejoin_p50_s"], x["rejoin_p99_s"]]
             for x in s["rejoin"]]), ""]
    if s["cost"]:
        parts += ["## Cost of losslessness", "", md_table(
            ["mode", "semi-sync", "netem ms", "runs", "commit p50 ms", "commit p99 ms", "writes/s",
             "semi-sync avg wait us"],
            [[x["mode"], "on" if x["semisync"] else "off", x["netem_ms"], x["runs"],
              x["commit_p50_ms"], x["commit_p99_ms"], x["writes_per_s"],
              x["semisync_avg_wait_us"]] for x in s["cost"]]), ""]
    if s["replica_loss"]:
        parts += ["## Replica loss and replacement", "", md_table(
            ["mode", "runs", "stall p50 s (no replica)", "resume p50 s", "clones", "clone MB",
             "clone s", "clone MB/s", "lost acked writes"],
            [[x["mode"], x["runs"], x["stall_p50_s"], x["resume_p50_s"], x["clones"],
              x["clone_mb_p50"], x["clone_s_p50"], x["clone_mb_per_s_p50"], x["lost_acked_writes"]]
             for x in s["replica_loss"]]), ""]
    if s["switchover"]:
        parts += ["## Planned switchover", "", md_table(
            ["mode", "runs", "stall p50 s", "stall p99 s", "client errors", "runs with errors",
             "lost acked writes"],
            [[x["mode"], x["runs"], x["stall_p50_s"], x["stall_p99_s"], x["errors"],
              x["runs_with_errors"], x["lost_acked_writes"]] for x in s["switchover"]]), ""]
    return "\n".join(parts).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="report", description=__doc__)
    ap.add_argument("--results", default="results")
    ap.add_argument("--no-write", action="store_true", help="print only, do not write SUMMARY.md")
    a = ap.parse_args(argv)
    rows = load_rows(Path(a.results))
    s = summarize(rows, load_errors(Path(a.results)))
    md = render_markdown(s)
    print(md)
    if not a.no_write:
        Path(a.results).mkdir(parents=True, exist_ok=True)
        (Path(a.results) / "SUMMARY.md").write_text(md)
        (Path(a.results) / "summary.json").write_text(json.dumps(s, indent=2, sort_keys=True) + "\n")
    return 0
