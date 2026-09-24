"""Fill the `[[N: <what>, <source>]]` number tags in the docs from results/*.jsonl.

Every tag is looked up by its `<what>` text in MAP (built below from the naming patterns the
docs use). A spec names the result files it reads (a glob under results/) and how the value
is computed. Values come from results/summary.json (what `bin/report` wrote) when that file
covers the same rows, otherwise from `report.summarize` over the rows, and metrics that
`bin/report` does not tabulate are computed from the raw rows here with the same
nearest-rank percentile.

  bin/fill-docs                 # --check: table of tag -> value -> status, changes nothing
  bin/fill-docs --write         # replace every resolvable tag in place

Statuses. `ok` resolved. `pending` mapped, but the rows it needs do not exist yet (or the
metric is null in them). `manual` cannot come from a result row (docker stats, ps). `unmapped`
no entry in MAP. Only `ok` tags are replaced, the rest stay in the docs and are listed.

Formatting. Seconds 2 decimals, ms 1 decimal, MB and MB/s 1 decimal, counts as integers.
The unit is appended only when the surrounding text lacks it: in a table cell when the
column header has no unit, in prose when the tag is not followed by one. A tag wrapped in
backticks loses the backticks when it is replaced. With no tags left to replace a second
--write changes nothing.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dbguard.harness.report import FAILOVER_SCENARIOS, MODES, percentile, summarize

DEFAULT_DOCS = ["README.md", "docs/DESIGN.md", "docs/CAPACITY.md", "docs/INTERVIEW.md",
                "docs/RUNBOOK.md"]
TAG_RE = re.compile(r"\[\[N: ([^\]]*)\]\]")
# literal examples of the tag syntax in the docs' own prose, never values
SKIP = {"...", "what, source file"}

UNITS = {"s": "s", "ms": "ms", "mb": "MB", "mbps": "MB/s", "count": "", "text": ""}
HEADER_UNIT_RE = {
    "s": re.compile(r"(?<![/\w])s\b"),
    "ms": re.compile(r"\bms\b"),
    "mb": re.compile(r"\bMB\b(?!/)"),
    "mbps": re.compile(r"MB/s"),
}
UNIT_AFTER_RE = re.compile(r"^[`*]*\s*(?:s|ms|MB/s|MB|us|seconds?|milliseconds?)\b")


# ---------------------------------------------------------------- context


class Ctx:
    """Rows by file name plus the report summary."""

    def __init__(self, results_dir: Path, root: Path):
        self.results_dir = Path(results_dir)
        self.root = Path(root)
        self.files: dict[str, list[dict]] = load_rows_by_file(self.results_dir)
        self.rows_all = [r for rs in self.files.values() for r in rs]
        self.summary, self.summary_origin = self._summary()

    def _summary(self) -> tuple[dict, str]:
        p = self.results_dir / "summary.json"
        if p.exists():
            try:
                s = json.loads(p.read_text())
            except json.JSONDecodeError:
                s = None
            newest = max((f.stat().st_mtime for f in self.results_dir.glob("*.jsonl")), default=0)
            # use it only when it describes exactly these rows (bin/report counts rows from
            # every *.jsonl, so a stale or differently filtered summary is recomputed)
            if s and s.get("rows") == len(self.rows_all) and p.stat().st_mtime >= newest:
                return s, "summary.json"
        return summarize(self.rows_all), "rows"

    def rows(self, glob: str) -> list[dict]:
        return [r for name, rs in self.files.items() if fnmatch.fnmatch(name, glob) for r in rs]

    def section(self, name: str, **match) -> dict | None:
        for x in self.summary.get(name) or []:
            if all(x.get(k) == v for k, v in match.items()):
                return x
        return None

    def fo(self, sc: str, mode: str, key: str):
        x = self.section("failover", scenario=sc, mode=mode)
        return None if x is None else x.get(key)

    def rj(self, sc: str, mode: str, key: str):
        x = self.section("rejoin", scenario=sc, mode=mode)
        return None if x is None else x.get(key)


def load_rows_by_file(results_dir: Path) -> dict[str, list[dict]]:
    """results/<scenario>_<mode>.jsonl rows, top level only (results/pilot/ and results/tmp/
    are ignored), without *.errors.jsonl and without rows that are error records."""
    out: dict[str, list[dict]] = {}
    for p in sorted(Path(results_dir).glob("*.jsonl")):
        if p.name.endswith(".errors.jsonl"):
            continue
        rows = []
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(r, dict) or "traceback" in r or "scenario" not in r:
                continue
            rows.append(r)
        out[p.name] = rows
    return out


# ---------------------------------------------------------------- specs


@dataclass
class Spec:
    glob: str                                   # result files under results/ it reads
    unit: str                                   # s ms mb mbps count text
    fn: Callable[[Ctx], Any] | None = None
    manual: str | None = None                   # reason, for values no row carries
    note: str = ""
    summary: bool = False                       # read from the report summary


def _fo(sc, mode, key, unit):
    return Spec(f"{sc}_{mode}.jsonl", unit, lambda c: c.fo(sc, mode, key), summary=True)


def _converged(sc, mode):
    def fn(c: Ctx):
        runs, conv = c.fo(sc, mode, "runs"), c.fo(sc, mode, "converged_runs")
        return None if runs is None else f"{conv}/{runs}"
    return Spec(f"{sc}_{mode}.jsonl", "text", fn, summary=True)


def _rj(sc, mode, key, unit):
    return Spec(f"{sc}_{mode}.jsonl", unit, lambda c: c.rj(sc, mode, key), summary=True)


def _events(rows: list[dict]) -> list[dict]:
    return [r["event"] for r in rows if isinstance(r.get("event"), dict)]


def _rejoins(rows: list[dict], branch: str | None = None) -> list[dict]:
    rj = [r["rejoin"] for r in rows if isinstance(r.get("rejoin"), dict)]
    return [j for j in rj if branch is None or j.get("branch") == branch]


def _p(values, q):
    return percentile(list(values), q)


def _nonempty(xs):
    return xs if xs else None


def _step(step: str, q: float, glob: str = "kill_dbguard.jsonl") -> Spec:
    def fn(c: Ctx):
        ds = [((e.get("steps") or {}).get(step) or {}).get("duration_s")
              for e in _events(c.rows(glob))]
        return _p(ds, q)
    return Spec(glob, "s", fn, note=f"event.steps.{step}.duration_s")


def _phantom_per_rebuild(sc, mode, agg):
    def fn(c: Ctx):
        ph = [int(j.get("phantom_gtids") or 0) for j in _rejoins(c.rows(f"{sc}_{mode}.jsonl"))
              if "rebuild" in (j.get("branch") or "")]
        if not ph:
            return None
        return max(ph) if agg == "max" else f"{sum(ph) / len(ph):.1f}"
    return Spec(f"{sc}_{mode}.jsonl", "count" if agg == "max" else "text", fn,
                note="rejoins with a rebuild branch only")


def _cost(ss: str, netem: int, key: str, unit: str) -> Spec:
    def fn(c: Ctx):
        x = c.section("cost", mode="dbguard", semisync=(ss == "on"), netem_ms=float(netem))
        return None if x is None else x.get(key)
    return Spec("cost_dbguard.jsonl", unit, fn, summary=True)


def _commit_cost(key: str) -> Spec:
    def fn(c: Ctx):
        on = c.section("cost", mode="dbguard", semisync=True, netem_ms=0.0)
        off = c.section("cost", mode="dbguard", semisync=False, netem_ms=0.0)
        if not on or not off or on.get(key) is None or off.get(key) is None:
            return None
        a, b = float(on[key]), float(off[key])
        return f"+{a - b:.1f} ms ({a:.1f} ms on vs {b:.1f} ms off)" if a >= b else \
            f"{a - b:.1f} ms ({a:.1f} ms on vs {b:.1f} ms off)"
    return Spec("cost_dbguard.jsonl", "text", fn, summary=True)


def _rl(key: str, unit: str) -> Spec:
    def fn(c: Ctx):
        x = c.section("replica_loss", mode="dbguard")
        return None if x is None else x.get(key)
    return Spec("replica-loss_dbguard.jsonl", unit, fn, summary=True)


def _rl_rows(unit: str, per_row: Callable[[dict], Any], note: str) -> Spec:
    return Spec("replica-loss_dbguard.jsonl", unit,
                lambda c: _p((per_row(r) for r in c.rows("replica-loss_dbguard.jsonl")), 50),
                note=note)


def _replace_ev(r: dict) -> dict:
    return r.get("event") if isinstance(r.get("event"), dict) else {}


def _post_clone_restart(r: dict):
    ev = _replace_ev(r)
    tot = (ev.get("rejoin") or {}).get("duration_s")
    clone = (ev.get("clone") or {}).get("duration_s")
    return (float(tot) - float(clone)) if tot is not None and clone is not None else None


def _sw(key: str, unit: str) -> Spec:
    def fn(c: Ctx):
        x = c.section("switchover", mode="dbguard")
        return None if x is None else x.get(key)
    return Spec("switchover_dbguard.jsonl", unit, fn, summary=True)


def _rejoin_duration(branch: str) -> Spec:
    return Spec("kill_dbguard.jsonl", "s", lambda c: _p(
        (j.get("duration_s") for j in _rejoins(c.rows("kill_dbguard.jsonl"), branch)), 50),
        note=f"rejoin.duration_s where branch={branch}")


def _fence_outcomes(rows: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for e in _events(rows):
        o = ((e.get("steps") or {}).get("fence") or {}).get("outcome")
        if o:
            out[o] = out.get(o, 0) + 1
    return out


def _fence_kill_of(c: Ctx):
    oc = _fence_outcomes(c.rows("partition-replicas_dbguard.jsonl"))
    n = oc.get("sql", 0) + oc.get("kill", 0)
    return f"{oc.get('kill', 0)} of {n} by kill, {oc.get('sql', 0)} by sql" if n else None


def _fenced(mode: str, with_outcome: bool) -> Spec:
    g = f"partition-replicas_{mode}.jsonl"

    def fn(c: Ctx):
        rows = c.rows(g)
        if not rows:
            return None
        fenced = sum(1 for r in rows if r.get("old_primary_fenced"))
        s = f"{fenced}/{len(rows)}"
        if with_outcome:
            oc = _fence_outcomes(rows)
            s += f" ({oc.get('sql', 0)} sql, {oc.get('kill', 0)} kill)"
        return s
    return Spec(g, "text", fn)


def _rejoin_split(c: Ctx):
    rep, reb = c.rj("kill", "dbguard", "repoint"), c.rj("kill", "dbguard", "rebuild")
    return None if rep is None else f"{rep} repoint, {reb} rebuild"


def _phantom_p50_max(c: Ctx):
    ph = [int(j.get("phantom_gtids") or 0) for j in _rejoins(c.rows("kill_dbguard.jsonl"), "rebuild")]
    return f"{_p(ph, 50):.0f} (median) and {max(ph)} (max)" if ph else None


def _dbguard_lost_except_kill_two(c: Ctx):
    rows = [r for r in c.rows("*_dbguard.jsonl") if r.get("scenario") != "kill-two"]
    return sum(int(r.get("lost_acked_writes") or 0) for r in rows) if rows else None


def _rs1_rows(c: Ctx):
    return [r for r in c.rows_all if (r.get("rs") or "rs1") == "rs1"]


def _disk_full(c: Ctx):
    rows = c.rows("disk-full_dbguard.jsonl")
    if not rows:
        return None
    n = len(rows)
    aborted = 0
    for r in rows:
        d = r.get("disk_full") or {}
        log = " ".join(d.get("mysqld_log") or [])
        if d.get("mysqld_alive_after") is False or "ABORT" in log or "Aborting" in log:
            aborted += 1
    failed_over = [r.get("failover_s") for r in rows if r.get("failover_s") is not None]
    parts = []
    if aborted:
        parts.append(f"mysqld aborted (binlog_error_action=ABORT_SERVER) in {aborted} of {n} runs")
    if n - aborted:
        parts.append(f"mysqld stayed up waiting on the full disk in {n - aborted} of {n} runs")
    fo = f"DBGuard failed over in {len(failed_over)} of {n}"
    if failed_over:
        fo += f" (median {_p(failed_over, 50):.2f} s)"
    return "; ".join(parts) + ", and " + fo


def _environment(c: Ctx):
    env = c.summary.get("environment") or []
    return "; ".join(env) if env else None


def _bugs_count(c: Ctx):
    p = c.root / "docs" / "BUGS.md"
    if not p.exists():
        return None
    return sum(1 for ln in p.read_text().splitlines() if ln.startswith("### "))


def build_map() -> dict[str, Spec]:
    m: dict[str, Spec] = {}
    # 1. "<scenario> <mode> <metric>", every failover scenario and mode
    for sc in FAILOVER_SCENARIOS + ["partition-manager"]:
        for mode in MODES:
            p = f"{sc} {mode} "
            m[p + "run count"] = _fo(sc, mode, "runs", "count")
            m[p + "failover p50"] = _fo(sc, mode, "failover_p50_s", "s")
            m[p + "failover p99"] = _fo(sc, mode, "failover_p99_s", "s")
            m[p + "lost acked writes"] = _fo(sc, mode, "lost_acked_writes", "count")
            m[p + "lost acked writes total"] = _fo(sc, mode, "lost_acked_writes", "count")
            m[p + "runs with loss"] = _fo(sc, mode, "runs_with_loss", "count")
            m[p + "phantom writes"] = _fo(sc, mode, "phantom_writes", "count")
            m[p + "single-writer violations"] = _fo(sc, mode, "single_writer_violations", "count")
            m[p + "converged runs"] = _converged(sc, mode)
            m[p + "writes on woken primary"] = _fo(sc, mode, "writes_on_woken_primary", "count")
            m[p + "rs2 changes"] = _fo(sc, mode, "rs2_state_changes", "count")
            m[p + "false failovers"] = _fo(sc, mode, "false_failovers", "count")
            m[p + "stall p50"] = _fo(sc, mode, "stall_p50_s", "s")
            m[p + "stall p99"] = _fo(sc, mode, "stall_p99_s", "s")
            # rejoin table
            m[p + "rejoins"] = _rj(sc, mode, "rejoins", "count")
            m[p + "rejoin repoint count"] = _rj(sc, mode, "repoint", "count")
            m[p + "rejoin rebuild count"] = _rj(sc, mode, "rebuild", "count")
            m[p + "rejoin other count"] = _rj(sc, mode, "other", "count")
            m[p + "phantom GTIDs mean"] = Spec(f"{sc}_{mode}.jsonl", "text", (
                lambda c, sc=sc, mode=mode: None if c.rj(sc, mode, "phantom_gtids_mean") is None
                else f"{float(c.rj(sc, mode, 'phantom_gtids_mean')):.1f}"), summary=True)
            m[p + "phantom GTIDs max"] = _rj(sc, mode, "phantom_gtids_max", "count")
            m[p + "rejoin p50"] = _rj(sc, mode, "rejoin_p50_s", "s")
            m[p + "rejoin p99"] = _rj(sc, mode, "rejoin_p99_s", "s")
            m[p + "phantom GTIDs per rebuild mean"] = _phantom_per_rebuild(sc, mode, "mean")
            m[p + "phantom GTIDs per rebuild max"] = _phantom_per_rebuild(sc, mode, "max")

    # 2. short forms: "<scenario short> <metric> [mode]", dbguard when no mode is named
    alias = {
        "kill run count": "kill dbguard run count",
        "kill failover p50": "kill dbguard failover p50",
        "kill failover p99": "kill dbguard failover p99",
        "kill lost acked writes total": "kill dbguard lost acked writes",
        "hang failover p50": "hang-container dbguard failover p50",
        "hang failover p99": "hang-container dbguard failover p99",
        "hang run count": "hang-container dbguard run count",
        "partition failover p50": "partition-replicas dbguard failover p50",
        "partition failover p99": "partition-replicas dbguard failover p99",
        "partition run count": "partition-replicas dbguard run count",
        "partition stall_s p50": "partition-replicas dbguard stall p50",
        "partition stall_s p99": "partition-replicas dbguard stall p99",
        "writes on woken primary total": "hang-container dbguard writes on woken primary",
    }
    for mode in ("naive", "orchestrator"):
        alias[f"kill run count {mode}"] = f"kill {mode} run count"
        alias[f"kill failover p50 {mode}"] = f"kill {mode} failover p50"
        alias[f"kill failover p99 {mode}"] = f"kill {mode} failover p99"
        alias[f"kill lost acked writes {mode}"] = f"kill {mode} lost acked writes"
    for mode in MODES:
        alias[f"false failovers {mode}"] = f"partition-manager {mode} false failovers"
    for a, target in alias.items():
        m[a] = m[target]

    # 3. cost, "cost <p50|p99|wps|runs> <on|off> <netem>ms", dbguard rows
    for ss in ("on", "off"):
        for n in (0, 2, 20):
            m[f"cost p50 {ss} {n}ms"] = _cost(ss, n, "commit_p50_ms", "ms")
            m[f"cost p99 {ss} {n}ms"] = _cost(ss, n, "commit_p99_ms", "ms")
            m[f"cost wps {ss} {n}ms"] = _cost(ss, n, "writes_per_s", "count")
            m[f"cost runs {ss} {n}ms"] = _cost(ss, n, "runs", "count")
    m["commit p50 semisync on vs off at 0 ms netem"] = _commit_cost("commit_p50_ms")
    m["commit p99 semisync on vs off at 0 ms netem"] = _commit_cost("commit_p99_ms")

    # 4. replica loss and replacement
    m["replica-loss dbguard run count"] = _rl("runs", "count")
    m["replica-loss dbguard lost acked writes"] = _rl("lost_acked_writes", "count")
    m["replica-loss clones"] = _rl("clones", "count")
    m["replica-loss resume p50"] = _rl("resume_p50_s", "s")
    m["stall_s with no semi-sync replica"] = _rl("stall_p50_s", "s")
    m["clone MB p50"] = _rl("clone_mb_p50", "mb")
    m["clone duration p50"] = _rl("clone_s_p50", "s")
    m["clone MB/s p50"] = _rl("clone_mb_per_s_p50", "mbps")
    m["dataset size MB"] = _rl_rows("mb", lambda r: r.get("dataset_mb"), "dataset_mb p50")
    m["post-clone restart p50"] = _rl_rows(
        "s", _post_clone_restart, "replace event rejoin.duration_s - clone.duration_s")
    m["time to replace p50"] = _rl_rows(
        "s", lambda r: (_replace_ev(r).get("rejoin") or {}).get("duration_s"),
        "replace event rejoin.duration_s: clone, restart and repoint (spare start not included)")

    # 5. switchover
    m["switchover run count"] = _sw("runs", "count")
    m["switchover stall p50"] = _sw("stall_p50_s", "s")
    m["switchover stall p99"] = _sw("stall_p99_s", "s")
    m["switchover stall_s p50"] = m["switchover stall p50"]
    m["switchover stall_s p99"] = m["switchover stall p99"]
    m["switchover client errors"] = _sw("errors", "count")
    m["switchover runs with errors"] = _sw("runs_with_errors", "count")
    m["switchover lost acked writes"] = _sw("lost_acked_writes", "count")

    # 6. from raw rows, not tabulated by bin/report
    m["choose step p99"] = _step("choose", 99)
    m["promote step p99"] = _step("promote", 99)
    m["repoint step p50"] = _step("repoint", 50)
    m["switchover prepare p50"] = _step("prepare", 50, "switchover_dbguard.jsonl")
    m["switchover prepare p99"] = _step("prepare", 99, "switchover_dbguard.jsonl")
    m["rejoin rebuild duration p50"] = _rejoin_duration("rebuild")
    m["rejoin repoint duration p50"] = _rejoin_duration("repoint")
    m["rejoin repoint count vs rebuild count"] = Spec("kill_dbguard.jsonl", "text", _rejoin_split,
                                                      summary=True)
    m["phantom GTIDs per rebuild p50 and max"] = Spec("kill_dbguard.jsonl", "text", _phantom_p50_max)
    m["fence outcome counts sql vs kill"] = Spec(
        "partition-replicas_dbguard.jsonl", "text", _fence_kill_of,
        note="kill-path fences out of sql+kill, event.steps.fence.outcome")
    m["partition-replicas dbguard runs with old primary fenced and fence outcome"] = _fenced("dbguard", True)
    m["partition-replicas naive runs with old primary fenced"] = _fenced("naive", False)
    m["dbguard lost acked writes total all scenarios except kill-two"] = Spec(
        "*_dbguard.jsonl", "count", _dbguard_lost_except_kill_two)
    m["total rs1 injections"] = Spec("*.jsonl", "count", lambda c: len(_rs1_rows(c)) or None,
                                     note="every row, cost runs included")
    m["rs2 state changes total across all rs1 injections"] = Spec(
        "*.jsonl", "count",
        lambda c: sum(int(r.get("rs2_state_changes") or 0) for r in _rs1_rows(c)) if _rs1_rows(c)
        else None)
    m["disk-full primary behaviour and failover outcome"] = Spec("disk-full_dbguard.jsonl", "text",
                                                                 _disk_full)
    m["host, Docker Desktop and MySQL version"] = Spec("*.jsonl", "text", _environment,
                                                       note="row host + mysql_version",
                                                       summary=True)
    m["BUGS.md entry count"] = Spec("", "count", _bugs_count, note="### headings in docs/BUGS.md")

    # 7. manual, no result row carries these
    stats = "docker stats --no-stream, idle and under the standard workload"
    for comp in ("node", "manager", "haproxy"):
        m[f"{comp} cpu loaded pct"] = Spec("", "text", manual=stats)
        m[f"{comp} mem loaded MB"] = Spec("", "text", manual=stats)
    m["node mem idle MB"] = Spec("", "text", manual=stats)
    m["manager mem MB"] = Spec("", "text", manual=stats)
    m["haproxy mem MB"] = Spec("", "text", manual=stats)
    m["agent RSS MB"] = Spec("", "text", manual="ps -o rss inside a node container")
    m["avg semi-sync wait us per netem level"] = Spec(
        "", "text", manual="Rpl_semi_sync_source_tx_avg_wait_time from the primary's /status "
                           "during each cost variant, not recorded in cost rows")
    m["client reconnect gap p50"] = Spec(
        "", "text", manual="no row field isolates the client's reconnect after HAProxy "
                           "switches; derive from ack logs kept with DBGUARD_KEEP_ACKS=1")
    return m


MAP = build_map()


# ---------------------------------------------------------------- scanning and formatting


@dataclass
class Tag:
    path: Path
    line_no: int          # 0-based
    start: int            # span of the tag within the line
    end: int
    body: str

    @property
    def what(self) -> str:
        return split_body(self.body)[0]

    @property
    def source(self) -> str | None:
        return split_body(self.body)[1]


def split_body(body: str) -> tuple[str, str | None]:
    """`kill failover p50, results/kill_dbguard.jsonl` -> (what, source). The source is the
    text after the last comma when it looks like a source (a path, docker stats, ps)."""
    if ", " in body:
        what, src = body.rsplit(", ", 1)
        if "/" in src or "." in src or src.startswith(("docker", "ps ")):
            return what.strip(), src.strip()
    return body.strip(), None


def scan(paths: list[Path]) -> list[Tag]:
    tags = []
    for p in paths:
        for i, line in enumerate(p.read_text().splitlines()):
            for mo in TAG_RE.finditer(line):
                if mo.group(1).strip() in SKIP:
                    continue
                tags.append(Tag(p, i, mo.start(), mo.end(), mo.group(1)))
    return tags


def fmt_value(v: Any, unit: str) -> str:
    if v is None:
        return "-"
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "yes" if v else "no"
    if unit == "s":
        return f"{float(v):.2f}"
    if unit in ("ms", "mb", "mbps"):
        return f"{float(v):.1f}"
    if unit == "count":
        return str(int(round(float(v))))
    return str(v)


def _table_header_cell(lines: list[str], line_no: int, col: int) -> str | None:
    i = line_no
    while i > 0 and lines[i - 1].lstrip().startswith("|"):
        i -= 1
    cells = lines[i].strip().strip("|").split("|")
    return cells[col].strip() if 0 <= col < len(cells) else None


def unit_in_context(lines: list[str], tag: Tag, unit: str) -> bool:
    """True when the text around the tag already carries a unit."""
    line = lines[tag.line_no]
    after = line[tag.end:]
    if UNIT_AFTER_RE.match(after):
        return True
    if line.lstrip().startswith("|"):
        col = line[:tag.start].count("|") - 1
        hdr = _table_header_cell(lines, tag.line_no, col)
        rx = HEADER_UNIT_RE.get(unit)
        if hdr and rx and rx.search(hdr):
            return True
    return False


def render(lines: list[str], tag: Tag, value: str, unit: str) -> str:
    u = UNITS.get(unit, "")
    if u and value != "-" and not unit_in_context(lines, tag, unit):
        return f"{value} {u}"
    return value


@dataclass
class Resolution:
    tag: Tag
    status: str            # ok pending manual unmapped
    value: str | None
    rendered: str | None
    origin: str


def resolve(tags: list[Tag], ctx: Ctx) -> list[Resolution]:
    out = []
    cache: dict[Path, list[str]] = {}
    for t in tags:
        lines = cache.setdefault(t.path, t.path.read_text().splitlines())
        spec = MAP.get(t.what)
        if spec is None:
            out.append(Resolution(t, "unmapped", None, None, ""))
            continue
        if spec.manual:
            out.append(Resolution(t, "manual", None, None, spec.manual))
            continue
        if spec.glob and not ctx.rows(spec.glob):
            out.append(Resolution(t, "pending", None, None, f"no rows in results/{spec.glob}"))
            continue
        try:
            v = spec.fn(ctx) if spec.fn else None
        except (TypeError, ValueError, KeyError) as e:
            out.append(Resolution(t, "pending", None, None, f"error: {e!r}"))
            continue
        if v is None:
            out.append(Resolution(t, "pending", None, None, f"null in results/{spec.glob}"))
            continue
        val = fmt_value(v, spec.unit)
        origin = ctx.summary_origin if spec.summary else "rows"
        if spec.note:
            origin += f" ({spec.note})"
        out.append(Resolution(t, "ok", val, render(lines, t, val, spec.unit), origin))
    return out


def rewrite(res: list[Resolution]) -> dict[Path, int]:
    """Replace every ok tag in place. A tag wrapped in backticks loses them."""
    changed: dict[Path, int] = {}
    by_file: dict[Path, list[Resolution]] = {}
    for r in res:
        if r.status == "ok":
            by_file.setdefault(r.tag.path, []).append(r)
    for path, rs in by_file.items():
        text = path.read_text()
        lines = text.split("\n")
        # right to left within a line so earlier spans stay valid
        for r in sorted(rs, key=lambda r: (r.tag.line_no, r.tag.start), reverse=True):
            ln = lines[r.tag.line_no]
            s, e = r.tag.start, r.tag.end
            assert ln[s:e] == f"[[N: {r.tag.body}]]", (path, r.tag.line_no)
            if s > 0 and e < len(ln) and ln[s - 1] == "`" and ln[e] == "`":
                s, e = s - 1, e + 1
            lines[r.tag.line_no] = ln[:s] + r.rendered + ln[e:]
        new = "\n".join(lines)
        if new != text:
            path.write_text(new)
            changed[path] = len(rs)
    return changed


def print_table(res: list[Resolution], root: Path, out=sys.stdout) -> None:
    def rel(p: Path) -> str:
        try:
            return str(p.resolve().relative_to(root.resolve()))
        except ValueError:
            return str(p)
    w = max((len(r.tag.what) for r in res), default=10)
    print(f"{'location':<24} {'tag':<{w}}  {'value':<14} {'status':<8} origin", file=out)
    for r in res:
        loc = f"{rel(r.tag.path)}:{r.tag.line_no + 1}"
        print(f"{loc:<24} {r.tag.what:<{w}}  {(r.rendered or '-'):<14} {r.status:<8} {r.origin}",
              file=out)
    counts: dict[str, int] = {}
    distinct: dict[str, set[str]] = {}
    for r in res:
        counts[r.status] = counts.get(r.status, 0) + 1
        distinct.setdefault(r.status, set()).add(r.tag.body)
    print("", file=out)
    print(f"{len(res)} tags ({len({r.tag.body for r in res})} distinct): " + ", ".join(
        f"{k} {counts[k]} ({len(distinct[k])} distinct)" for k in sorted(counts)), file=out)
    unm = sorted(distinct.get("unmapped", set()))
    if unm:
        print("unmapped:", file=out)
        for u in unm:
            print(f"  [[N: {u}]]", file=out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fill-docs", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--check", action="store_true", help="print tag -> value -> status (default)")
    g.add_argument("--write", action="store_true", help="replace every resolvable tag in place")
    ap.add_argument("--results", default="results", help="directory of <scenario>_<mode>.jsonl")
    ap.add_argument("--docs", nargs="+", default=DEFAULT_DOCS)
    ap.add_argument("--root", default=".", help="repo root (for docs/BUGS.md)")
    a = ap.parse_args(argv)
    root = Path(a.root)
    docs = [Path(d) if Path(d).is_absolute() else root / d for d in a.docs]
    ctx = Ctx(Path(a.results), root)
    res = resolve(scan(docs), ctx)
    print(f"rows: {len(ctx.rows_all)} from {sum(1 for v in ctx.files.values() if v)} files in "
          f"{a.results}, summary from {ctx.summary_origin}")
    print_table(res, root)
    if a.write:
        changed = rewrite(res)
        for p, n in changed.items():
            print(f"wrote {n} values into {p}")
        left = [r for r in res if r.status != "ok"]
        print(f"{len(left)} tags left in place")
    return 0
