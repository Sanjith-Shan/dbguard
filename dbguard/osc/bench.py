"""``dbgctl osc bench``, what an ALTER costs the foreground, measured on a throwaway server.

For each mode the table is rebuilt from a seed copy, and a writer (N threads of single-row
INSERTs and UPDATEs on the chaos.writes shape) runs before, during and after the change while
every operation's latency is recorded. Modes are ``osc`` (the copy path), ``osc-throttled``,
``inplace`` (MySQL's own online DDL), ``instant``, and ``osc-instant``, which shows the tool
picks INSTANT itself. Afterwards every acknowledged insert must be in the new table.
"""

from __future__ import annotations

import os
import random
import threading
import time
from collections.abc import Callable
from typing import Any

from dbguard.osc.runner import Options, Runner

DB = "osc_bench"
SEED = "writes_seed"
TABLE = "writes"
DDL = (f"CREATE TABLE IF NOT EXISTS {DB}.{SEED} (client_id INT NOT NULL, seq BIGINT NOT NULL, "
       "ts TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), run_id VARCHAR(64) NOT NULL, "
       "payload VARBINARY(512) NULL, PRIMARY KEY (client_id, seq, run_id)) ENGINE=InnoDB")


def load_seed(ex, rows: int, log: Callable[[str], None], batch: int = 50_000) -> float:
    """rows of (i % 100, i, 'load', 256 random bytes), generated server side with a recursive
    CTE in batches, so loading is not bound by the client."""
    ex.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
    ex.execute(DDL)
    have = int(ex.query(f"SELECT COUNT(*) AS n FROM {DB}.{SEED}")[0]["n"])
    if have == rows:
        log(f"bench: seed table already has {rows} rows")
        return 0.0
    ex.execute(f"TRUNCATE TABLE {DB}.{SEED}")
    ex.execute(f"SET SESSION cte_max_recursion_depth = {batch + 10}")
    t = time.monotonic()
    for start in range(0, rows, batch):
        n = min(batch, rows - start)
        ex.execute(
            f"INSERT INTO {DB}.{SEED} (client_id, seq, run_id, payload) "
            f"WITH RECURSIVE g(i) AS (SELECT {start} UNION ALL SELECT i + 1 FROM g "
            f"WHERE i < {start + n - 1}) SELECT i % 100, i, 'load', RANDOM_BYTES(256) FROM g")
    dt = time.monotonic() - t
    log(f"bench: loaded {rows} seed rows in {dt:.1f}s")
    return dt


def reset_table(ex, log: Callable[[str], None]) -> float:
    """Recreate the bench table from the seed, returning the seconds it took."""
    for t in (TABLE, "__osc_new_" + TABLE, "__osc_old_" + TABLE):
        ex.execute(f"DROP TABLE IF EXISTS {DB}.{t}")
    t = time.monotonic()
    ex.execute(f"CREATE TABLE {DB}.{TABLE} LIKE {DB}.{SEED}")
    ex.execute(f"INSERT INTO {DB}.{TABLE} SELECT * FROM {DB}.{SEED}")
    ex.execute(f"ANALYZE TABLE {DB}.{TABLE}")
    dt = time.monotonic() - t
    log(f"bench: table rebuilt from seed in {dt:.1f}s")
    return dt


class Writer:
    """threads x (50% INSERT of a new row, 50% UPDATE of a random seed row), autocommit.
    ops holds (start, end, ok, kind, thread) for every attempted statement."""

    def __init__(self, connect, threads: int, seed_rows: int, client_base: int = 1000):
        self.connect, self.threads, self.seed_rows = connect, threads, seed_rows
        self.client_base = client_base
        self.stop = threading.Event()
        self.ops: list[list[tuple]] = [[] for _ in range(threads)]
        self.inserted: list[list[int]] = [[] for _ in range(threads)]
        self.errors: list[str] = []
        self._th: list[threading.Thread] = []

    def _loop(self, tid: int) -> None:
        ex = self.connect()
        rnd = random.Random(tid)
        cid = self.client_base + tid
        seq = 0
        ops, ins = self.ops[tid], self.inserted[tid]
        try:
            while not self.stop.is_set():
                payload = os.urandom(256)
                if rnd.random() < 0.5:
                    seq += 1
                    kind = "i"
                    sql = (f"INSERT INTO {DB}.{TABLE} (client_id, seq, run_id, payload) "
                           "VALUES (%s, %s, 'bench', %s)")
                    args = (cid, seq, payload)
                else:
                    i = rnd.randrange(self.seed_rows)
                    kind = "u"
                    sql = (f"UPDATE {DB}.{TABLE} SET payload=%s, ts=NOW(6) "
                           "WHERE client_id=%s AND seq=%s AND run_id='load'")
                    args = (payload, i % 100, i)
                t0 = time.monotonic()
                try:
                    ex.execute(sql, args)
                    ok = True
                    if kind == "i":
                        ins.append(seq)
                except Exception as e:  # noqa: BLE001
                    ok = False
                    if len(self.errors) < 20:
                        self.errors.append(f"{kind}: {e}")
                    try:
                        ex.close()
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        ex = self.connect()
                    except Exception:  # noqa: BLE001
                        time.sleep(0.1)
                ops.append((t0, time.monotonic(), ok, kind, tid))
        finally:
            ex.close()

    def start(self) -> None:
        """Start the writer threads."""
        for i in range(self.threads):
            th = threading.Thread(target=self._loop, args=(i,), daemon=True)
            th.start()
            self._th.append(th)

    def join(self) -> None:
        """Stop the writers and wait for them."""
        self.stop.set()
        for th in self._th:
            th.join(timeout=30)

    def all_ops(self) -> list[tuple]:
        """Every recorded operation, ordered by completion time."""
        return sorted((o for lst in self.ops for o in lst), key=lambda o: o[1])


def pct(vals: list[float], p: float) -> float | None:
    """The ``p``-th percentile by rounded rank, None for no values."""
    if not vals:
        return None
    s = sorted(vals)
    k = min(len(s) - 1, max(0, int(round(p / 100.0 * (len(s) - 1)))))
    return s[k]


def window_stats(ops: list[tuple], t0: float, t1: float) -> dict[str, Any]:
    """Operations that completed inside [t0, t1], plus any that were in flight across it
    (so a statement blocked for the whole of a short window still counts)."""
    sel = [o for o in ops if (t0 <= o[1] <= t1) or (o[0] < t1 and o[1] > t1 and o[0] >= t0)
           or (o[0] <= t0 and o[1] >= t1)]
    done = [o for o in ops if t0 <= o[1] <= t1 and o[2]]
    lat = [(o[1] - o[0]) * 1000 for o in sel if o[2]]
    dur = t1 - t0
    return {
        "window_s": round(dur, 3),
        "ops_ok": len(done),
        "errors": sum(1 for o in sel if not o[2]),
        "qps": round(len(done) / dur, 1) if dur >= 1.0 else None,
        "p50_ms": round(pct(lat, 50), 2) if lat else None,
        "p99_ms": round(pct(lat, 99), 2) if lat else None,
        "max_ms": round(max(lat), 2) if lat else None,
    }


def settle(ex, log: Callable[[str], None], max_s: float = 120.0) -> float:
    """Waits until InnoDB has flushed most dirty pages and purged the history left by the
    table rebuild, so each mode's baseline starts from the same quiet state."""
    t = time.monotonic()
    while time.monotonic() - t < max_s:
        st = {r["Variable_name"]: int(r["Value"]) for r in ex.query(
            "SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_buffer_pool_pages_dirty',"
            "'Innodb_buffer_pool_pages_total')")}
        hist = int(ex.query("SELECT COUNT AS n FROM information_schema.INNODB_METRICS "
                            "WHERE NAME='trx_rseg_history_len'")[0]["n"])
        dirty = st.get("Innodb_buffer_pool_pages_dirty", 0)
        if dirty < 0.02 * max(1, st.get("Innodb_buffer_pool_pages_total", 1)) and hist < 1000:
            break
        time.sleep(1)
    dt = time.monotonic() - t
    log(f"bench: settled in {dt:.0f}s")
    return dt


def slow_ops(ops: list[tuple], c0: float, c1: float, timeline: list, threshold_ms: float = 250.0,
             ) -> dict[str, Any]:
    """Operations slower than threshold_ms that overlap the change, each attributed to the
    osc phase that was running when it started (timeline offsets are from c0, the change
    start, give or take the runner's connect time)."""
    marks = [("start", 0.0)] + [(n, float(t)) for n, t in timeline]
    by_phase: dict[str, int] = {}
    worst: list[tuple[float, str, float]] = []
    for o in ops:
        lat = (o[1] - o[0]) * 1000
        if lat < threshold_ms or o[1] < c0 or o[0] > c1:
            continue
        rel = o[0] - c0
        phase = "start"
        for n, t in marks:
            if rel >= t:
                phase = n
        by_phase[phase] = by_phase.get(phase, 0) + 1
        worst.append((round(lat, 1), phase, round(rel, 2)))
    worst.sort(reverse=True)
    return {"threshold_ms": threshold_ms, "count": len(worst), "by_phase": by_phase,
            "worst": worst[:5]}


def run_mode(connect, mode: str, *, rows: int, threads: int, phase_s: float, alter: str,
             instant_alter: str, log: Callable[[str], None],
             throttle_threads: int = 4) -> dict[str, Any]:
    """Rebuild the table, run one mode under load, and report latency by phase."""
    ex = connect()
    try:
        reset_s = reset_table(ex, log)
        settle(ex, log)
        size = ex.query("SELECT DATA_LENGTH AS d, INDEX_LENGTH AS i FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (DB, TABLE))[0]
    finally:
        ex.close()
    w = Writer(connect, threads, rows)
    w.start()
    log(f"bench[{mode}]: writer up, {phase_s:.0f}s baseline")
    time.sleep(phase_s)
    change: dict[str, Any] = {}
    c0 = time.monotonic()
    try:
        if mode in ("osc", "osc-instant", "osc-throttled"):
            # the tool's default: keep the old table, it is dropped after the measurement
            opts = Options(db=DB, table=TABLE,
                           alter=instant_alter if mode == "osc-instant" else alter,
                           allow_instant=(mode == "osc-instant"), drop_old=False,
                           max_load_threads=(throttle_threads if mode == "osc-throttled"
                                             else 20))
            r = Runner(opts, connect, log=lambda m: log(f"bench[{mode}] {m}"))
            change = r.run()
        elif mode == "inplace":
            ex = connect()
            try:
                ex.execute(f"ALTER TABLE {DB}.{TABLE} {alter}, ALGORITHM=INPLACE, LOCK=NONE")
            finally:
                ex.close()
        elif mode == "instant":
            ex = connect()
            try:
                ex.execute(f"ALTER TABLE {DB}.{TABLE} {instant_alter}, ALGORITHM=INSTANT")
            finally:
                ex.close()
        else:
            raise ValueError(f"unknown mode {mode}")
        change_ok, change_err = True, None
    except Exception as e:  # noqa: BLE001
        change_ok, change_err = False, f"{type(e).__name__}: {e}"
        log(f"bench[{mode}]: change failed, {change_err}")
    c1 = time.monotonic()
    log(f"bench[{mode}]: change took {c1 - c0:.2f}s, {phase_s:.0f}s after")
    time.sleep(phase_s)
    a1 = time.monotonic()
    w.join()
    ops = w.all_ops()
    drop_old_s = None
    if change.get("old_table"):
        ex = connect()
        try:
            t = time.monotonic()
            ex.execute(f"DROP TABLE IF EXISTS {DB}.{change['old_table']}")
            drop_old_s = round(time.monotonic() - t, 3)
        finally:
            ex.close()
    slow = slow_ops(ops, c0, c1, change.get("timeline") or [])
    first = min((o[0] for o in ops), default=c0)
    before = window_stats(ops, max(first, c0 - phase_s), c0)
    during = window_stats(ops, c0, c1)
    after = window_stats(ops, c1, a1)

    # the writer lost nothing: every acknowledged insert is in the final table
    ex = connect()
    try:
        got = {int(r["client_id"]): int(r["n"]) for r in ex.query(
            f"SELECT client_id, COUNT(*) AS n FROM {DB}.{TABLE} WHERE run_id='bench' "
            "GROUP BY client_id")}
        total = int(ex.query(f"SELECT COUNT(*) AS n FROM {DB}.{TABLE}")[0]["n"])
        cols = [r["COLUMN_NAME"] for r in ex.query(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND "
            "TABLE_NAME=%s", (DB, TABLE))]
    finally:
        ex.close()
    acked = {1000 + i: len(v) for i, v in enumerate(w.inserted)}
    missing = sum(max(0, n - got.get(c, 0)) for c, n in acked.items())

    def drop(b, d):
        if b.get("qps") and d.get("qps") is not None:
            return round(100.0 * (b["qps"] - d["qps"]) / b["qps"], 1)
        return None

    res = {
        "mode": mode, "change_ok": change_ok, "change_error": change_err,
        "change_s": round(c1 - c0, 3), "reset_s": round(reset_s, 1),
        "table_data_mb": round(int(size["d"]) / 2**20, 1),
        "table_index_mb": round(int(size["i"]) / 2**20, 1),
        "before": before, "during": during, "after": after,
        "qps_drop_during_pct": drop(before, during),
        "p99_during_vs_before": (round(during["p99_ms"] / before["p99_ms"], 2)
                                 if during.get("p99_ms") and before.get("p99_ms") else None),
        "writer_errors": w.errors[:5],
        "slow_ops_during": slow, "drop_old_after_s": drop_old_s,
        "acked_inserts": sum(acked.values()), "missing_inserts": missing,
        "final_rows": total, "final_columns": cols,
    }
    for k in ("method", "rows_copied", "chunks", "copy_mb_est", "copy_mb_s", "rows_s",
              "phases_s", "checksum_chunks", "checksum_mismatches", "swap_s", "instant_s",
              "final_chunk_size", "throttled_s", "total_s", "timeline"):
        if k in change:
            res["osc_" + k] = change[k]
    log(f"bench[{mode}]: before {before['qps']} qps p99 {before['p99_ms']} ms, during "
        f"{during['qps']} qps p99 {during['p99_ms']} ms max {during['max_ms']} ms, after "
        f"{after['qps']} qps, missing inserts {missing}")
    return res


def run_bench(connect, *, rows: int, threads: int, phase_s: float, modes: list[str],
              alter: str, instant_alter: str, log: Callable[[str], None],
              keep: bool = False, repeat: int = 1, throttle_threads: int = 4) -> dict[str, Any]:
    """Load the seed once, then run every mode ``repeat`` times and report them together."""
    ex = connect()
    try:
        info = ex.query("SELECT VERSION() AS v, @@innodb_buffer_pool_size AS bp, "
                        "@@binlog_format AS bf, @@gtid_mode AS gm, @@log_bin AS lb, "
                        "@@sync_binlog AS sb, @@innodb_flush_log_at_trx_commit AS fl")[0]
        load_s = load_seed(ex, rows, log)
    finally:
        ex.close()
    out: dict[str, Any] = {
        "server": {"version": info["v"], "buffer_pool_mb": int(info["bp"]) // 2**20,
                   "binlog_format": info["bf"], "gtid_mode": info["gm"],
                   "log_bin": int(info["lb"]), "sync_binlog": int(info["sb"]),
                   "innodb_flush_log_at_trx_commit": int(info["fl"])},
        "settings": {"rows": rows, "threads": threads, "phase_s": phase_s, "alter": alter,
                     "instant_alter": instant_alter, "load_s": round(load_s, 1),
                     "repeat": repeat, "throttle_threads": throttle_threads},
        "runs": [],
    }
    for i in range(repeat):
        for m in modes:
            r = run_mode(connect, m, rows=rows, threads=threads, phase_s=phase_s, alter=alter,
                         instant_alter=instant_alter, log=log, throttle_threads=throttle_threads)
            r["rep"] = i + 1
            out["runs"].append(r)
    if not keep:
        ex = connect()
        try:
            ex.execute(f"DROP DATABASE IF EXISTS {DB}")
        finally:
            ex.close()
    return out
