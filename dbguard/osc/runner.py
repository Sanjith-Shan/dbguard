"""The online schema change itself. See docs/OSC.md for the design and its limits.

Order of operations (copy path):
  preflight -> shadow (CREATE TABLE LIKE, ALTER) -> validate shadow -> triggers -> max PK ->
  chunked copy -> checksum -> RENAME TABLE swap -> drop triggers -> optionally drop old.
Everything runs on the primary as ordinary binlogged statements, so replicas apply the same
row changes and the same DDL through normal replication.
"""

from __future__ import annotations

import os
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from dbguard.osc.chunks import (
    ChunkSizer,
    boundary_sql,
    checksum_sql,
    chunk_ranges,
    copy_sql,
    max_pk_sql,
    row_tuple,
)
from dbguard.osc.db import error_code
from dbguard.osc.sqlutil import (
    MAX_TABLE_NAME,
    AlterSpec,
    InstantVerdict,
    instant_verdict,
    old_name,
    parse_alter,
    q,
    qt,
    shadow_name,
    trigger_names,
)
from dbguard.osc.table import Executor, TableInfo, load_table, osc_triggers

RETRYABLE = {1205, 1213}           # lock wait timeout, deadlock
INSTANT_REFUSED = {1845, 1846, 4092}  # not supported, not supported (reason), too many versions
DUP_KEY_WARNING = 1062


class OscError(RuntimeError):
    pass


@dataclass
class Options:
    db: str
    table: str
    alter: str
    chunk_size: int = 1000
    target_chunk_s: float = 0.1
    adaptive: bool = True
    max_lag_s: float = 2.0
    max_load_threads: int = 20
    dry_run: bool = False
    allow_instant: bool = True
    drop_old: bool = False
    allow_type_change: bool = False
    progress_interval_s: float = 2.0
    lock_wait_timeout_s: int = 2
    swap_attempts: int = 10
    track_progress: bool = True


# ------------------------------------------------------------------ preflight (pure)

@dataclass
class Preflight:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    instant: InstantVerdict | None = None

    @property
    def ok(self) -> bool:
        return not self.errors


def preflight(t: TableInfo, spec: AlterSpec, *, shadow: TableInfo | None = None,
              old: TableInfo | None = None, read_only: bool = False,
              datadir: str | None = None) -> Preflight:
    p = Preflight()
    if read_only:
        p.errors.append("server is read-only (super_read_only or read_only is 1). Point --host "
                        "at the primary, `dbgctl status` shows which node it is")
    if not t.exists:
        p.errors.append(f"table {t.db}.{t.name} does not exist")
        return p
    if len(t.name) > MAX_TABLE_NAME:
        p.errors.append(f"table name longer than {MAX_TABLE_NAME} characters, the shadow and "
                        "trigger names would not fit in 64")
    if (t.engine or "").upper() != "INNODB":
        p.errors.append(f"engine is {t.engine}, only InnoDB is supported")
    if not t.pk:
        p.errors.append("table has no PRIMARY KEY, the chunked copy and the triggers need one")
    leftover = osc_triggers(t)
    if leftover:
        p.errors.append("leftover osc triggers from an earlier run: " + ", ".join(leftover)
                        + ". Run `dbgctl osc cleanup` first")
    elif t.triggers:
        p.errors.append("table already has triggers (" + ", ".join(t.triggers) + "), this tool "
                        "adds its own and does not merge with existing ones")
    if shadow is not None and shadow.exists:
        p.errors.append(f"shadow table {shadow.name} exists from an earlier run. Run "
                        "`dbgctl osc cleanup` first")
    if old is not None and old.exists:
        p.errors.append(f"{old.name} exists from an earlier swap. Drop it or run "
                        "`dbgctl osc cleanup --old`")
    if t.fks:
        p.errors.append("table has or is referenced by foreign keys (" + ", ".join(t.fks)
                        + "), RENAME would leave them pointing at the old table")
    pk_lower = {c.lower() for c in t.pk}
    for c in spec.clauses:
        if c.kind in ("drop_pk", "add_pk"):
            p.errors.append(f"`{c.text}` changes the PRIMARY KEY, the copy needs a stable one")
        elif c.kind in ("rename_column", "rename_table"):
            p.errors.append(f"`{c.text}` renames, which the column mapping of the copy does not "
                            "follow. Rename in a separate native ALTER (it is INSTANT)")
        elif c.kind in ("drop_column", "modify", "change") and c.column \
                and c.column.lower() in pk_lower:
            p.errors.append(f"`{c.text}` touches primary key column {c.column}")
        elif c.kind == "add_index" and c.unique:
            p.warnings.append(f"`{c.text}` adds a UNIQUE index. INSERT IGNORE would drop "
                              "duplicate rows silently, the checksum's row counts catch it and "
                              "abort the change")
        elif c.kind == "add_column" and c.column and t.col(c.column):
            p.errors.append(f"column {c.column} already exists")
    size = t.data_bytes + t.index_bytes
    p.warnings.append(
        f"disk headroom not verified: the copy needs about {size / 2**20:.0f} MiB more "
        f"(table data+index {size / 2**20:.0f} MiB from information_schema, which is an "
        f"estimate) plus binlog for the copied rows, in datadir {datadir or '?'}. Free space "
        "cannot be read over SQL, check `df` on the host")
    p.instant = instant_verdict(spec, has_fulltext=t.has_fulltext, row_format=t.row_format,
                                total_row_versions=t.total_row_versions)
    return p


@dataclass
class ShadowCheck:
    errors: list[str]
    copy_cols: list[str]       # columns present and writable in both tables
    checksum_cols: list[str]   # copy_cols whose type did not change
    changed_type: list[str]


def validate_shadow(src: TableInfo, shadow: TableInfo, allow_type_change: bool) -> ShadowCheck:
    errors = []
    if [c.lower() for c in shadow.pk] != [c.lower() for c in src.pk]:
        errors.append(f"the ALTER changed the primary key from {src.pk} to {shadow.pk}")
    copy_cols, checksum_cols, changed = [], [], []
    for c in src.columns:
        s = shadow.col(c.name)
        if s is None or c.generated or s.generated:
            continue
        copy_cols.append(c.name)
        if s.column_type.lower() == c.column_type.lower():
            checksum_cols.append(c.name)
        else:
            changed.append(f"{c.name} {c.column_type} -> {s.column_type}")
    if changed and not allow_type_change:
        errors.append("the ALTER changes column types (" + "; ".join(changed) + "). A value "
                      "that does not fit would make the trigger fail the application's own "
                      "write. Pass --allow-type-change if every value fits")
    for s in shadow.columns:
        if src.col(s.name) is None and not s.nullable and s.default is None \
                and not s.generated and not s.auto_increment:
            errors.append(f"new column {s.name} is NOT NULL without a DEFAULT. The trigger's "
                          "REPLACE would fail application writes in strict mode, add a DEFAULT")
    return ShadowCheck(errors, copy_cols, checksum_cols, changed)


# ------------------------------------------------------------------ SQL builders

def trigger_sql(db: str, table: str, shadow: str, cols: list[str], pk: list[str]) -> dict[str, str]:
    """The three AFTER triggers (pt-osc shape). They run inside the writer's transaction, so
    a write to the source and its mirror in the shadow commit or roll back together."""
    names = trigger_names(table)
    src, dst = qt(db, table), qt(db, shadow)
    cl = ", ".join(q(c) for c in cols)
    new_vals = ", ".join(f"NEW.{q(c)}" for c in cols)
    old_match = " AND ".join(f"{dst}.{q(c)} <=> OLD.{q(c)}" for c in pk)
    pk_same = " AND ".join(f"OLD.{q(c)} <=> NEW.{q(c)}" for c in pk)
    replace = f"REPLACE INTO {dst} ({cl}) VALUES ({new_vals})"
    return {
        names["INSERT"]: (f"CREATE TRIGGER {qt(db, names['INSERT'])} AFTER INSERT ON {src} "
                          f"FOR EACH ROW {replace}"),
        names["UPDATE"]: (f"CREATE TRIGGER {qt(db, names['UPDATE'])} AFTER UPDATE ON {src} "
                          f"FOR EACH ROW BEGIN "
                          f"DELETE IGNORE FROM {dst} WHERE NOT ({pk_same}) AND {old_match}; "
                          f"{replace}; END"),
        names["DELETE"]: (f"CREATE TRIGGER {qt(db, names['DELETE'])} AFTER DELETE ON {src} "
                          f"FOR EACH ROW DELETE IGNORE FROM {dst} WHERE {old_match}"),
    }


def swap_sql(db: str, table: str) -> str:
    return (f"RENAME TABLE {qt(db, table)} TO {qt(db, old_name(table))}, "
            f"{qt(db, shadow_name(table))} TO {qt(db, table)}")


@dataclass
class ChunkSum:
    lo: tuple | None
    hi: tuple | None
    src_cnt: int
    src_crc: int
    dst_cnt: int
    dst_crc: int

    @property
    def match(self) -> bool:
        return self.src_cnt == self.dst_cnt and self.src_crc == self.dst_crc


def compare_checksums(sums: list[ChunkSum]) -> list[ChunkSum]:
    return [s for s in sums if not s.match]


# ------------------------------------------------------------------ progress row

PROGRESS_DDL = (
    "CREATE TABLE IF NOT EXISTS dbguard.osc_progress ("
    " db VARCHAR(64) NOT NULL, tbl VARCHAR(64) NOT NULL,"
    " phase VARCHAR(16) NOT NULL, method VARCHAR(8) NULL, alter_text TEXT NULL,"
    " rows_copied BIGINT NOT NULL DEFAULT 0, rows_est BIGINT NOT NULL DEFAULT 0,"
    " chunks INT NOT NULL DEFAULT 0, chunk_size INT NOT NULL DEFAULT 0,"
    " throttled_s DOUBLE NOT NULL DEFAULT 0, started TIMESTAMP(6) NULL,"
    " updated TIMESTAMP(6) NULL, owner VARCHAR(128) NULL, note TEXT NULL,"
    " PRIMARY KEY (db, tbl)) ENGINE=InnoDB")


class Progress:
    """One row in dbguard.osc_progress per table, upserted at most every progress interval
    plus at every phase change. Failures to write it are reported once and never abort."""

    def __init__(self, ex: Executor, db: str, table: str, enabled: bool, log: Callable):
        self.ex, self.db, self.table, self.enabled, self.log = ex, db, table, enabled, log
        self.warned = False

    def init(self, alter: str) -> None:
        if not self.enabled:
            return
        try:
            self.ex.execute("CREATE DATABASE IF NOT EXISTS dbguard")
            self.ex.execute(PROGRESS_DDL)
            self.ex.execute(
                "REPLACE INTO dbguard.osc_progress (db, tbl, phase, alter_text, started, updated,"
                " owner) VALUES (%s, %s, 'preflight', %s, NOW(6), NOW(6), %s)",
                (self.db, self.table, alter, f"{socket.gethostname()}:{os.getpid()}"))
        except Exception as e:  # noqa: BLE001
            self._warn(e)

    def update(self, **fields: Any) -> None:
        if not self.enabled or not fields:
            return
        sets = ", ".join(f"{k}=%s" for k in fields) + ", updated=NOW(6)"
        try:
            self.ex.execute(f"UPDATE dbguard.osc_progress SET {sets} WHERE db=%s AND tbl=%s",
                            (*fields.values(), self.db, self.table))
        except Exception as e:  # noqa: BLE001
            self._warn(e)

    def _warn(self, e: Exception) -> None:
        if not self.warned:
            self.log(f"warning: cannot write dbguard.osc_progress ({e}), continuing without it")
            self.warned = True


# ------------------------------------------------------------------ throttle

class Throttle:
    """Waits while Threads_running is above the limit or replica lag is above max_lag_s.
    lag_fn returns the worst replica lag in seconds, None when unknown (then it waits too,
    as pt-osc does), and is itself None when no --manager was given (lag not checked)."""

    def __init__(self, ex: Executor, max_threads: int, max_lag_s: float,
                 lag_fn: Callable[[], float | None] | None, log: Callable,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic, lag_every_s: float = 1.0):
        self.ex, self.max_threads, self.max_lag_s = ex, max_threads, max_lag_s
        self.lag_fn, self.log, self.sleep, self.clock = lag_fn, log, sleep, clock
        self.lag_every_s = lag_every_s
        self._lag_at, self._lag = -1e9, 0.0
        self.throttled_s = 0.0
        self.last_reason: str | None = None

    def threads_running(self) -> int:
        rows = self.ex.query("SHOW GLOBAL STATUS LIKE 'Threads_running'")
        return int(rows[0].get("Value") or rows[0].get("VALUE") or 0) if rows else 0

    def lag(self) -> float | None:
        if self.lag_fn is None:
            return 0.0
        now = self.clock()
        if now - self._lag_at >= self.lag_every_s:
            try:
                self._lag = self.lag_fn()
            except Exception:  # noqa: BLE001
                self._lag = None
            self._lag_at = now
        return self._lag

    def reason(self) -> str | None:
        tr = self.threads_running()
        if tr > self.max_threads:
            return f"Threads_running {tr} > {self.max_threads}"
        lag = self.lag()
        if lag is None:
            return "replica lag unknown (manager unreachable or a replica reports no lag)"
        if lag > self.max_lag_s:
            return f"replica lag {lag:.1f}s > {self.max_lag_s:g}s"
        return None

    def wait(self, tick: Callable[[], None] | None = None) -> float:
        start = self.clock()
        while True:
            r = self.reason()
            if r is None:
                break
            if r != self.last_reason:
                self.log(f"throttle: {r}, pausing")
            self.last_reason = r
            if tick:
                tick()
            self.sleep(0.25)
        if self.last_reason is not None and self.clock() > start:
            self.log("throttle: resumed")
        self.last_reason = None
        waited = self.clock() - start
        self.throttled_s += waited
        return waited


def manager_lag_fn(manager_url: str, rs: str, timeout: float = 1.0) -> Callable[[], float | None]:
    """Worst lag_s over the set's replicas from GET /v1/sets/{rs}. A replica with lag_s null
    makes the answer unknown. Nodes that are down, fenced or spare are not replicas."""
    from dbguard.cli.client import ManagerClient

    mc = ManagerClient(manager_url, timeout=timeout)

    def fn() -> float | None:
        st = mc.set(rs)
        worst = 0.0
        for name, n in (st.get("nodes") or {}).items():
            if name == st.get("primary") or n.get("role") != "replica":
                continue
            if n.get("lag_s") is None:
                return None
            worst = max(worst, float(n["lag_s"]))
        return worst
    return fn


# ------------------------------------------------------------------ the runner

def _retry(fn: Callable[[], Any], attempts: int = 10, sleep: Callable = time.sleep) -> Any:
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            if error_code(e) in RETRYABLE and i < attempts - 1:
                sleep(0.05 * (i + 1))
                continue
            raise


class Runner:
    def __init__(self, opts: Options, connect: Callable[[], Executor], *,
                 lag_fn: Callable[[], float | None] | None = None,
                 log: Callable[[str], None] = print,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.o, self.connect, self.lag_fn = opts, connect, lag_fn
        self.log, self.clock, self.sleep = log, clock, sleep
        self.ex: Executor | None = None
        self.spec: AlterSpec = parse_alter(opts.alter)
        self.shadow = shadow_name(opts.table)
        self.triggers = trigger_names(opts.table)
        self.created_shadow = False
        self.created_triggers: list[str] = []
        self.swapped = False
        self.result: dict[str, Any] = {"db": opts.db, "table": opts.table, "alter": opts.alter,
                                       "dry_run": opts.dry_run}
        self.progress: Progress | None = None

    # -- helpers
    def _server_facts(self) -> tuple[bool, str | None]:
        r = self.ex.query("SELECT @@GLOBAL.super_read_only AS sro, @@GLOBAL.read_only AS ro, "
                          "@@GLOBAL.datadir AS datadir")[0]
        return bool(int(r["sro"]) or int(r["ro"])), r["datadir"]

    def _t(self, name: str) -> TableInfo:
        return load_table(self.ex, self.o.db, name)

    # -- entry point
    def run(self) -> dict[str, Any]:
        t0 = self.clock()
        self.ex = self.connect()
        try:
            self._run()
            self.result["ok"] = True
        except BaseException as e:
            self.result["ok"] = False
            self.result["error"] = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            self.log(f"osc: aborting, {self.result['error']}")
            self._cleanup_after_failure()
            raise
        finally:
            self.result["total_s"] = round(self.clock() - t0, 3)
            if self.ex is not None and hasattr(self.ex, "close"):
                self.ex.close()
        return self.result

    def _run(self) -> None:
        o, ex = self.o, self.ex
        ex.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        ex.execute(f"SET SESSION lock_wait_timeout = {int(o.lock_wait_timeout_s)}")
        ex.execute("SET SESSION innodb_lock_wait_timeout = 10")
        # information_schema caches table sizes for a day by default, read them fresh
        ex.execute("SET SESSION information_schema_stats_expiry = 0")
        read_only, datadir = self._server_facts()
        src = self._t(o.table)
        pf = preflight(src, self.spec, shadow=self._t(self.shadow),
                       old=self._t(old_name(o.table)), read_only=read_only, datadir=datadir)
        self.result["preflight"] = {"errors": pf.errors, "warnings": pf.warnings,
                                    "instant": pf.instant.ok if pf.instant else False,
                                    "instant_reason": pf.instant.reason if pf.instant else None,
                                    "rows_est": src.rows_est,
                                    "data_bytes": src.data_bytes, "index_bytes": src.index_bytes}
        for w in pf.warnings:
            self.log(f"preflight warning: {w}")
        if not pf.ok:
            for e in pf.errors:
                self.log(f"preflight error: {e}")
            raise OscError("preflight failed: " + "; ".join(pf.errors))
        self.log(f"preflight ok: {o.db}.{o.table} pk=({', '.join(src.pk)}) "
                 f"~{src.rows_est} rows, INSTANT {'possible' if pf.instant.ok else 'no'} "
                 f"({pf.instant.reason})")

        if o.dry_run:
            self._dry_run(src, pf)
            return
        self.progress = Progress(ex, o.db, o.table, o.track_progress, self.log)
        self.progress.init(o.alter)

        if pf.instant.ok and o.allow_instant:
            if self._try_instant():
                return
        self._copy_path(src)

    def _dry_run(self, src: TableInfo, pf: Preflight) -> None:
        """Creates the empty shadow, applies the ALTER to it to prove it parses and to see
        the resulting columns, then drops it. No triggers, no copy, no swap."""
        o = self.o
        plan = "instant" if (pf.instant.ok and o.allow_instant) else "copy"
        self._make_shadow()
        sh = self._t(self.shadow)
        chk = validate_shadow(src, sh, o.allow_type_change)
        self._drop_shadow()
        self.result.update(method=plan, shadow_errors=chk.errors, copy_cols=chk.copy_cols,
                           checksum_cols=chk.checksum_cols,
                           new_columns=[c.name for c in sh.columns if not src.col(c.name)])
        self.log(f"dry run: would use {plan}. ALTER applied cleanly to an empty shadow"
                 + (" but: " + "; ".join(chk.errors) if chk.errors else "")
                 + f". Copy columns {len(chk.copy_cols)}, checksum columns "
                   f"{len(chk.checksum_cols)}. Nothing else was changed")
        if chk.errors:
            raise OscError("dry run: " + "; ".join(chk.errors))

    def _try_instant(self) -> bool:
        o = self.o
        self.progress.update(phase="instant", method="instant")
        sql = f"ALTER TABLE {qt(o.db, o.table)} {o.alter}, ALGORITHM=INSTANT"
        self.log(f"instant: {sql}")
        t = self.clock()
        try:
            self.ex.execute(sql)
        except Exception as e:  # noqa: BLE001
            if error_code(e) in INSTANT_REFUSED:
                self.log(f"instant: MySQL refused ({e}), falling back to the copy")
                self.result["instant_refused"] = str(e)
                return False
            raise
        dt = self.clock() - t
        self.result.update(method="instant", instant_s=round(dt, 4))
        self.progress.update(phase="done", note=f"ALGORITHM=INSTANT in {dt:.3f}s")
        self.log(f"instant: done in {dt * 1000:.1f} ms, metadata only, no rows copied")
        return True

    def _make_shadow(self) -> None:
        o = self.o
        self.ex.execute(f"CREATE TABLE {qt(o.db, self.shadow)} LIKE {qt(o.db, o.table)}")
        self.created_shadow = True
        self.ex.execute(f"ALTER TABLE {qt(o.db, self.shadow)} {o.alter}")

    def _drop_shadow(self) -> None:
        self.ex.execute(f"DROP TABLE IF EXISTS {qt(self.o.db, self.shadow)}")
        self.created_shadow = False

    def _copy_path(self, src: TableInfo) -> None:
        o, ex = self.o, self.ex
        self.result["method"] = "copy"
        self.progress.update(phase="shadow", method="copy", rows_est=src.rows_est)
        phases: dict[str, float] = {}
        self.result["phases_s"] = phases

        t = self.clock()
        self._make_shadow()
        sh = self._t(self.shadow)
        chk = validate_shadow(src, sh, o.allow_type_change)
        if chk.errors:
            raise OscError("; ".join(chk.errors))
        self.result.update(copy_cols=chk.copy_cols, checksum_cols=chk.checksum_cols,
                           changed_type=chk.changed_type)
        for name, sql in trigger_sql(o.db, o.table, self.shadow, chk.copy_cols, src.pk).items():
            # record before executing: a CREATE that timed out may still have happened
            self.created_triggers.append(name)
            _retry(lambda s=sql: ex.execute(s), sleep=self.sleep)
        phases["shadow_triggers"] = round(self.clock() - t, 3)
        self.log(f"shadow {self.shadow} created and altered, triggers "
                 f"{', '.join(self.created_triggers)} in place")

        t = self.clock()
        boundaries, copied = self._copy(src, chk.copy_cols)
        copy_s = self.clock() - t
        phases["copy"] = round(copy_s, 3)
        mb = (src.data_bytes + 0.0) / 2**20
        self.result.update(rows_copied=copied, chunks=len(boundaries),
                           copy_mb_est=round(mb, 1),
                           copy_mb_s=round(mb / copy_s, 2) if copy_s > 0 else None,
                           rows_s=round(copied / copy_s) if copy_s > 0 else None)

        t = self.clock()
        self.progress.update(phase="checksum", rows_copied=copied, chunks=len(boundaries))
        sums = self._checksum(src.pk, chk.checksum_cols, chunk_ranges(boundaries))
        bad = compare_checksums(sums)
        phases["checksum"] = round(self.clock() - t, 3)
        self.result.update(checksum_chunks=len(sums), checksum_mismatches=len(bad))
        if bad:
            for s in bad[:5]:
                self.log(f"checksum mismatch in ({s.lo}, {s.hi}]: source {s.src_cnt} rows "
                         f"crc {s.src_crc}, shadow {s.dst_cnt} rows crc {s.dst_crc}")
            raise OscError(f"checksum mismatch in {len(bad)} of {len(sums)} chunks")
        self.log(f"checksum: {len(sums)} chunks match "
                 f"({len(chk.checksum_cols)} columns compared, counts on all)")

        t = self.clock()
        self.progress.update(phase="swap")
        self._swap()
        phases["swap"] = round(self.clock() - t, 3)
        self._drop_triggers_after_swap()
        old = old_name(o.table)
        if o.drop_old:
            ex.execute(f"DROP TABLE {qt(o.db, old)}")
            self.result["old_table"] = None
            self.log(f"dropped {old}")
        else:
            self.result["old_table"] = old
            self.log(f"kept the original as {o.db}.{old}. Drop it with "
                     f"`DROP TABLE {qt(o.db, old)}` or `dbgctl osc cleanup --old`")
        self.progress.update(phase="done", rows_copied=copied)

    def _copy(self, src: TableInfo, cols: list[str]) -> tuple[list[tuple], int]:
        o, ex = self.o, self.ex
        row = ex.query(max_pk_sql(o.db, o.table, src.pk))
        if not row:
            self.log("copy: source is empty, the triggers carry any new rows")
            return [], 0
        max_pk = row_tuple(row[0], src.pk)
        sizer = ChunkSizer(size=o.chunk_size, target_s=o.target_chunk_s, adaptive=o.adaptive)
        throttle = Throttle(ex, o.max_load_threads, o.max_lag_s, self.lag_fn, self.log,
                            sleep=self.sleep, clock=self.clock)
        self._throttle = throttle
        lo: tuple | None = None
        boundaries: list[tuple] = []
        copied = 0
        start = last_tick = self.clock()
        est = max(src.rows_est, 1)

        def tick(force: bool = False) -> None:
            nonlocal last_tick
            now = self.clock()
            if not force and now - last_tick < o.progress_interval_s:
                return
            last_tick = now
            el = now - start
            rate = copied / el if el > 0 else 0
            pct = min(100.0, 100.0 * copied / est)
            eta = (est - copied) / rate if rate > 0 and copied < est else 0
            self.log(f"copy: {copied}/{est} rows (~{pct:.1f}%) chunk={sizer.size} "
                     f"{rate:.0f} rows/s eta ~{eta:.0f}s throttled {throttle.throttled_s:.1f}s")
            self.progress.update(phase="copy", rows_copied=copied, chunks=len(boundaries),
                                 chunk_size=sizer.size, throttled_s=round(throttle.throttled_s, 2))

        while True:
            throttle.wait(tick)
            t = self.clock()
            size = sizer.size
            bsql, bargs = boundary_sql(o.db, o.table, src.pk, lo, max_pk, size)
            b = ex.query(bsql, bargs)
            last = not b
            hi = max_pk if last else row_tuple(b[0], src.pk)
            csql, cargs = copy_sql(o.db, o.table, self.shadow, cols, src.pk, lo, hi)
            n = _retry(lambda: ex.execute(csql, cargs), sleep=self.sleep)
            self._check_warnings()
            copied += n
            boundaries.append(hi)
            lo = hi
            sizer.update(self.clock() - t, 0 if last else size)
            tick()
            if last:
                break
        tick(force=True)
        self.result["throttled_s"] = round(throttle.throttled_s, 2)
        self.result["final_chunk_size"] = sizer.size
        return boundaries, copied

    def _check_warnings(self) -> None:
        bad = [w for w in self.ex.query("SHOW WARNINGS")
               if int(w.get("Code") or 0) != DUP_KEY_WARNING]
        if bad:
            w = bad[0]
            raise OscError(f"copy produced warning {w.get('Code')} ({w.get('Message')}), a value "
                           "was changed on the way into the shadow")

    def _checksum(self, pk: list[str], cols: list[str], ranges) -> list[ChunkSum]:
        o, ex = self.o, self.ex
        out = []
        for lo, hi in ranges:
            ssql, sargs = checksum_sql(o.db, o.table, cols, pk, lo, hi)
            dsql, dargs = checksum_sql(o.db, self.shadow, cols, pk, lo, hi)

            def one():
                ex.execute("BEGIN")
                try:
                    s = ex.query(ssql, sargs)[0]
                    d = ex.query(dsql, dargs)[0]
                    ex.execute("COMMIT")
                except BaseException:
                    try:
                        ex.execute("ROLLBACK")
                    except Exception:  # noqa: BLE001
                        pass
                    raise
                return s, d
            s, d = _retry(one, sleep=self.sleep)
            out.append(ChunkSum(lo, hi, int(s["cnt"]), int(s["crc"]), int(d["cnt"]),
                                int(d["crc"])))
        return out

    def _swap(self) -> None:
        """One RENAME TABLE statement swaps both names atomically (MySQL takes exclusive
        metadata locks on all tables first). It waits for open transactions on the source,
        and new writers queue behind it, so lock_wait_timeout is short and the swap is
        retried rather than letting a long transaction stall the application."""
        o = self.o
        sql = swap_sql(o.db, o.table)
        for i in range(o.swap_attempts):
            t = self.clock()
            try:
                self.ex.execute(sql)
                self.swapped = True
                self.result["swap_s"] = round(self.clock() - t, 4)
                self.result["swap_attempts"] = i + 1
                self.log(f"swap: {sql} in {(self.clock() - t) * 1000:.1f} ms")
                return
            except Exception as e:  # noqa: BLE001
                if error_code(e) == 1205 and i < o.swap_attempts - 1:
                    self.log(f"swap: metadata lock wait timed out (attempt {i + 1}), retrying")
                    self.sleep(0.5)
                    continue
                raise

    def _drop_triggers_after_swap(self) -> None:
        o = self.o
        old = old_name(o.table)
        for name in self.triggers.values():
            _retry(lambda n=name: self.ex.execute(f"DROP TRIGGER IF EXISTS {qt(o.db, n)}"),
                   sleep=self.sleep)
        left_new = osc_triggers(self._t(o.table))
        left_old = osc_triggers(self._t(old))
        if left_new or left_old:
            raise OscError(f"triggers still present after drop: {left_new + left_old}")
        self.created_triggers = []
        self.log("triggers dropped (they had moved to the old table with the rename), verified")

    def _cleanup_after_failure(self) -> None:
        """Triggers first, then the shadow: dropping the shadow while the triggers still
        point at it would fail every application write. After the swap the shadow is the
        live table and is never dropped here."""
        o = self.o
        ex = self.ex
        old_tid = ex.thread_id() if hasattr(ex, "thread_id") else None
        try:
            ex = self.connect()
            if old_tid:
                try:
                    ex.execute(f"KILL {int(old_tid)}")
                except Exception:  # noqa: BLE001
                    pass
        except Exception as e:  # noqa: BLE001
            self.log(f"cleanup: cannot reconnect ({e}), trying the old connection")
            ex = self.ex
        done = []
        try:
            ex.execute("SET SESSION lock_wait_timeout = 30")
            for name in list(self.created_triggers):
                ex.execute(f"DROP TRIGGER IF EXISTS {qt(o.db, name)}")
                done.append(f"trigger {name}")
            if self.created_shadow and not self.swapped:
                ex.execute(f"DROP TABLE IF EXISTS {qt(o.db, self.shadow)}")
                done.append(f"table {self.shadow}")
            if done:
                self.log("cleanup: dropped " + ", ".join(done))
            self.result["cleaned_up"] = done
            if self.progress:
                self.progress.ex = ex  # the run's own connection may be dead
                self.progress.update(phase="failed", note=self.result.get("error", "")[:1000])
        except Exception as e:  # noqa: BLE001
            self.log(f"cleanup failed ({e}). Run `dbgctl osc cleanup --db {o.db} "
                     f"--table {o.table}`")
            self.result["cleanup_error"] = str(e)
        finally:
            if ex is not self.ex and hasattr(ex, "close"):
                ex.close()


def cleanup(ex: Executor, db: str, table: str, *, drop_old: bool = False,
            log: Callable[[str], None] = print) -> list[str]:
    """Removes what a failed or interrupted run left behind, triggers before the shadow.
    Safe to run twice."""
    done = []
    ex.execute("SET SESSION lock_wait_timeout = 30")
    for t in (table, old_name(table)):
        info = load_table(ex, db, t)
        for trg in osc_triggers(info):
            ex.execute(f"DROP TRIGGER IF EXISTS {qt(db, trg)}")
            done.append(f"trigger {trg}")
    if load_table(ex, db, shadow_name(table)).exists:
        ex.execute(f"DROP TABLE IF EXISTS {qt(db, shadow_name(table))}")
        done.append(f"table {shadow_name(table)}")
    if drop_old and load_table(ex, db, old_name(table)).exists:
        ex.execute(f"DROP TABLE IF EXISTS {qt(db, old_name(table))}")
        done.append(f"table {old_name(table)}")
    try:
        ex.execute("DELETE FROM dbguard.osc_progress WHERE db=%s AND tbl=%s", (db, table))
    except Exception:  # noqa: BLE001
        pass
    log("cleanup: " + (", ".join(done) if done else "nothing to remove"))
    return done


def read_progress(ex: Executor, db: str | None = None) -> list[dict[str, Any]]:
    sql = ("SELECT db, tbl, phase, method, rows_copied, rows_est, chunks, chunk_size, "
           "throttled_s, started, updated, owner, note, "
           "TIMESTAMPDIFF(MICROSECOND, updated, NOW(6)) / 1e6 AS age_s "
           "FROM dbguard.osc_progress")
    try:
        if db:
            return ex.query(sql + " WHERE db=%s ORDER BY updated DESC", (db,))
        return ex.query(sql + " ORDER BY updated DESC")
    except Exception as e:  # noqa: BLE001
        if error_code(e) in (1146, 1049):  # table or database missing, nothing ever ran
            return []
        raise
