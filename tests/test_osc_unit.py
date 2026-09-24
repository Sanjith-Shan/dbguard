"""dbguard.osc without MySQL: alter parsing, preflight decisions, chunk boundaries (composite
keys included), checksum comparison, and the runner's cleanup on failure against a fake
executor that models just enough of a server."""

from __future__ import annotations

import re

import pymysql
import pytest

from dbguard.osc.chunks import (
    ChunkSizer,
    boundary_sql,
    checksum_sql,
    chunk_ranges,
    copy_sql,
    range_predicate,
)
from dbguard.osc.runner import (
    ChunkSum,
    Options,
    OscError,
    Runner,
    Throttle,
    compare_checksums,
    preflight,
    trigger_sql,
    validate_shadow,
)
from dbguard.osc.sqlutil import instant_verdict, parse_alter, split_top_level
from dbguard.osc.table import Column, TableInfo


# ------------------------------------------------------------------ alter parsing

def test_split_top_level_respects_parens_and_quotes():
    parts = split_top_level("ADD COLUMN a VARCHAR(10) DEFAULT 'x,y', ADD INDEX k (a, b)")
    assert parts == ["ADD COLUMN a VARCHAR(10) DEFAULT 'x,y'", "ADD INDEX k (a, b)"]


def test_parse_alter_kinds():
    s = parse_alter("ADD COLUMN note VARCHAR(32) NULL, ADD INDEX idx_ts (ts), "
                    "ADD UNIQUE KEY u (a), DROP COLUMN old, MODIFY b INT, "
                    "CHANGE c d INT, CHANGE e e BIGINT, DROP PRIMARY KEY, DROP INDEX k2")
    kinds = [(c.kind, c.column) for c in s.clauses]
    assert kinds == [("add_column", "note"), ("add_index", None), ("add_index", None),
                     ("drop_column", "old"), ("modify", "b"), ("rename_column", "c"),
                     ("change", "e"), ("drop_pk", None), ("drop_index", None)]
    assert s.clauses[2].unique and not s.clauses[1].unique


def test_parse_alter_after_inside_string_is_not_positioned():
    s = parse_alter("ADD COLUMN x VARCHAR(10) DEFAULT 'after first'")
    assert s.clauses[0].kind == "add_column" and not s.clauses[0].positioned
    s = parse_alter("ADD x INT AFTER seq")
    assert s.clauses[0].positioned and s.clauses[0].column == "x"
    s = parse_alter("ADD COLUMN `we``ird` INT")
    assert s.clauses[0].column == "we`ird"


def test_parse_alter_rejects_full_statement():
    with pytest.raises(ValueError):
        parse_alter("ALTER TABLE t ADD COLUMN x INT")


def test_instant_verdict():
    ok = instant_verdict(parse_alter("ADD COLUMN a INT NULL, ADD COLUMN b INT"),
                         has_fulltext=False, row_format="Dynamic", total_row_versions=0)
    assert ok.ok
    for alter, kw in [
        ("ADD COLUMN a INT, ADD INDEX i (a)", {}),
        ("ADD COLUMN a INT FIRST", {}),
        ("ADD COLUMN a INT", {"has_fulltext": True}),
        ("ADD COLUMN a INT", {"row_format": "Compressed"}),
        ("ADD COLUMN a INT", {"total_row_versions": 64}),
        ("ADD COLUMN a INT, ALGORITHM=INPLACE", {}),
        ("ADD COLUMN g INT AS (a + 1)", {}),
    ]:
        args = {"has_fulltext": False, "row_format": "Dynamic", "total_row_versions": 3} | kw
        assert not instant_verdict(parse_alter(alter), **args).ok, alter


# ------------------------------------------------------------------ preflight

def cols(*spec):
    return [Column(n, t, True, None, "") for n, t in spec]


def writes_table(**kw) -> TableInfo:
    base = dict(db="chaos", name="writes", engine="InnoDB", row_format="Dynamic",
                columns=cols(("client_id", "int"), ("seq", "bigint"), ("ts", "timestamp(6)"),
                             ("run_id", "varchar(64)"), ("payload", "varbinary(512)")),
                pk=["client_id", "seq", "run_id"], rows_est=1000, data_bytes=10 << 20,
                total_row_versions=0)
    base.update(kw)
    return TableInfo(**base)


def test_preflight_ok_and_instant():
    p = preflight(writes_table(), parse_alter("ADD COLUMN note VARCHAR(32) NULL"))
    assert p.ok and p.instant.ok
    assert any("disk headroom not verified" in w for w in p.warnings)


@pytest.mark.parametrize("table_kw,alter,needle", [
    ({"pk": []}, "ADD COLUMN x INT", "no PRIMARY KEY"),
    ({"triggers": ["audit_ins"]}, "ADD COLUMN x INT", "already has triggers"),
    ({"triggers": ["__osc_ins_writes"]}, "ADD COLUMN x INT", "osc cleanup"),
    ({"fks": ["fk1"]}, "ADD COLUMN x INT", "foreign keys"),
    ({"engine": "MyISAM"}, "ADD COLUMN x INT", "only InnoDB"),
    ({}, "DROP PRIMARY KEY", "PRIMARY KEY"),
    ({}, "DROP COLUMN seq", "primary key column seq"),
    ({}, "MODIFY run_id VARCHAR(128) NOT NULL", "primary key column run_id"),
    ({}, "RENAME COLUMN payload TO body", "renames"),
    ({}, "ADD COLUMN payload INT", "already exists"),
    ({"name": "t" * 60}, "ADD COLUMN x INT", "longer than"),
])
def test_preflight_errors(table_kw, alter, needle):
    p = preflight(writes_table(**table_kw), parse_alter(alter))
    assert not p.ok
    assert any(needle in e for e in p.errors), p.errors


def test_preflight_leftovers_and_read_only():
    t = writes_table()
    p = preflight(t, parse_alter("ADD COLUMN x INT"),
                  shadow=TableInfo("chaos", "__osc_new_writes"),
                  old=TableInfo("chaos", "__osc_old_writes"), read_only=True)
    text = " ".join(p.errors)
    assert "read-only" in text and "__osc_new_writes exists" in text \
        and "__osc_old_writes exists" in text


def test_preflight_missing_table():
    p = preflight(TableInfo("chaos", "nope", exists=False), parse_alter("ADD COLUMN x INT"))
    assert not p.ok and "does not exist" in p.errors[0]


def test_preflight_unique_index_warns():
    p = preflight(writes_table(), parse_alter("ADD UNIQUE INDEX u (ts)"))
    assert p.ok and any("UNIQUE" in w for w in p.warnings) and not p.instant.ok


def test_validate_shadow():
    src = writes_table()
    sh = writes_table(columns=src.columns + [Column("note", "varchar(32)", True, None, "")])
    chk = validate_shadow(src, sh, False)
    assert not chk.errors and chk.copy_cols == chk.checksum_cols and len(chk.copy_cols) == 5

    sh = writes_table(columns=src.columns + [Column("n", "int", False, None, "")])
    assert "NOT NULL without a DEFAULT" in validate_shadow(src, sh, False).errors[0]

    changed = [Column(c.name, "varbinary(64)" if c.name == "payload" else c.column_type,
                      True, None, "") for c in src.columns]
    sh = writes_table(columns=changed)
    chk = validate_shadow(src, sh, False)
    assert chk.errors and "payload" in chk.errors[0]
    chk = validate_shadow(src, sh, True)
    assert not chk.errors and "payload" in chk.copy_cols and "payload" not in chk.checksum_cols

    sh = writes_table(pk=["client_id", "seq"])
    assert "primary key" in validate_shadow(src, sh, False).errors[0]

    gen = src.columns + [Column("g", "int", True, None, "VIRTUAL GENERATED")]
    chk = validate_shadow(writes_table(columns=gen), writes_table(columns=gen), False)
    assert "g" not in chk.copy_cols


# ------------------------------------------------------------------ chunks

def test_range_predicate_single_and_composite():
    assert range_predicate(["id"], None, None) == ("1=1", [])
    assert range_predicate(["id"], (5,), (10,)) == ("`id` > %s AND `id` <= %s", [5, 10])
    sql, args = range_predicate(["a", "b"], (1, "x"), None)
    assert sql == "(`a` >= %s AND ((`a` > %s) OR (`a` = %s AND `b` > %s)))"
    assert args == [1, 1, 1, "x"]
    sql, args = range_predicate(["a", "b"], None, (2, "y"))
    assert sql == "(`a` <= %s AND ((`a` < %s) OR (`a` = %s AND `b` <= %s)))"
    assert args == [2, 2, 2, "y"]
    with pytest.raises(ValueError):
        range_predicate(["a", "b"], (1,), None)


def _eval_predicate(sql: str, args: list, pk: list[str], row: tuple) -> bool:
    """Evaluates a range_predicate string in Python for one row."""
    it = iter(args)
    expr = re.sub(r"%s", lambda m: repr(next(it)), sql)
    for i, c in enumerate(pk):
        expr = expr.replace(f"`{c}`", f"row[{i}]")
    expr = re.sub(r"(?<![<>])=(?!=)", "==", expr).replace("AND", "and").replace("OR", "or")
    return eval(expr, {"row": row})  # noqa: S307  (test only, our own SQL)


@pytest.mark.parametrize("k", [1, 2, 3])
def test_range_predicate_matches_tuple_order(k):
    import itertools
    pk = ["a", "b", "c"][:k]
    rows = list(itertools.product(range(3), repeat=k))
    for lo in [None] + rows[::4]:
        for hi in [None] + rows[::5]:
            sql, args = range_predicate(pk, lo, hi)
            for r in rows:
                want = (lo is None or r > lo) and (hi is None or r <= hi)
                assert _eval_predicate(sql, args, pk, r) == want, (lo, hi, r)


def test_boundary_and_copy_sql():
    sql, args = boundary_sql("d", "t", ["a", "b"], (1, 2), (9, 9), 500)
    assert "ORDER BY `a`, `b` LIMIT 1 OFFSET 499" in sql and args == [1, 1, 1, 2, 9, 9, 9, 9]
    assert "FORCE INDEX (PRIMARY)" in sql
    sql, args = copy_sql("d", "t", "__osc_new_t", ["a", "b", "c"], ["a", "b"], (1, 2), (3, 4))
    assert sql.startswith("INSERT IGNORE INTO `d`.`__osc_new_t` (`a`, `b`, `c`) SELECT")
    assert sql.endswith("FOR SHARE") and args == [1, 1, 1, 2, 3, 3, 3, 4]


def test_chunk_ranges_cover_everything():
    assert chunk_ranges([]) == [(None, None)]
    assert chunk_ranges([(3,), (7,)]) == [(None, (3,)), ((3,), (7,)), ((7,), None)]


def test_chunk_sizer_adapts_and_clamps():
    s = ChunkSizer(size=1000, target_s=0.1)
    assert s.update(0.05, 1000) == 2000       # too fast, doubles (clamped at x2)
    assert s.update(0.001, 2000) == 4000      # much too fast still only x2
    assert s.update(0.4, 4000) == 2000        # too slow, halves (clamped at x0.5)
    assert s.update(0.2, 2000) == 1000
    assert s.update(0.1, 100) == 1000         # short last chunk, unchanged
    s = ChunkSizer(size=12, min_size=10)
    assert s.update(10.0, 12) == 10
    s = ChunkSizer(size=90_000, max_size=100_000)
    assert s.update(0.01, 90_000) == 100_000
    fixed = ChunkSizer(size=1000, adaptive=False)
    assert fixed.update(10.0, 1000) == 1000


def test_checksum_sql_and_compare():
    sql, _ = checksum_sql("d", "t", ["a", "b"], ["a"], None, (5,))
    assert "BIT_XOR(CRC32(CONCAT_WS('#', `a`, `b`, CONCAT(ISNULL(`a`), ISNULL(`b`)))))" in sql
    assert "COUNT(*)" in sql and sql.endswith("FOR SHARE")
    sums = [ChunkSum(None, (1,), 10, 123, 10, 123), ChunkSum((1,), (2,), 10, 1, 10, 2),
            ChunkSum((2,), None, 3, 0, 4, 0)]
    bad = compare_checksums(sums)
    assert [(b.lo, b.hi) for b in bad] == [((1,), (2,)), ((2,), None)]


def test_trigger_sql_shape():
    trg = trigger_sql("d", "t", "__osc_new_t", ["a", "b", "v"], ["a", "b"])
    ins, upd, dele = trg["__osc_ins_t"], trg["__osc_upd_t"], trg["__osc_del_t"]
    assert "AFTER INSERT ON `d`.`t`" in ins and "REPLACE INTO `d`.`__osc_new_t`" in ins
    assert "VALUES (NEW.`a`, NEW.`b`, NEW.`v`)" in ins
    assert "NOT (OLD.`a` <=> NEW.`a` AND OLD.`b` <=> NEW.`b`)" in upd and "REPLACE" in upd
    assert "DELETE IGNORE FROM `d`.`__osc_new_t` WHERE `d`.`__osc_new_t`.`a` <=> OLD.`a`" in dele


# ------------------------------------------------------------------ throttle

class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_throttle_waits_on_threads_and_lag():
    clock = FakeClock()
    running = iter([30, 30, 5, 5, 5, 5, 5, 5])
    lags = iter([5.0, 0.5])

    class Ex:
        def query(self, sql, args=None):
            return [{"Variable_name": "Threads_running", "Value": str(next(running))}]

    logs = []
    th = Throttle(Ex(), 20, 2.0, lambda: next(lags), logs.append, sleep=clock.sleep,
                  clock=clock, lag_every_s=0.0)
    waited = th.wait()
    assert waited == pytest.approx(0.75)
    assert any("Threads_running 30" in m for m in logs)
    assert any("replica lag 5.0s" in m for m in logs)


def test_throttle_pauses_on_unknown_lag_and_skips_without_manager():
    clock = FakeClock()

    class Ex:
        def query(self, sql, args=None):
            return [{"Variable_name": "Threads_running", "Value": "1"}]

    answers = iter([None, None, 0.1])
    th = Throttle(Ex(), 20, 2.0, lambda: next(answers), lambda m: None, sleep=clock.sleep,
                  clock=clock, lag_every_s=0.0)
    assert th.wait() == pytest.approx(0.5)
    assert Throttle(Ex(), 20, 2.0, None, lambda m: None).reason() is None


# ------------------------------------------------------------------ runner against a fake server

class FakeServer:
    """Models tables (columns, pk, rows as sorted pk tuples), triggers, and the statements
    the runner sends. fail_on is a regex: the first statement matching it raises."""

    def __init__(self, rows, pk=("a", "b"), fail_on=None, fail_exc=None, mismatch=False,
                 instant_error=None):
        self.log: list[str] = []
        self.tables = {"t": {"cols": [("a", "int"), ("b", "varchar(8)"), ("v", "int")],
                             "pk": list(pk), "rows": sorted(rows), "triggers": []}}
        self.fail_on, self.fail_exc = fail_on, fail_exc
        self.mismatch, self.instant_error = mismatch, instant_error
        self.killed: list[int] = []

    def open(self):
        return FakeExecutor(self)


class FakeExecutor:
    _ids = iter(range(100, 10**6))

    def __init__(self, srv: FakeServer):
        self.srv, self.id = srv, next(self._ids)

    def thread_id(self):
        return self.id

    def close(self):
        pass

    # --- helpers
    def _tname(self, sql, pattern):
        m = re.search(pattern, sql)
        return m.group(1) if m else None

    def _range(self, sql, args, pk):
        from dbguard.osc.chunks import bound_arg_count
        k, n = len(pk), bound_arg_count(len(pk))
        where = sql.split("WHERE", 1)[1] if "WHERE" in sql else ""
        has_lo, has_hi = " > " in where, " <= " in where
        args = list(args or [])
        lo = tuple(args[n - k:n]) if has_lo else None
        off = n if has_lo else 0
        hi = tuple(args[off + n - k:off + n]) if has_hi else None
        return lo, hi

    def _in(self, row, lo, hi):
        return (lo is None or row > lo) and (hi is None or row <= hi)

    def execute(self, sql, args=None):
        srv = self.srv
        srv.log.append(sql)
        if srv.fail_on and re.search(srv.fail_on, sql):
            srv.fail_on = None
            raise srv.fail_exc or pymysql.err.OperationalError(2013, "Lost connection")
        if sql.startswith("CREATE TABLE") and "LIKE" in sql:
            new = self._tname(sql, r"CREATE TABLE `d`\.`([^`]+)`")
            srcn = self._tname(sql, r"LIKE `d`\.`([^`]+)`")
            s = srv.tables[srcn]
            srv.tables[new] = {"cols": list(s["cols"]), "pk": list(s["pk"]), "rows": [],
                               "triggers": []}
        elif sql.startswith("ALTER TABLE"):
            name = self._tname(sql, r"ALTER TABLE `d`\.`([^`]+)`")
            if "ALGORITHM=INSTANT" in sql and srv.instant_error:
                raise srv.instant_error
            srv.tables[name]["cols"].append(("note", "varchar(32)"))
        elif sql.startswith("CREATE TRIGGER"):
            trg = self._tname(sql, r"CREATE TRIGGER `d`\.`([^`]+)`")
            on = self._tname(sql, r" ON `d`\.`([^`]+)`")
            srv.tables[on]["triggers"].append(trg)
        elif sql.startswith("DROP TRIGGER"):
            trg = self._tname(sql, r"DROP TRIGGER IF EXISTS `d`\.`([^`]+)`")
            for t in srv.tables.values():
                if trg in t["triggers"]:
                    t["triggers"].remove(trg)
        elif sql.startswith("DROP TABLE"):
            srv.tables.pop(self._tname(sql, r"DROP TABLE (?:IF EXISTS )?`d`\.`([^`]+)`"), None)
        elif sql.startswith("RENAME TABLE"):
            names = re.findall(r"`d`\.`([^`]+)`", sql)
            a, a2, b, b2 = names
            ta, tb = srv.tables.pop(a), srv.tables.pop(b)
            srv.tables[a2], srv.tables[b2] = ta, tb
        elif sql.startswith("INSERT IGNORE"):
            dst = self._tname(sql, r"INSERT IGNORE INTO `d`\.`([^`]+)`")
            src = self._tname(sql, r"FROM `d`\.`([^`]+)`")
            s, d = srv.tables[src], srv.tables[dst]
            lo, hi = self._range(sql, args, s["pk"])
            new = [r for r in s["rows"] if self._in(r, lo, hi) and r not in d["rows"]]
            d["rows"] = sorted(d["rows"] + new)
            return len(new)
        elif sql.startswith("KILL"):
            srv.killed.append(int(sql.split()[1]))
        return 0

    def query(self, sql, args=None):
        srv = self.srv
        srv.log.append(sql)
        if srv.fail_on and re.search(srv.fail_on, sql):
            srv.fail_on = None
            raise srv.fail_exc or pymysql.err.OperationalError(2013, "Lost connection")
        if "@@GLOBAL.super_read_only" in sql:
            return [{"sro": 0, "ro": 0, "datadir": "/var/lib/mysql/"}]
        if "information_schema" in sql and args:
            name = args[1] if len(args) > 1 else args[0].split("/")[1]
            t = srv.tables.get(name)
            if "information_schema.TABLES" in sql:
                return [] if t is None else [{"engine": "InnoDB", "row_format": "Dynamic",
                                              "table_rows": len(t["rows"]),
                                              "data_length": 16384, "index_length": 0}]
            if t is None:
                return []
            if "COLUMNS" in sql:
                return [{"name": c, "column_type": ty, "nullable": "YES", "dflt": None,
                         "extra": ""} for c, ty in t["cols"]]
            if "STATISTICS" in sql:
                return [{"index_name": "PRIMARY", "column_name": c, "non_unique": 0,
                         "index_type": "BTREE", "seq": i + 1} for i, c in enumerate(t["pk"])]
            if "TRIGGERS" in sql:
                return [{"name": n} for n in t["triggers"]]
            if "INNODB_TABLES" in sql:
                return [{"v": 0}]
            return []
        if "Threads_running" in sql:
            return [{"Variable_name": "Threads_running", "Value": "2"}]
        if sql == "SHOW WARNINGS":
            return []
        m = re.search(r"FROM `d`\.`([^`]+)` FORCE INDEX \(PRIMARY\)", sql)
        if m:
            t = srv.tables[m.group(1)]
            pk = t["pk"]
            if "DESC LIMIT 1" in sql:
                return [dict(zip(pk, t["rows"][-1]))] if t["rows"] else []
            lo, hi = self._range(sql, args, pk)
            sel = [r for r in t["rows"] if self._in(r, lo, hi)]
            if "OFFSET" in sql:
                off = int(sql.rsplit("OFFSET", 1)[1])
                return [dict(zip(pk, sel[off]))] if off < len(sel) else []
            if "COUNT(*)" in sql:
                crc = 0
                for r in sel:
                    crc ^= hash(r) & 0xFFFFFFFF
                if srv.mismatch and m.group(1).startswith("__osc_new_") and sel:
                    crc ^= 1
                return [{"cnt": len(sel), "crc": crc}]
        return []


ROWS = [(a, f"k{b:03d}") for a in range(3) for b in range(40)]  # 120 rows, composite pk


def make_runner(srv, alter="ADD COLUMN note VARCHAR(32) NULL, ADD INDEX i (v)", **kw):
    opts = Options(db="d", table="t", alter=alter, chunk_size=25, adaptive=False,
                   progress_interval_s=1e9, **kw)
    logs = []
    clock = FakeClock()
    r = Runner(opts, srv.open, log=logs.append, clock=clock, sleep=clock.sleep)
    return r, logs


def test_runner_copy_path_composite_pk():
    srv = FakeServer(ROWS)
    r, logs = make_runner(srv)
    res = r.run()
    assert res["ok"] and res["method"] == "copy"
    assert res["rows_copied"] == 120 and res["chunks"] == 5   # 25,25,25,25,20
    assert res["checksum_chunks"] == 6 and res["checksum_mismatches"] == 0
    t = srv.tables["t"]
    assert ("note", "varchar(32)") in t["cols"] and len(t["rows"]) == 120
    assert "__osc_old_t" in srv.tables and "__osc_new_t" not in srv.tables
    assert not t["triggers"] and not srv.tables["__osc_old_t"]["triggers"]
    # boundary queries walk the composite key: the second one starts after the 25th row
    bounds = [s for s in srv.log if "OFFSET 24" in s]
    assert " > " not in bounds[0] and "(`a` = %s AND `b` > %s)" in bounds[1]
    # triggers created after the shadow, dropped after the rename
    order = [i for i, s in enumerate(srv.log) if s.startswith(("CREATE TABLE", "CREATE TRIGGER",
                                                                "RENAME", "DROP TRIGGER"))
             and "`d`." in s]
    kinds = [srv.log[i].split()[0] + srv.log[i].split()[1] for i in order]
    assert kinds == ["CREATETABLE"] + ["CREATETRIGGER"] * 3 + ["RENAMETABLE"] + \
        ["DROPTRIGGER"] * 3


def test_runner_drop_old():
    srv = FakeServer(ROWS)
    r, _ = make_runner(srv, drop_old=True)
    assert r.run()["old_table"] is None and "__osc_old_t" not in srv.tables


def test_runner_empty_table():
    srv = FakeServer([])
    r, _ = make_runner(srv)
    res = r.run()
    assert res["ok"] and res["rows_copied"] == 0 and res["checksum_chunks"] == 1


def test_runner_instant_path():
    srv = FakeServer(ROWS)
    r, logs = make_runner(srv, alter="ADD COLUMN note VARCHAR(32) NULL")
    res = r.run()
    assert res["method"] == "instant"
    assert not any(s.startswith("CREATE TABLE `d`.`__osc") for s in srv.log)
    assert any(s.endswith("ALGORITHM=INSTANT") for s in srv.log)


def test_runner_instant_refused_falls_back_to_copy():
    srv = FakeServer(ROWS, instant_error=pymysql.err.OperationalError(1846, "not supported"))
    r, logs = make_runner(srv, alter="ADD COLUMN note VARCHAR(32) NULL")
    res = r.run()
    assert res["method"] == "copy" and res["ok"] and "instant_refused" in res


def test_runner_no_instant_flag_copies():
    srv = FakeServer(ROWS)
    r, _ = make_runner(srv, alter="ADD COLUMN note VARCHAR(32) NULL", allow_instant=False)
    assert r.run()["method"] == "copy"


def cleanup_order(srv):
    tail = srv.log[[i for i, s in enumerate(srv.log) if s.startswith("KILL")][-1]:]
    return [s for s in tail if s.startswith(("DROP TRIGGER", "DROP TABLE"))]


def test_failure_during_copy_drops_triggers_then_shadow():
    srv = FakeServer(ROWS, fail_on=r"OFFSET 24")
    srv.fail_on = None
    r, logs = make_runner(srv)
    # fail on the third copy chunk
    calls = {"n": 0}
    orig = FakeExecutor.execute

    def flaky(self, sql, args=None):
        if sql.startswith("INSERT IGNORE"):
            calls["n"] += 1
            if calls["n"] == 3:
                raise pymysql.err.OperationalError(2013, "Lost connection")
        return orig(self, sql, args)
    FakeExecutor.execute = flaky
    try:
        with pytest.raises(pymysql.err.OperationalError):
            r.run()
    finally:
        FakeExecutor.execute = orig
    assert not r.result["ok"]
    drops = cleanup_order(srv)
    assert [d.split()[1] for d in drops] == ["TRIGGER"] * 3 + ["TABLE"]
    assert "__osc_new_t" not in srv.tables and not srv.tables["t"]["triggers"]
    assert srv.tables["t"]["cols"] == [("a", "int"), ("b", "varchar(8)"), ("v", "int")]
    assert srv.killed  # the stuck first connection was killed before cleanup


def test_ctrl_c_during_copy_cleans_up():
    srv = FakeServer(ROWS)
    r, _ = make_runner(srv)
    orig = FakeExecutor.query

    def interrupt(self, sql, args=None):
        if "OFFSET" in sql and "`b` > %s" in sql:
            raise KeyboardInterrupt
        return orig(self, sql, args)
    FakeExecutor.query = interrupt
    try:
        with pytest.raises(KeyboardInterrupt):
            r.run()
    finally:
        FakeExecutor.query = orig
    assert r.result["error"] == "KeyboardInterrupt"
    assert set(srv.tables) == {"t"} and not srv.tables["t"]["triggers"]


def test_checksum_mismatch_aborts_and_cleans_up():
    srv = FakeServer(ROWS, mismatch=True)
    r, logs = make_runner(srv)
    with pytest.raises(OscError, match="checksum mismatch"):
        r.run()
    assert set(srv.tables) == {"t"} and not srv.tables["t"]["triggers"]
    assert not any(s.startswith("RENAME") for s in srv.log)


def test_failure_after_swap_never_drops_the_live_table():
    srv = FakeServer(ROWS)
    r, _ = make_runner(srv)
    orig = FakeExecutor.execute
    state = {"renamed": False}

    def fail_after_rename(self, sql, args=None):
        if sql.startswith("RENAME"):
            state["renamed"] = True
        elif state["renamed"] and sql.startswith("DROP TRIGGER") and "__osc_upd_t" in sql \
                and not state.get("failed"):
            state["failed"] = True
            raise pymysql.err.OperationalError(2013, "Lost connection")
        return orig(self, sql, args)
    FakeExecutor.execute = fail_after_rename
    try:
        with pytest.raises(pymysql.err.OperationalError):
            r.run()
    finally:
        FakeExecutor.execute = orig
    assert "t" in srv.tables and ("note", "varchar(32)") in srv.tables["t"]["cols"]
    assert not any(s.startswith("DROP TABLE") and "`t`" in s for s in srv.log)
    assert not srv.tables["__osc_old_t"]["triggers"]   # cleanup finished the trigger drops


def test_preflight_failure_changes_nothing():
    srv = FakeServer(ROWS)
    srv.tables["t"]["triggers"].append("audit")
    r, logs = make_runner(srv)
    with pytest.raises(OscError, match="preflight"):
        r.run()
    assert not any(s.startswith(("CREATE", "ALTER", "DROP", "RENAME")) for s in srv.log)


def test_dry_run_creates_and_drops_shadow_only():
    srv = FakeServer(ROWS)
    r, logs = make_runner(srv, dry_run=True)
    res = r.run()
    assert res["ok"] and res["method"] == "copy" and res["new_columns"] == ["note"]
    assert set(srv.tables) == {"t"}
    assert not any(s.startswith(("CREATE TRIGGER", "INSERT", "RENAME")) for s in srv.log)


def test_cleanup_command_is_idempotent():
    from dbguard.osc.runner import cleanup
    srv = FakeServer(ROWS)
    srv.tables["__osc_new_t"] = {"cols": [], "pk": ["a"], "rows": [], "triggers": []}
    srv.tables["t"]["triggers"] += ["__osc_ins_t", "__osc_upd_t"]
    ex = srv.open()
    done = cleanup(ex, "d", "t", log=lambda m: None)
    assert done == ["trigger __osc_ins_t", "trigger __osc_upd_t", "table __osc_new_t"]
    assert cleanup(ex, "d", "t", log=lambda m: None) == []


def test_throttle_total_includes_pause_in_progress():
    clock = FakeClock()
    seen = []

    class Ex:
        def query(self, sql, args=None):
            return [{"Variable_name": "Threads_running", "Value": "50" if clock.t < 1 else "1"}]

    th = Throttle(Ex(), 20, 2.0, None, lambda m: None, sleep=clock.sleep, clock=clock)
    th.wait(tick=lambda: seen.append(th.total_s()))
    assert seen[0] == 0.0 and seen[-1] == pytest.approx(0.75)
    assert th.total_s() == pytest.approx(1.0) and th.throttled_s == pytest.approx(1.0)
