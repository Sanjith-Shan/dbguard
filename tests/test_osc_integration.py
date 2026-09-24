"""dbgctl osc against a real MySQL 8.4 at 127.0.0.1:23306 (the throwaway dev container of
docs/OSC.md), skipped when nothing answers there. A writer thread inserts, updates and
deletes during the change, and afterwards every acknowledged write must be in the new table."""

from __future__ import annotations

import os
import random
import socket
import threading
import time

import pytest

pytestmark = pytest.mark.integration

HOST, PORT = "127.0.0.1", int(os.environ.get("OSC_TEST_PORT", "23306"))
USER = os.environ.get("OSC_TEST_USER", "root")
PASSWORD = os.environ.get("OSC_TEST_PASSWORD", "root")
DB = "osc_it"
ROWS = 20_000


def _reachable() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=0.5):
            pass
    except OSError:
        return False
    try:
        from dbguard.osc.db import connector
        connector(HOST, PORT, USER, PASSWORD, timeout=2)().close()
        return True
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture(scope="module")
def connect():
    if not _reachable():
        pytest.skip(f"no MySQL at {HOST}:{PORT}")
    from dbguard.osc.db import connector
    c = connector(HOST, PORT, USER, PASSWORD)
    yield c
    ex = c()
    ex.execute(f"DROP DATABASE IF EXISTS {DB}")
    ex.close()


@pytest.fixture()
def table(connect):
    ex = connect()
    ex.execute(f"DROP DATABASE IF EXISTS {DB}")
    ex.execute(f"CREATE DATABASE {DB}")
    ex.execute(f"CREATE TABLE {DB}.writes (client_id INT NOT NULL, seq BIGINT NOT NULL, "
               "ts TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), run_id VARCHAR(64) NOT NULL, "
               "payload VARBINARY(512) NULL, PRIMARY KEY (client_id, seq, run_id)) ENGINE=InnoDB")
    ex.execute("SET SESSION cte_max_recursion_depth = 100000")
    ex.execute(f"INSERT INTO {DB}.writes (client_id, seq, run_id, payload) "
               f"WITH RECURSIVE g(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM g WHERE i < {ROWS - 1}) "
               "SELECT i % 10, i, 'load', RANDOM_BYTES(64) FROM g")
    ex.close()
    return connect


class Writer(threading.Thread):
    """Inserts rows for client 500, updates random seed rows to a known payload, and deletes
    some of its own rows. Every statement is retried until acknowledged, so the expected
    state is exact."""

    def __init__(self, connect):
        super().__init__(daemon=True)
        self.connect, self.stop = connect, threading.Event()
        self.inserted: set[int] = set()
        self.deleted: set[int] = set()
        self.updated: dict[tuple, bytes] = {}
        self.errors: list[str] = []

    def _do(self, ex, sql, args):
        for _ in range(50):
            try:
                ex.execute(sql, args)
                return ex
            except Exception as e:  # noqa: BLE001
                self.errors.append(str(e))
                if "Duplicate entry" in str(e):   # the first attempt committed
                    return ex
                try:
                    ex.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.05)
                ex = self.connect()
        raise RuntimeError("writer gave up")

    def run(self):
        ex = self.connect()
        rnd = random.Random(7)
        seq = 0
        while not self.stop.is_set():
            r = rnd.random()
            if r < 0.5:
                seq += 1
                ex = self._do(ex, f"INSERT INTO {DB}.writes (client_id, seq, run_id, payload) "
                                  "VALUES (500, %s, 'it', %s)", (seq, b"i%d" % seq))
                self.inserted.add(seq)
            elif r < 0.85:
                i = rnd.randrange(ROWS)
                val = b"u%d-%d" % (i, rnd.randrange(10**9))
                ex = self._do(ex, f"UPDATE {DB}.writes SET payload=%s WHERE client_id=%s AND "
                                  "seq=%s AND run_id='load'", (val, i % 10, i))
                self.updated[(i % 10, i, "load")] = val
            elif self.inserted - self.deleted:
                s = rnd.choice(sorted(self.inserted - self.deleted))
                ex = self._do(ex, f"DELETE FROM {DB}.writes WHERE client_id=500 AND seq=%s "
                                  "AND run_id='it'", (s,))
                self.deleted.add(s)
        ex.close()


def _run(connect, alter, **kw):
    from dbguard.osc.runner import Options, Runner
    opts = Options(db=DB, table="writes", alter=alter, chunk_size=500, **kw)
    logs: list[str] = []
    r = Runner(opts, connect, log=logs.append)
    return r, logs


def test_copy_under_writes_loses_nothing(table):
    connect = table
    w = Writer(connect)
    w.start()
    time.sleep(0.5)
    r, logs = _run(connect, "ADD COLUMN note VARCHAR(32) NULL, ADD INDEX idx_ts (ts)",
                   allow_instant=False, target_chunk_s=0.02)
    res = r.run()
    time.sleep(0.5)
    w.stop.set()
    w.join(10)
    assert res["ok"] and res["method"] == "copy", logs
    assert res["checksum_mismatches"] == 0 and res["rows_copied"] >= ROWS - 50

    ex = connect()
    try:
        cols = [x["COLUMN_NAME"] for x in ex.query(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s "
            "AND TABLE_NAME='writes'", (DB,))]
        assert "note" in cols
        idx = ex.query("SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND "
                       "TABLE_NAME='writes' AND INDEX_NAME='idx_ts'", (DB,))
        assert idx
        n = ex.query(f"SELECT COUNT(*) AS n FROM {DB}.writes")[0]["n"]
        assert n == ROWS + len(w.inserted) - len(w.deleted)
        mine = {x["seq"] for x in ex.query(
            f"SELECT seq FROM {DB}.writes WHERE client_id=500 AND run_id='it'")}
        assert mine == w.inserted - w.deleted
        for (c, s, rid), val in w.updated.items():
            got = ex.query(f"SELECT payload FROM {DB}.writes WHERE client_id=%s AND seq=%s "
                           "AND run_id=%s", (c, s, rid))
            assert got and got[0]["payload"] == val
        trg = ex.query("SELECT TRIGGER_NAME FROM information_schema.TRIGGERS "
                       "WHERE TRIGGER_SCHEMA=%s", (DB,))
        assert not trg
        tables = {x["TABLE_NAME"] for x in ex.query(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s", (DB,))}
        assert tables == {"writes", "__osc_old_writes"}
    finally:
        ex.close()
    assert len(w.inserted) > 50 and len(w.updated) > 20 and w.deleted, "writer did too little"


def test_instant_is_detected_and_used(table):
    r, logs = _run(table, "ADD COLUMN note2 VARCHAR(8) NULL")
    res = r.run()
    assert res["method"] == "instant" and res["instant_s"] < 5


def test_rejected_alter_leaves_nothing(table):
    from dbguard.osc.runner import OscError
    r, logs = _run(table, "ADD COLUMN must INT NOT NULL", allow_instant=False)
    with pytest.raises(OscError, match="NOT NULL without a DEFAULT"):
        r.run()
    ex = table()
    try:
        tables = {x["TABLE_NAME"] for x in ex.query(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s", (DB,))}
        assert tables == {"writes"}
        assert not ex.query("SELECT 1 FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=%s",
                            (DB,))
    finally:
        ex.close()
