"""What the schema-change tool needs to know about a table, read from information_schema.

Columns, primary key, indexes, triggers, foreign keys, row format and InnoDB's instant row
version count, which together decide whether preflight refuses the change and whether it can
run as ALGORITHM=INSTANT. Everything goes through an ``Executor``, anything with
``query(sql, args) -> list[dict]`` and ``execute(sql, args) -> int``, so tests use a fake.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from dbguard.osc.sqlutil import TRIGGER_PREFIX


class Executor(Protocol):
    """One SQL connection, real (osc/db.py) or fake (tests)."""

    def query(self, sql: str, args: Any = None) -> list[dict[str, Any]]:
        """Rows as dicts."""

    def execute(self, sql: str, args: Any = None) -> int:
        """Affected row count."""


@dataclass
class Column:
    """One column as information_schema.COLUMNS describes it."""

    name: str
    column_type: str
    nullable: bool
    default: Any
    extra: str

    @property
    def generated(self) -> bool:
        """A generated column, which is computed and never copied."""
        return "GENERATED" in self.extra.upper() and "DEFAULT_GENERATED" not in self.extra.upper()

    @property
    def auto_increment(self) -> bool:
        """An AUTO_INCREMENT column."""
        return "auto_increment" in self.extra.lower()


@dataclass
class TableInfo:
    """A table's shape. ``exists`` is False when it was not found."""

    db: str
    name: str
    exists: bool = True
    columns: list[Column] = field(default_factory=list)
    pk: list[str] = field(default_factory=list)
    has_fulltext: bool = False
    unique_non_pk: list[str] = field(default_factory=list)
    triggers: list[str] = field(default_factory=list)
    fks: list[str] = field(default_factory=list)
    row_format: str | None = None
    engine: str | None = None
    rows_est: int = 0
    data_bytes: int = 0
    index_bytes: int = 0
    total_row_versions: int | None = None

    def col(self, name: str) -> Column | None:
        """The column named ``name``, case-insensitive."""
        for c in self.columns:
            if c.name.lower() == name.lower():
                return c
        return None

    @property
    def copyable(self) -> list[str]:
        """Columns the copy writes, every one except generated columns."""
        return [c.name for c in self.columns if not c.generated]


def _v(row: dict, key: str):
    """information_schema column names come back upper or lower case depending on the alias,
    read either."""
    for k in (key, key.upper(), key.lower()):
        if k in row:
            return row[k]
    return None


def load_table(ex: Executor, db: str, table: str) -> TableInfo:
    """Read ``db.table``'s shape from information_schema."""
    t = TableInfo(db=db, name=table)
    rows = ex.query(
        "SELECT ENGINE AS engine, ROW_FORMAT AS row_format, TABLE_ROWS AS table_rows, "
        "DATA_LENGTH AS data_length, INDEX_LENGTH AS index_length FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (db, table))
    if not rows:
        t.exists = False
        return t
    r = rows[0]
    t.engine = _v(r, "engine")
    t.row_format = _v(r, "row_format")
    t.rows_est = int(_v(r, "table_rows") or 0)
    t.data_bytes = int(_v(r, "data_length") or 0)
    t.index_bytes = int(_v(r, "index_length") or 0)
    for r in ex.query(
            "SELECT COLUMN_NAME AS name, COLUMN_TYPE AS column_type, IS_NULLABLE AS nullable, "
            "COLUMN_DEFAULT AS dflt, EXTRA AS extra FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION", (db, table)):
        t.columns.append(Column(name=_v(r, "name"), column_type=str(_v(r, "column_type")),
                                nullable=_v(r, "nullable") == "YES", default=_v(r, "dflt"),
                                extra=str(_v(r, "extra") or "")))
    idx: dict[str, dict] = {}
    for r in ex.query(
            "SELECT INDEX_NAME AS index_name, COLUMN_NAME AS column_name, NON_UNIQUE AS non_unique, "
            "INDEX_TYPE AS index_type, SEQ_IN_INDEX AS seq FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (db, table)):
        name = _v(r, "index_name")
        d = idx.setdefault(name, {"cols": [], "unique": not int(_v(r, "non_unique")),
                                  "type": _v(r, "index_type")})
        d["cols"].append(_v(r, "column_name"))
    for name, d in idx.items():
        if name == "PRIMARY":
            t.pk = d["cols"]
        elif (d["type"] or "").upper() == "FULLTEXT":
            t.has_fulltext = True
        elif d["unique"]:
            t.unique_non_pk.append(name)
    t.triggers = [_v(r, "name") for r in ex.query(
        "SELECT TRIGGER_NAME AS name FROM information_schema.TRIGGERS "
        "WHERE EVENT_OBJECT_SCHEMA=%s AND EVENT_OBJECT_TABLE=%s", (db, table))]
    t.fks = [_v(r, "name") for r in ex.query(
        "SELECT CONSTRAINT_NAME AS name FROM information_schema.REFERENTIAL_CONSTRAINTS "
        "WHERE (CONSTRAINT_SCHEMA=%s AND TABLE_NAME=%s) "
        "OR (UNIQUE_CONSTRAINT_SCHEMA=%s AND REFERENCED_TABLE_NAME=%s)", (db, table, db, table))]
    try:
        rv = ex.query("SELECT TOTAL_ROW_VERSIONS AS v FROM information_schema.INNODB_TABLES "
                      "WHERE NAME=%s", (f"{db}/{table}",))
        t.total_row_versions = int(_v(rv[0], "v")) if rv else None
    except Exception:  # noqa: BLE001  (column exists from 8.0.29, not fatal if missing)
        t.total_row_versions = None
    return t


def osc_triggers(t: TableInfo) -> list[str]:
    """Triggers an earlier run of this tool left on the table."""
    return [n for n in t.triggers if n.startswith(TRIGGER_PREFIX)]
