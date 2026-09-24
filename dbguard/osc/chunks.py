"""Chunking by primary key. A chunk is the half-open range (lo, hi] in PK order, where lo is
None for "from the start" and hi is None for "to the end". Composite keys compare in tuple
order, written out column by column (see _bound) so InnoDB serves it as a range scan."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbguard.osc.sqlutil import q, qt


def _bound(pk: list[str], vals: tuple, lower: bool) -> tuple[str, list[Any]]:
    """pk > vals (lower) or pk <= vals (upper) in the expanded form the range optimizer
    understands. MySQL 8.4 does not turn a row constructor comparison `(a, b) > (x, y)` into
    a range (EXPLAIN shows type=index, a scan from the start of the PK for every chunk,
    which made the copy quadratic in the first bench), so like pt-osc it is written as
    a >= x AND (a > x OR (a = x AND b > y)), and the leading term bounds the scan."""
    if len(vals) != len(pk):
        raise ValueError(f"bound has {len(vals)} values for {len(pk)} key columns")
    if len(pk) == 1:
        return f"{q(pk[0])} {'>' if lower else '<='} %s", [vals[0]]
    terms, args = [], []
    for i in range(len(pk)):
        eqs = [f"{q(pk[j])} = %s" for j in range(i)]
        last = i == len(pk) - 1
        op = ">" if lower else ("<=" if last else "<")
        terms.append("(" + " AND ".join(eqs + [f"{q(pk[i])} {op} %s"]) + ")")
        args.extend(vals[: i + 1])
    lead = f"{q(pk[0])} {'>=' if lower else '<='} %s"
    return f"({lead} AND ({' OR '.join(terms)}))", [vals[0]] + args


def bound_arg_count(k: int) -> int:
    """How many arguments one bound of a ``k``-column key takes."""
    return 1 if k == 1 else 1 + k * (k + 1) // 2


def range_predicate(pk: list[str], lo: tuple | None, hi: tuple | None) -> tuple[str, list[Any]]:
    """SQL predicate and args for lo < pk <= hi. Both open gives `1=1`."""
    parts, args = [], []
    for vals, lower in ((lo, True), (hi, False)):
        if vals is not None:
            sql, a = _bound(pk, tuple(vals), lower)
            parts.append(sql)
            args.extend(a)
    return (" AND ".join(parts) or "1=1"), args


def order_by(pk: list[str], desc: bool = False) -> str:
    """ORDER BY list for the primary key."""
    d = " DESC" if desc else ""
    return ", ".join(q(c) + d for c in pk)


def boundary_sql(db: str, table: str, pk: list[str], lo: tuple | None, max_pk: tuple,
                 chunk: int) -> tuple[str, list[Any]]:
    """The PK of the chunk-th row after lo, capped at max_pk. No row means the rest of the
    table up to max_pk fits in one chunk."""
    pred, args = range_predicate(pk, lo, max_pk)
    sql = (f"SELECT {', '.join(q(c) for c in pk)} FROM {qt(db, table)} FORCE INDEX (PRIMARY) "
           f"WHERE {pred} ORDER BY {order_by(pk)} LIMIT 1 OFFSET {max(0, int(chunk) - 1)}")
    return sql, args


def max_pk_sql(db: str, table: str, pk: list[str]) -> str:
    """The largest primary key, where the copy stops (later rows come through triggers)."""
    return (f"SELECT {', '.join(q(c) for c in pk)} FROM {qt(db, table)} FORCE INDEX (PRIMARY) "
            f"ORDER BY {order_by(pk, desc=True)} LIMIT 1")


def row_tuple(row: dict, pk: list[str]) -> tuple:
    """A row's primary key as a tuple."""
    return tuple(row[c] for c in pk)


def copy_sql(db: str, src: str, dst: str, cols: list[str], pk: list[str],
             lo: tuple | None, hi: tuple | None) -> tuple[str, list[Any]]:
    """INSERT IGNORE ... SELECT ... FOR SHARE. The shared locks on the source rows make a
    concurrent DELETE or UPDATE of a row in this chunk wait until the chunk commits, so its
    trigger runs after the copy and the shadow ends up with the writer's version. Without
    them a row deleted mid-copy could be resurrected in the shadow (pt-osc does the same
    with LOCK IN SHARE MODE)."""
    pred, args = range_predicate(pk, lo, hi)
    cl = ", ".join(q(c) for c in cols)
    sql = (f"INSERT IGNORE INTO {qt(db, dst)} ({cl}) SELECT {cl} FROM {qt(db, src)} "
           f"FORCE INDEX (PRIMARY) WHERE {pred} FOR SHARE")
    return sql, args


def checksum_expr(cols: list[str]) -> str:
    """CRC32 of the row's columns joined with '#', plus a NULL bitmap so NULL and '' differ
    (CONCAT_WS skips NULLs). BIT_XOR over the chunk makes it order independent."""
    vals = ", ".join(q(c) for c in cols)
    nulls = ", ".join(f"ISNULL({q(c)})" for c in cols)
    return f"BIT_XOR(CRC32(CONCAT_WS('#', {vals}, CONCAT({nulls}))))"


def checksum_sql(db: str, table: str, cols: list[str], pk: list[str],
                 lo: tuple | None, hi: tuple | None) -> tuple[str, list[Any]]:
    """COUNT and order-independent CRC of one chunk, read under shared locks."""
    pred, args = range_predicate(pk, lo, hi)
    sql = (f"SELECT COUNT(*) AS cnt, COALESCE({checksum_expr(cols)}, 0) AS crc "
           f"FROM {qt(db, table)} FORCE INDEX (PRIMARY) WHERE {pred} FOR SHARE")
    return sql, args


def chunk_ranges(boundaries: list[tuple]) -> list[tuple[tuple | None, tuple | None]]:
    """Copy boundaries b1..bn become (None,b1], (b1,b2], ..., (bn, None). The open last range
    covers rows inserted past the copy's max PK, which only the triggers wrote."""
    ranges: list[tuple[tuple | None, tuple | None]] = []
    lo = None
    for b in boundaries:
        ranges.append((lo, b))
        lo = b
    ranges.append((lo, None))
    return ranges


@dataclass
class ChunkSizer:
    """Adapts the chunk row count so one chunk takes about `target_s`. The change per step is
    clamped to x0.5..x2 so one slow chunk (a lock wait, a checkpoint) does not collapse it."""
    size: int = 1000
    target_s: float = 0.1
    min_size: int = 10
    max_size: int = 100_000
    adaptive: bool = True

    def update(self, elapsed_s: float, rows: int) -> int:
        """Resize after a chunk of ``rows`` took ``elapsed_s``, returning the new size."""
        # a short chunk (the last one) says little about speed, keep the size
        if not self.adaptive or rows < self.size * 0.5:
            return self.size
        factor = 2.0 if elapsed_s <= 0 else min(2.0, max(0.5, self.target_s / elapsed_s))
        self.size = int(min(self.max_size, max(self.min_size, self.size * factor)))
        return self.size
