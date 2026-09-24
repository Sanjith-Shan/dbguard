"""GTID set arithmetic in pure Python, the one place DBGuard compares what nodes hold.

Every failover decision reduces to it. Choosing the winner, the subset check that HALTs a
diverged set, catch-up, and rejoin's repoint-or-rebuild are all ``is_subset`` and ``subtract``
here, held to MySQL's ``GTID_SUBSET`` and ``GTID_SUBTRACT`` by hypothesis tests. A set is a map
from ``(uuid, tag)`` to a sorted tuple of merged inclusive intervals, so equal sets have one
representation. The trap it exists to avoid is comparing GTID strings. ``"u:1-3:4-5"`` equals
``"u:1-5"``, and ``"u:1-10"`` holds ``"u:1-9"`` though neither string contains the other.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator

Interval = tuple[int, int]
Key = tuple[str, str]  # (uuid lowercase, tag lowercase or "")

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$"
)
_TAG_RE = re.compile(r"^[a-z_][a-z0-9_]{0,31}$")
_MAX_GNO = (1 << 63) - 1


class GtidParseError(ValueError):
    """The text is not a GTID set MySQL would accept."""


def _norm_uuid(text: str) -> str:
    u = text.strip().lower()
    if not _UUID_RE.match(u):
        raise GtidParseError(f"bad uuid {text!r}")
    if "-" not in u:
        u = f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:32]}"
    return u


def _normalize(intervals: Iterable[Interval]) -> tuple[Interval, ...]:
    """Sort and merge overlapping or adjacent intervals."""
    out: list[list[int]] = []
    for s, e in sorted(intervals):
        if s > e:
            raise GtidParseError(f"bad interval {s}-{e}")
        if out and s <= out[-1][1] + 1:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return tuple((s, e) for s, e in out)


def _subtract_iv(a: tuple[Interval, ...], b: tuple[Interval, ...]) -> tuple[Interval, ...]:
    """The parts of sorted intervals ``a`` not covered by sorted intervals ``b``."""
    out: list[Interval] = []
    j = 0
    for s, e in a:
        cur = s
        while j < len(b) and b[j][1] < cur:
            j += 1
        k = j
        while k < len(b) and b[k][0] <= e:
            bs, be = b[k]
            if bs > cur:
                out.append((cur, bs - 1))
            cur = max(cur, be + 1)
            if cur > e:
                break
            k += 1
        if cur <= e:
            out.append((cur, e))
    return tuple(out)


def _intersect_iv(a: tuple[Interval, ...], b: tuple[Interval, ...]) -> tuple[Interval, ...]:
    """The overlap of two sorted interval lists."""
    out: list[Interval] = []
    i = j = 0
    while i < len(a) and j < len(b):
        s = max(a[i][0], b[j][0])
        e = min(a[i][1], b[j][1])
        if s <= e:
            out.append((s, e))
        if a[i][1] < b[j][1]:
            i += 1
        else:
            j += 1
    return tuple(out)


class GtidSet:
    """Immutable, hashable GTID set."""

    __slots__ = ("_hash", "_m")

    def __init__(self, mapping: dict[Key, Iterable[Interval]] | None = None):
        m: dict[Key, tuple[Interval, ...]] = {}
        for key, ivs in (mapping or {}).items():
            norm = _normalize(ivs)
            if norm:
                m[key] = norm
        self._m: dict[Key, tuple[Interval, ...]] = dict(sorted(m.items()))
        self._hash: int | None = None

    # construction -----------------------------------------------------------------
    @classmethod
    def parse(cls, text: str | None) -> GtidSet:
        """Parse MySQL's text form, tolerating whitespace, newlines and upper case.

        None and "" are the empty set. A GtidSet passes through unchanged."""
        if text is None:
            return cls()
        if isinstance(text, GtidSet):
            return text
        text = text.strip()
        if not text:
            return cls()
        acc: dict[Key, list[Interval]] = {}
        for part in text.split(","):
            part = "".join(part.split())  # drop whitespace and newlines anywhere
            if not part:
                continue
            fields = part.split(":")
            uuid = _norm_uuid(fields[0])
            tag = ""
            saw_interval = False
            for f in fields[1:]:
                if not f:
                    raise GtidParseError(f"empty field in {part!r}")
                if f[0].isdigit():
                    if "-" in f:
                        a, _, b = f.partition("-")
                        s, e = int(a), int(b)
                    else:
                        s = e = int(f)
                    if s < 1 or e > _MAX_GNO or s > e:
                        raise GtidParseError(f"bad interval {f!r}")
                    acc.setdefault((uuid, tag), []).append((s, e))
                    saw_interval = True
                else:
                    t = f.lower()
                    if not _TAG_RE.match(t):
                        raise GtidParseError(f"bad tag {f!r}")
                    tag = t
            if not saw_interval and len(fields) > 1:
                # "uuid:tag" with no interval is legal only as nothing, reject it
                raise GtidParseError(f"no interval in {part!r}")
            if len(fields) == 1:
                raise GtidParseError(f"no interval in {part!r}")
        return cls(acc)

    @classmethod
    def of(cls, uuid: str, *intervals: Interval | int, tag: str = "") -> GtidSet:
        """Build a one-uuid set from ints and ``(start, end)`` pairs, mostly for tests."""
        ivs = [(i, i) if isinstance(i, int) else i for i in intervals]
        return cls({(_norm_uuid(uuid), tag.lower()): ivs})

    # views ------------------------------------------------------------------------
    def items(self) -> Iterator[tuple[Key, tuple[Interval, ...]]]:
        """Iterate ``((uuid, tag), intervals)`` in sorted order."""
        return iter(self._m.items())

    def count(self) -> int:
        """Number of transactions in the set."""
        return sum(e - s + 1 for ivs in self._m.values() for s, e in ivs)

    def __len__(self) -> int:
        return self.count()

    @property
    def is_empty(self) -> bool:
        """True when the set holds no transaction."""
        return not self._m

    def __bool__(self) -> bool:
        return bool(self._m)

    def contains(self, uuid: str, n: int, tag: str = "") -> bool:
        """True when transaction ``uuid[:tag]:n`` is in the set."""
        for s, e in self._m.get((uuid.lower(), tag.lower()), ()):
            if s <= n <= e:
                return True
            if s > n:
                return False
        return False

    # algebra ----------------------------------------------------------------------
    def union(self, other: GtidSet) -> GtidSet:
        """Every transaction in either set, ``a | b``."""
        other = _coerce(other)
        keys = set(self._m) | set(other._m)
        return GtidSet({k: self._m.get(k, ()) + other._m.get(k, ()) for k in keys})

    def subtract(self, other: GtidSet) -> GtidSet:
        """GTID_SUBTRACT(self, other), ``a - b``. Rejoin counts phantoms with it."""
        other = _coerce(other)
        out = {}
        for k, ivs in self._m.items():
            o = other._m.get(k)
            out[k] = _subtract_iv(ivs, o) if o else ivs
        return GtidSet(out)

    def intersection(self, other: GtidSet) -> GtidSet:
        """Transactions in both sets, ``a & b``."""
        other = _coerce(other)
        return GtidSet(
            {k: _intersect_iv(ivs, other._m[k]) for k, ivs in self._m.items() if k in other._m}
        )

    def is_subset(self, other: GtidSet) -> bool:
        """GTID_SUBSET(self, other). The empty set is a subset of everything."""
        return self.subtract(_coerce(other)).is_empty

    def is_superset(self, other: GtidSet) -> bool:
        """GTID_SUBSET(other, self)."""
        return _coerce(other).is_subset(self)

    __or__ = union
    __sub__ = subtract
    __and__ = intersection

    def __le__(self, other: GtidSet) -> bool:
        return self.is_subset(other)

    def __ge__(self, other: GtidSet) -> bool:
        return self.is_superset(other)

    # identity ---------------------------------------------------------------------
    def __eq__(self, other: object) -> bool:
        """Set equality. A string operand is parsed first, never compared as text."""
        if isinstance(other, str):
            other = GtidSet.parse(other)
        if not isinstance(other, GtidSet):
            return NotImplemented
        return self._m == other._m

    def __hash__(self) -> int:
        if self._hash is None:
            self._hash = hash(tuple(self._m.items()))
        return self._hash

    def __str__(self) -> str:
        """Canonical text, uuids sorted and untagged intervals before tagged ones."""
        by_uuid: dict[str, list[tuple[str, tuple[Interval, ...]]]] = {}
        for (u, t), ivs in self._m.items():
            by_uuid.setdefault(u, []).append((t, ivs))
        parts = []
        for u in sorted(by_uuid):
            fields = [u]
            for t, ivs in sorted(by_uuid[u]):  # "" sorts first: untagged before tags
                if t:
                    fields.append(t)
                fields.extend(str(s) if s == e else f"{s}-{e}" for s, e in ivs)
            parts.append(":".join(fields))
        return ",".join(parts)

    def __repr__(self) -> str:
        return f"GtidSet({str(self)!r})"

    def __setattr__(self, name, value):
        if name == "_hash" or (name == "_m" and not hasattr(self, "_m")):
            object.__setattr__(self, name, value)
        else:
            raise AttributeError("GtidSet is immutable")


def _coerce(x: GtidSet | str | None) -> GtidSet:
    """Accept a GtidSet or its text wherever a set is expected."""
    if isinstance(x, GtidSet):
        return x
    return GtidSet.parse(x)
