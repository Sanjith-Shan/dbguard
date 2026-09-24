"""Property tests for dbguard.gtid.GtidSet."""

from __future__ import annotations

import random
import socket

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from dbguard.gtid import GtidParseError, GtidSet

UUIDS = [
    "3e11fa47-71ca-11e1-9e33-c80aa9429562",
    "aaaaaaaa-bbbb-cccc-dddd-000000000001",
    "0f0f0f0f-1111-2222-3333-444444444444",
]
U = UUIDS[0]


def interval(hi: int):
    return st.tuples(st.integers(1, hi), st.integers(0, hi // 10 + 5)).map(
        lambda t: (t[0], min(hi, t[0] + t[1]))
    )


def gtid_sets(hi: int = 10**6, tags: bool = False):
    keys = st.sampled_from(UUIDS)
    if tags:
        keys = st.tuples(keys, st.sampled_from(["", "", "blue", "red"]))
    else:
        keys = keys.map(lambda u: (u, ""))
    return st.dictionaries(keys, st.lists(interval(hi), min_size=0, max_size=6), max_size=3).map(
        GtidSet
    )


def as_pyset(g: GtidSet) -> set[tuple[str, str, int]]:
    return {(u, t, n) for (u, t), ivs in g.items() for s, e in ivs for n in range(s, e + 1)}


def render_messy(g: GtidSet, rnd: random.Random) -> str:
    """Another valid textual form of the same set: split intervals, shuffle, uppercase."""
    parts = []
    for (u, t), ivs in g.items():
        pieces = []
        for s, e in ivs:
            if e > s and rnd.random() < 0.5:
                m = rnd.randint(s, e - 1)
                pieces += [f"{s}-{m}", f"{m + 1}-{e}" if m + 1 < e else str(e)]
            else:
                pieces.append(f"{s}-{e}" if e > s else str(s))
        rnd.shuffle(pieces)
        uu = u.upper() if rnd.random() < 0.5 else u
        parts.append(":".join([uu] + ([t] if t else []) + pieces))
    rnd.shuffle(parts)
    return ",\n".join(parts)


# ---------------------------------------------------------------------------------- basics


def test_empty_and_whitespace():
    assert GtidSet.parse("").is_empty
    assert GtidSet.parse("  \n ").is_empty
    assert GtidSet.parse(None).is_empty
    assert len(GtidSet.parse("")) == 0
    assert str(GtidSet.parse("")) == ""


def test_canonical_merges_adjacent():
    assert str(GtidSet.parse(f"{U}:1-3:4-5")) == f"{U}:1-5"
    assert str(GtidSet.parse(f"{U}:4-5:1-3:7")) == f"{U}:1-5:7"
    assert str(GtidSet.parse(f"{U}:1-10,{U}:5-12")) == f"{U}:1-12"


def test_mysql_multiline_format_and_case():
    text = f"{UUIDS[1].upper()}:1-7,\n{U}:1-3:5"
    g = GtidSet.parse(text)
    assert str(g) == f"{U}:1-3:5,{UUIDS[1]}:1-7"
    assert g.count() == 11
    assert g.contains(U, 5) and not g.contains(U, 4)
    assert g.contains(UUIDS[1].upper(), 7)


def test_tagged_gtids():
    g = GtidSet.parse(f"{U}:1-3:blue:1-2:4,{U}:red:9")
    assert g.count() == 3 + 2 + 1 + 1
    assert g.contains(U, 4, tag="blue") and not g.contains(U, 4)
    assert str(g) == f"{U}:1-3:blue:1-2:4:red:9"


@pytest.mark.parametrize("bad", ["nope:1-2", f"{U}", f"{U}:0", f"{U}:5-3", f"{U}::1", f"{U}:1-x"])
def test_parse_rejects_garbage(bad):
    with pytest.raises((GtidParseError, ValueError)):
        GtidSet.parse(bad)


def test_immutable_and_hashable():
    g = GtidSet.parse(f"{U}:1-3")
    with pytest.raises(AttributeError):
        g._m = {}
    assert {g, GtidSet.parse(f"{U}:1:2:3")} == {g}


def test_empty_subset_semantics():
    """The spec's 'subset check passing on empty sets' trap: empty is a subset of
    everything, so the manager must not treat an empty retrieved set as 'agrees'."""
    e = GtidSet()
    assert e.is_subset(GtidSet.parse(f"{U}:1"))
    assert e.is_subset(e)
    assert not GtidSet.parse(f"{U}:1").is_subset(e)


def test_string_comparison_is_not_set_comparison():
    """Regression for 'GTID sets compared as strings'."""
    a_text = f"{U}:1-3:4-5"
    b_text = f"{U}:1-5"
    assert a_text != b_text
    assert GtidSet.parse(a_text) == GtidSet.parse(b_text)
    # lexicographic order is not containment either
    small, big = f"{U}:1-9", f"{U}:1-10"
    assert small > big  # as strings "9" > "1"
    assert GtidSet.parse(small).is_subset(GtidSet.parse(big))
    assert len(GtidSet.parse(big)) > len(GtidSet.parse(small))


# ------------------------------------------------------------------------------ properties


@given(gtid_sets(tags=True))
def test_roundtrip(s):
    assert GtidSet.parse(str(s)) == s
    assert str(GtidSet.parse(str(s))) == str(s)


@given(gtid_sets(tags=True), st.randoms(use_true_random=False))
def test_messy_rendering_equal_as_sets(s, rnd):
    text = render_messy(s, rnd)
    assert GtidSet.parse(text) == s
    assert hash(GtidSet.parse(text)) == hash(s)


@given(gtid_sets(), gtid_sets())
def test_subset_iff_subtract_empty(a, b):
    assert a.is_subset(b) == (a.subtract(b).count() == 0)


@given(gtid_sets(), gtid_sets())
def test_algebra(a, b):
    assert (a - b) | (a & b) == a
    assert a.is_subset(a | b) and b.is_subset(a | b)
    assert (a | b) - b == a - b
    assert (a & b).is_subset(a) and (a & b).is_subset(b)
    assert len(a | b) == len(a) + len(b) - len(a & b)
    assert len(a - b) == len(a) - len(a & b)
    assert (a - b) & b == GtidSet()
    assert a | b == b | a and a & b == b & a


@settings(max_examples=300)
@given(gtid_sets(hi=60, tags=True), gtid_sets(hi=60, tags=True))
def test_against_python_sets(a, b):
    """Small universe, so we can compare with a brute-force model."""
    pa, pb = as_pyset(a), as_pyset(b)
    assert as_pyset(a | b) == pa | pb
    assert as_pyset(a - b) == pa - pb
    assert as_pyset(a & b) == pa & pb
    assert a.is_subset(b) == (pa <= pb)
    assert len(a) == len(pa)
    assert (a == b) == (pa == pb)


# ----------------------------------------------------------------------- real MySQL oracle


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


@pytest.mark.integration
def test_matches_mysql_gtid_functions():
    if not _port_open("127.0.0.1", 13311):
        pytest.skip("no mysqld at 127.0.0.1:13311")
    pymysql = pytest.importorskip("pymysql")
    try:
        conn = pymysql.connect(host="127.0.0.1", port=13311, user="root", password="root",
                               connect_timeout=2)
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"cannot connect: {e}")
    rnd = random.Random(1234)
    try:
        with conn.cursor() as cur:
            for _ in range(200):
                a = _random_set(rnd)
                b = _random_set(rnd)
                if rnd.random() < 0.3:
                    b = a | b  # make subsets common
                ta, tb = render_messy(a, rnd), render_messy(b, rnd)
                cur.execute("SELECT GTID_SUBSET(%s,%s), GTID_SUBTRACT(%s,%s)", (ta, tb, ta, tb))
                sub, diff = cur.fetchone()
                assert bool(sub) == a.is_subset(b), (ta, tb)
                assert GtidSet.parse(diff) == a - b, (ta, tb, diff)
    finally:
        conn.close()


def _random_set(rnd: random.Random) -> GtidSet:
    m = {}
    for u in rnd.sample(UUIDS, rnd.randint(0, 3)):
        ivs = []
        for _ in range(rnd.randint(1, 5)):
            s = rnd.randint(1, 10**6)
            ivs.append((s, min(10**6, s + rnd.randint(0, 5000))))
        m[(u, "")] = ivs
    return GtidSet(m)
