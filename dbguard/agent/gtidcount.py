"""Phantom GTID counting for /rebuild.

Uses dbguard.gtid.GtidSet when the manager's module is present, else a small local
parser of "uuid:1-5:7,uuid2:1-3" that is enough to count `a - b`.
"""

from __future__ import annotations


def _parse(text: str | None) -> dict[str, list[tuple[int, int]]]:
    out: dict[str, list[tuple[int, int]]] = {}
    if not text:
        return out
    for part in text.replace("\n", "").split(","):
        part = part.strip()
        if not part:
            continue
        fields = part.split(":")
        sid = fields[0].lower()
        # 8.3+ tagged GTIDs look like uuid:tag:1-5; a field that is not numeric is a tag.
        tag = ""
        for f in fields[1:]:
            if f and not f[0].isdigit():
                tag = f.lower()
                continue
            lo, _, hi = f.partition("-")
            out.setdefault(f"{sid}:{tag}" if tag else sid, []).append(
                (int(lo), int(hi) if hi else int(lo)))
    return out


def _interval_count(a: list[tuple[int, int]], b: list[tuple[int, int]]) -> int:
    count = 0
    for lo, hi in a:
        pieces = [(lo, hi)]
        for blo, bhi in b:
            nxt = []
            for plo, phi in pieces:
                if bhi < plo or blo > phi:
                    nxt.append((plo, phi))
                    continue
                if plo < blo:
                    nxt.append((plo, blo - 1))
                if phi > bhi:
                    nxt.append((bhi + 1, phi))
            pieces = nxt
        count += sum(phi - plo + 1 for plo, phi in pieces)
    return count


def subtract_count(a: str | None, b: str | None) -> int:
    """Number of transactions in GTID set `a` that are not in `b`."""
    try:
        from dbguard.gtid import GtidSet  # type: ignore[attr-defined]

        return int(GtidSet.parse(a or "").subtract(GtidSet.parse(b or "")).count())
    except Exception:  # noqa: BLE001, S110  absent, or its API drifted: count locally
        pass
    return sum(_interval_count(ivs, _parse(b).get(k, [])) for k, ivs in _parse(a).items())
