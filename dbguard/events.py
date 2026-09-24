"""Event rows, the manager's durable record of every decision it made and action it took.

A failover, switchover, rejoin, rebuild or state change becomes one ``Event``, written as one
JSON line to ``<state-dir>/events.jsonl`` and served by ``GET /v1/events``. The harness reads
the failover's step timings from these rows, so the field names are the contract in
docs/INTERFACES.md ("Event row") and never change. Models allow extra keys so later additions
reach the file without a schema change. Each append is fsync'd before the manager moves on,
because an event describes something already done to the fleet.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EventType = Literal[
    "failover", "switchover", "rejoin", "rebuild", "replace", "halt", "resume",
    "suspect", "degraded", "healthy", "stall", "bootstrap", "cold_start", "split_brain",
]
Trigger = Literal["dead", "hung", "partition", "planned", "manual"]


class _M(BaseModel):
    """Base for event parts. Unknown keys are kept, so the row can grow without a migration."""

    model_config = ConfigDict(extra="allow")


class Detect(_M):
    """What detection saw. ``replica_total`` counts witnesses, ``replica_votes`` the dead votes."""

    manager_probe_failed: bool = False
    replica_votes: int = 0
    replica_total: int = 0
    duration_s: float = 0.0


class FenceStep(_M):
    """Step 1, how the old primary was fenced (``sql``, ``kill``) or why not."""

    duration_s: float = 0.0
    outcome: Literal["sql", "kill", "unreachable", "skipped"] = "skipped"


class ChooseStep(_M):
    """Step 3, the candidates compared, the winner, and whether every set was a subset of it."""

    duration_s: float = 0.0
    candidates: list[str] = Field(default_factory=list)
    winner: str | None = None
    subset_ok: bool = True


class CatchupStep(_M):
    """Step 4, whether the winner applied its relay log before the deadline."""

    duration_s: float = 0.0
    ok: bool = True


class PromoteStep(_M):
    """Step 6, the agent's /promote on the winner."""

    duration_s: float = 0.0


class RepointStep(_M):
    """Step 5, the replicas moved under the winner."""

    duration_s: float = 0.0
    nodes: list[str] = Field(default_factory=list)


class Steps(_M):
    """Per-step timings of a failover or switchover, each None when the step did not run."""

    prepare: RepointStep | None = None   # switchover: replicas moved under the candidate first
    fence: FenceStep | None = None
    choose: ChooseStep | None = None
    catchup: CatchupStep | None = None
    promote: PromoteStep | None = None
    repoint: RepointStep | None = None


class Rejoin(_M):
    """How a node came back, repointed as is or rebuilt by clone, and its phantom count."""

    branch: Literal["repoint", "rebuild", "manual", "none"] = "none"
    phantom_gtids: int = 0
    duration_s: float = 0.0
    node: str | None = None


class Clone(_M):
    """Size, speed and donor of a clone done by /rebuild."""

    bytes: int = 0
    duration_s: float = 0.0
    mb_per_s: float | None = None
    donor: str | None = None


class Event(_M):
    """One row of the event log. ``type`` says which of the optional parts are filled."""

    ts: float = Field(default_factory=time.time)
    rs: str
    type: EventType
    mode: Literal["dbguard", "naive"] = "dbguard"
    old_primary: str | None = None
    new_primary: str | None = None
    trigger: Trigger | None = None
    detect: Detect | None = None
    steps: Steps | None = None
    total_s: float | None = None
    watermark_gtid: str | None = None
    rejoin: Rejoin | None = None
    clone: Clone | None = None
    stall_s: float | None = None   # switchover: quiesce start to promote answered
    note: str | None = None

    def row(self) -> dict:
        """The JSON-ready dict written to the log and the API."""
        return self.model_dump(mode="json")


class EventLog:
    """Append-only JSONL file plus an in-memory ring buffer for the API.

    Writes are line-buffered and fsync'd: an event describes an action already taken on
    the fleet, so losing it on a manager crash would make the log lie by omission.
    """

    def __init__(self, path: str | os.PathLike | None, keep: int = 2000):
        self.path = Path(path) if path else None
        self._ring: deque[Event] = deque(maxlen=keep)
        self._lock = threading.Lock()
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.path.exists():
                self._load_tail()
                self._terminate_torn_line()

    def _load_tail(self) -> None:
        """Refill the ring from the file on restart, skipping lines that do not parse."""
        assert self.path
        with self.path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    self._ring.append(Event.model_validate_json(line))
                except Exception:  # noqa: BLE001 - a torn last line must not stop startup
                    continue

    def _terminate_torn_line(self) -> None:
        """A crash mid-write leaves a line without its newline. Close it so the next
        event starts on its own line instead of being glued to the torn one."""
        assert self.path
        with self.path.open("rb+") as f:
            f.seek(0, os.SEEK_END)
            if f.tell() == 0:
                return
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                f.write(b"\n")

    def append(self, ev: Event) -> Event:
        """Record ``ev`` in memory and durably on disk, then return it."""
        line = json.dumps(ev.row(), separators=(",", ":"))
        with self._lock:
            self._ring.append(ev)
            if self.path:
                with self.path.open("a") as f:
                    f.write(line + "\n")
                    f.flush()
                    os.fsync(f.fileno())
        return ev

    def query(self, rs: str | None = None, since: float | None = None,
              types: set[str] | None = None) -> list[Event]:
        """Events oldest first, filtered by set, by ``ts > since`` and by type."""
        with self._lock:
            evs = list(self._ring)
        return [
            e for e in evs
            if (rs is None or e.rs == rs)
            and (since is None or e.ts > since)
            and (types is None or e.type in types)
        ]

    def last(self, rs: str, types: set[str] | None = None) -> Event | None:
        """The newest event of set ``rs``, optionally of the given types."""
        evs = self.query(rs=rs, types=types)
        return evs[-1] if evs else None
