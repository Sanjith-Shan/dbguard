"""Plain data the manager reasons over. No I/O here."""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any

from dbguard.gtid import GtidSet


class State(str, enum.Enum):
    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"
    FAILING_OVER = "FAILING_OVER"
    DEGRADED = "DEGRADED"
    REBUILDING = "REBUILDING"
    HALTED = "HALTED"


def _gs(v: Any) -> GtidSet:
    try:
        return GtidSet.parse(v) if v else GtidSet()
    except ValueError:
        return GtidSet()


@dataclass(frozen=True)
class ReplicaView:
    configured: bool = False
    source_host: str | None = None
    io_running: str | None = None
    sql_running: str | None = None
    seconds_behind_source: int | None = None
    retrieved: GtidSet = field(default_factory=GtidSet)
    executed: GtidSet = field(default_factory=GtidSet)
    last_io_error: str | None = None
    last_sql_error: str | None = None

    @classmethod
    def from_json(cls, d: dict | None) -> ReplicaView:
        d = d or {}
        return cls(
            configured=bool(d.get("configured")),
            source_host=d.get("source_host"),
            io_running=d.get("io_running"),
            sql_running=d.get("sql_running"),
            seconds_behind_source=d.get("seconds_behind_source"),
            retrieved=_gs(d.get("retrieved_gtid_set")),
            executed=_gs(d.get("executed_gtid_set")),
            last_io_error=d.get("last_io_error") or None,
            last_sql_error=d.get("last_sql_error") or None,
        )


@dataclass(frozen=True)
class SemisyncView:
    source_enabled: bool = False
    replica_enabled: bool = False
    source_status: bool = False
    replica_status: bool = False
    source_clients: int = 0
    avg_wait_time_us: int = 0
    no_tx: int = 0
    yes_tx: int = 0

    @classmethod
    def from_json(cls, d: dict | None) -> SemisyncView:
        d = d or {}
        kw = {}
        for k, f in cls.__dataclass_fields__.items():
            v = d.get(k)
            kw[k] = f.default if v is None else type(f.default)(v)
        return cls(**kw)


@dataclass(frozen=True)
class NodeView:
    """What the manager knows about one node after one poll."""

    name: str
    ts: float
    reachable: bool                       # the agent answered /status
    error: str | None = None              # why the agent did not answer
    mysqld_alive: bool = False
    mysqld_responsive: bool = False
    fenced: bool = False
    super_read_only: bool | None = None
    read_only: bool | None = None
    gtid_executed: GtidSet = field(default_factory=GtidSet)
    replica: ReplicaView = field(default_factory=ReplicaView)
    semisync: SemisyncView = field(default_factory=SemisyncView)
    source_reachable: bool | None = None
    heartbeat_age_s: float | None = None
    raw: dict | None = None

    @classmethod
    def unreachable(cls, name: str, error: str, ts: float | None = None) -> NodeView:
        return cls(name=name, ts=ts or time.time(), reachable=False, error=error)

    @classmethod
    def from_status(cls, name: str, d: dict, ts: float | None = None) -> NodeView:
        hb = d.get("heartbeat") or {}
        return cls(
            name=name,
            ts=ts or time.time(),
            reachable=True,
            mysqld_alive=bool(d.get("mysqld_alive")),
            mysqld_responsive=bool(d.get("mysqld_responsive")),
            fenced=bool(d.get("fenced")),
            super_read_only=d.get("super_read_only"),
            read_only=d.get("read_only"),
            gtid_executed=_gs(d.get("gtid_executed")),
            replica=ReplicaView.from_json(d.get("replica")),
            semisync=SemisyncView.from_json(d.get("semisync")),
            source_reachable=d.get("source_reachable"),
            heartbeat_age_s=hb.get("age_s"),
            raw=d,
        )

    # derived -----------------------------------------------------------------------
    @property
    def usable(self) -> bool:
        """Agent answers and mysqld answers."""
        return self.reachable and self.mysqld_responsive

    @property
    def writable(self) -> bool:
        return self.usable and self.super_read_only is False and not self.fenced

    @property
    def have(self) -> GtidSet:
        """Everything this node holds, applied or only in its relay log.

        Retrieved_Gtid_Set alone is not cumulative. It is cleared by CHANGE REPLICATION
        SOURCE, RESET REPLICA and relay_log_recovery at restart, so a replica repointed
        earlier can show a small retrieved set while holding everything. Comparing
        retrieved sets alone picks the wrong winner (docs/BUGS.md, Manager).
        """
        return self.gtid_executed | self.replica.executed | self.replica.retrieved

    @property
    def relay_applied(self) -> bool:
        return self.replica.retrieved.is_subset(self.gtid_executed | self.replica.executed)

    def replicating_from(self, primary: str | None) -> bool:
        r = self.replica
        return (
            self.usable
            and r.configured
            and primary is not None
            and r.source_host == primary
            and r.io_running == "Yes"
            and r.sql_running == "Yes"
        )

    @property
    def role(self) -> str:
        if not self.usable:
            return "down"
        if self.fenced:
            return "fenced"
        if self.super_read_only is False:
            return "primary"
        return "replica"

    @property
    def semisync_word(self) -> str:
        if self.semisync.source_enabled:
            return "source"
        if self.semisync.replica_enabled:
            return "replica"
        return "off"


@dataclass(frozen=True)
class ProbeResult:
    """The manager's own probe of the believed primary: SELECT 1 and a heartbeat write."""

    ok: bool
    ts: float
    duration_s: float = 0.0
    select_ok: bool = False     # SELECT 1 answered (the write may still have stalled)
    kind: str | None = None     # timeout | connect | readonly | error
    error: str | None = None

    @property
    def write_stalled(self) -> bool:
        return self.select_ok and not self.ok and self.kind == "timeout"


@dataclass
class Observation:
    """One poll of one set."""

    ts: float
    primary: str | None
    probe: ProbeResult | None
    nodes: dict[str, NodeView]

    def replicas(self, members: list[str]) -> list[NodeView]:
        return [self.nodes[n] for n in members if n != self.primary and n in self.nodes]
