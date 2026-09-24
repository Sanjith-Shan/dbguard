"""Prometheus metrics for the manager, served at ``/metrics``.

State per set, failovers by outcome and trigger, per-step durations, phantom GTIDs discarded,
rebuilds, and per-node lag, heartbeat age and semi-sync wait. One registry per Manager, because
tests build several managers in one process.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from dbguard.manager.model import Observation, State

_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30, 60, 120)


class Metrics:
    """The manager's collectors, on a private registry."""

    def __init__(self):
        r = self.registry = CollectorRegistry()
        self.set_state = Gauge("dbguard_set_state", "1 for the current state of the set",
                               ["rs", "state"], registry=r)
        self.failovers = Counter("dbguard_failovers_total", "Failovers by outcome and trigger",
                                 ["rs", "outcome", "trigger"], registry=r)
        self.step_seconds = {
            name: Histogram(f"dbguard_{name}_seconds", f"{name} duration", ["rs"],
                            buckets=_BUCKETS, registry=r)
            for name in ("detect", "fence", "promote", "repoint", "failover_total")
        }
        self.phantom = Counter("dbguard_phantom_gtids_discarded_total",
                               "GTIDs discarded by rebuilding a rejoining node", ["rs"],
                               registry=r)
        self.rebuilt = Counter("dbguard_replicas_rebuilt_total",
                               "Nodes rebuilt by clone (rejoin or replacement)", ["rs", "reason"],
                               registry=r)
        self.semisync_wait = Gauge("dbguard_semisync_avg_wait_us",
                                   "Rpl_semi_sync_source_tx_avg_wait_time on the primary",
                                   ["rs", "node"], registry=r)
        self.lag = Gauge("dbguard_replica_lag_seconds", "Seconds_Behind_Source", ["rs", "node"],
                         registry=r)
        self.hb_age = Gauge("dbguard_heartbeat_age_seconds", "Heartbeat row age seen by a node",
                            ["rs", "node"], registry=r)
        self.probe_ok = Gauge("dbguard_manager_probe_ok", "1 if the last manager probe passed",
                              ["rs"], registry=r)

    def state(self, rs: str, state: State) -> None:
        """Set the one-hot state gauge of ``rs``."""
        for s in State:
            self.set_state.labels(rs=rs, state=s.value).set(1 if s == state else 0)

    def observe(self, rs: str, ob: Observation) -> None:
        """Update the probe and per-node gauges from one poll."""
        if ob.probe is not None:
            self.probe_ok.labels(rs=rs).set(1 if ob.probe.ok else 0)
        for name, nv in ob.nodes.items():
            if not nv.reachable:
                continue
            if nv.semisync.source_enabled:
                self.semisync_wait.labels(rs=rs, node=name).set(nv.semisync.avg_wait_time_us)
            lag = nv.replica.seconds_behind_source
            if lag is not None:
                self.lag.labels(rs=rs, node=name).set(lag)
            if nv.heartbeat_age_s is not None:
                self.hb_age.labels(rs=rs, node=name).set(nv.heartbeat_age_s)

    def render(self) -> bytes:
        """The Prometheus text exposition."""
        return generate_latest(self.registry)
