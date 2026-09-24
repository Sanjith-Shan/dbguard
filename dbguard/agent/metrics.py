"""Prometheus metrics for one agent, served at ``/metrics``.

Fences by method (sql, kill, failed), role changes, rebuilds, wake-guard decisions,
self-fences, and the latency of ``GET /primary``, which is how the in-memory answer is shown
to cost microseconds. One registry per agent keeps tests independent.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

ROLES = ("primary", "replica", "fenced", "unknown")


class AgentMetrics:
    """The agent's collectors, on a private registry."""

    def __init__(self) -> None:
        r = self.registry = CollectorRegistry()
        self.fences = Counter("dbguard_agent_fences_total", "Fences by method", ["method"],
                              registry=r)
        self.promotions = Counter("dbguard_agent_promotions_total", "Promotions", registry=r)
        self.repoints = Counter("dbguard_agent_repoints_total", "Repoints", registry=r)
        self.rebuilds = Counter("dbguard_agent_rebuilds_total", "Clone rebuilds", ["outcome"],
                                registry=r)
        self.phantom_gtids = Counter("dbguard_agent_phantom_gtids_discarded_total",
                                     "Transactions discarded by clone rebuilds", registry=r)
        self.heartbeat_writes = Counter("dbguard_agent_heartbeat_writes_total",
                                        "Heartbeat rows written", registry=r)
        self.heartbeat_errors = Counter("dbguard_agent_heartbeat_errors_total",
                                        "Heartbeat write errors", registry=r)
        self.wake_guard = Counter("dbguard_agent_wake_guard_total", "Wake guard decisions",
                                  ["reason", "decision"], registry=r)
        self.self_fences = Counter("dbguard_agent_self_fences_total",
                                   "Self-fences by the lease (manager and replicas gone)",
                                   registry=r)
        self.primary_check = Histogram(
            "dbguard_agent_primary_check_seconds", "GET /primary handler latency",
            buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0), registry=r)
        self.mysqld_restarts = Counter("dbguard_agent_mysqld_restarts_total",
                                       "mysqld restarts by the supervisor", registry=r)
        self.heartbeat_age = Gauge("dbguard_agent_heartbeat_age_seconds",
                                   "Age of this set's heartbeat row as seen locally", registry=r)
        self.heartbeat_stalled = Gauge("dbguard_agent_heartbeat_stalled_seconds",
                                       "How long the in-flight heartbeat write has waited",
                                       registry=r)
        self.role = Gauge("dbguard_agent_role", "1 for the current role", ["role"], registry=r)
        self.fenced = Gauge("dbguard_agent_fenced", "Fence flag", registry=r)
        self.mysqld_alive = Gauge("dbguard_agent_mysqld_alive", "mysqld process alive",
                                  registry=r)
        self.unauthorized = Counter("dbguard_agent_unauthorized_total",
                                    "Requests refused for a missing or wrong agent token",
                                    registry=r)

    def set_role(self, role: str) -> None:
        """Set the one-hot role gauge."""
        for name in ROLES:
            self.role.labels(role=name).set(1 if name == role else 0)

    def render(self) -> bytes:
        """The Prometheus text exposition."""
        return generate_latest(self.registry)
