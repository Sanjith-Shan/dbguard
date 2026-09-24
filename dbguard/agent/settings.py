"""Agent configuration, read from the environment only (docs/INTERFACES.md)."""

from __future__ import annotations

import os
import shlex
from dataclasses import dataclass, field


def _bool(value: str | None, default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class AgentSettings:
    node: str = "localhost"
    rs: str = "rs0"
    semisync: bool = True
    manager_url: str | None = None
    port: int = 8080
    state_dir: str = "/var/lib/dbguard"
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "dbguard"
    mysql_password: str = "dbguard"
    root_password: str | None = None
    repl_user: str = "repl"
    repl_password: str = "repl"
    source_port: int = 3306
    mysqld_cmd: list[str] = field(default_factory=lambda: ["docker-entrypoint.sh", "mysqld"])
    supervise: bool = True
    sql_timeout_s: float = 1.0
    status_deadline_s: float = 2.0
    fence_deadline_s: float = 2.0
    role_change_timeout_s: float = 30.0
    heartbeat_interval_s: float = 0.5
    wake_gap_s: float = 3.0
    manager_timeout_s: float = 1.0
    self_fence_after_s: float = 10.0
    self_fence_interval_s: float = 1.0
    clone_timeout_s: float = 3600.0
    restart_wait_s: float = 600.0
    restart_hold_s: float = 20.0
    wake_guard: bool = True

    @property
    def fence_file(self) -> str:
        return os.path.join(self.state_dir, "fenced")

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> AgentSettings:
        e = os.environ if env is None else env
        return cls(
            node=e.get("DBGUARD_NODE") or os.uname().nodename,
            rs=e.get("DBGUARD_RS", "rs0"),
            semisync=_bool(e.get("DBGUARD_SEMISYNC"), True),
            manager_url=(e.get("DBGUARD_MANAGER_URL") or None),
            port=int(e.get("DBGUARD_AGENT_PORT", "8080")),
            state_dir=e.get("DBGUARD_STATE_DIR", "/var/lib/dbguard"),
            mysql_host=e.get("DBGUARD_MYSQL_HOST", "127.0.0.1"),
            mysql_port=int(e.get("DBGUARD_MYSQL_PORT", "3306")),
            mysql_user=e.get("DBGUARD_MYSQL_USER", "dbguard"),
            mysql_password=e.get("DBGUARD_MYSQL_PASSWORD", "dbguard"),
            root_password=e.get("MYSQL_ROOT_PASSWORD") or None,
            repl_user=e.get("DBGUARD_REPL_USER", "repl"),
            repl_password=e.get("DBGUARD_REPL_PASSWORD", "repl"),
            source_port=int(e.get("DBGUARD_SOURCE_PORT", "3306")),
            wake_guard=_bool(e.get("DBGUARD_WAKE_GUARD"), True),
            restart_hold_s=float(e.get("DBGUARD_RESTART_HOLD_S", "20")),
            self_fence_after_s=float(e.get("DBGUARD_SELF_FENCE_AFTER_S", "10")),
            mysqld_cmd=shlex.split(e.get("DBGUARD_MYSQLD_CMD", "docker-entrypoint.sh mysqld")),
        )
