"""Fleet configuration, the pydantic models behind deploy/fleet.yaml.

Every knob the detector, the failover and replacement read comes from here (detection window,
probe budget, fence and catch-up deadlines, cooldown, rejoin policy). The schema is fixed by
docs/INTERFACES.md ("Config, deploy/fleet.yaml") and unknown keys are rejected, so a typo in
the file fails at startup instead of silently keeping a default. ``DBGUARD_MODE`` in the
environment overrides ``mode``, so one file serves the dbguard fleet and the naive baseline.
``DBGUARD_AGENT_TOKEN``, when non-empty, overrides ``agent_token`` (dbguard/auth.py).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from dbguard.auth import token_from_env

Mode = Literal["dbguard", "naive"]


class MysqlCreds(BaseModel):
    """Accounts the manager uses for SQL and hands to agents for replication."""

    model_config = ConfigDict(extra="forbid")

    user: str = "dbguard"
    password: str = "dbguard"
    repl_user: str = "repl"
    repl_password: str = "repl"
    port: int = 3306


class SetConfig(BaseModel):
    """One replica set, its members in bootstrap order (``nodes[0]`` is the first primary)
    and an optional spare for replacement."""

    model_config = ConfigDict(extra="forbid")

    nodes: list[str] = Field(min_length=1)
    spare: str | None = None

    @model_validator(mode="after")
    def _check_nodes(self) -> SetConfig:
        """Reject a node listed twice or used as both member and spare."""
        if len(set(self.nodes)) != len(self.nodes):
            raise ValueError(f"duplicate node in set: {self.nodes}")
        if self.spare is not None and self.spare in self.nodes:
            raise ValueError(f"spare {self.spare} is also listed in nodes")
        return self


class FleetConfig(BaseModel):
    """The whole fleet.yaml. Defaults follow the spec, and the lab file overrides some of them."""

    model_config = ConfigDict(extra="forbid")

    mode: Mode = "dbguard"
    detect_window_s: float = 5
    probe_timeout_s: float = 1
    probe_failures: int = 3
    fence_deadline_s: float = 3
    catchup_deadline_s: float = 30
    rebuild_after_s: float = 60
    cooldown_s: float = 20
    rejoin: Literal["auto", "manual"] = "auto"
    poll_interval_s: float = 0.5
    mysql: MysqlCreds = Field(default_factory=MysqlCreds)
    agent_port: int = 8080
    agent_token: str | None = Field(default=None, repr=False)
    sets: dict[str, SetConfig]

    @model_validator(mode="after")
    def _check_sets(self) -> FleetConfig:
        """Reject a node that belongs to two sets."""
        seen: dict[str, str] = {}
        for rs, sc in self.sets.items():
            for n in [*sc.nodes, *([sc.spare] if sc.spare else [])]:
                if n in seen:
                    raise ValueError(f"node {n} is in both {seen[n]} and {rs}")
                seen[n] = rs
        return self

    def set_of(self, node: str) -> str | None:
        """Return the replica-set name a node (member or spare) belongs to."""
        for rs, sc in self.sets.items():
            if node in sc.nodes or node == sc.spare:
                return rs
        return None


def load_config(path: str | Path, env: dict[str, str] | None = None) -> FleetConfig:
    """Load and validate fleet.yaml. ``DBGUARD_MODE`` and a non-empty ``DBGUARD_AGENT_TOKEN``
    in the environment win over the file."""
    env = os.environ if env is None else env
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: top level must be a mapping")
    mode = env.get("DBGUARD_MODE")
    if mode:
        raw["mode"] = mode
    token = token_from_env(env)
    if token:
        raw["agent_token"] = token
    return FleetConfig.model_validate(raw)
