"""Fleet configuration (deploy/fleet.yaml), pydantic v2 models.

The schema is fixed by docs/INTERFACES.md ("Config, deploy/fleet.yaml").
The environment variable ``DBGUARD_MODE`` overrides ``mode`` so the same file serves
both the dbguard fleet and the naive baseline.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

Mode = Literal["dbguard", "naive"]


class MysqlCreds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user: str = "dbguard"
    password: str = "dbguard"
    repl_user: str = "repl"
    repl_password: str = "repl"
    port: int = 3306


class SetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nodes: list[str] = Field(min_length=1)
    spare: str | None = None

    @model_validator(mode="after")
    def _check_nodes(self) -> SetConfig:
        if len(set(self.nodes)) != len(self.nodes):
            raise ValueError(f"duplicate node in set: {self.nodes}")
        if self.spare is not None and self.spare in self.nodes:
            raise ValueError(f"spare {self.spare} is also listed in nodes")
        return self


class FleetConfig(BaseModel):
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
    sets: dict[str, SetConfig]

    @model_validator(mode="after")
    def _check_sets(self) -> FleetConfig:
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
    """Load and validate fleet.yaml. ``DBGUARD_MODE`` in the environment wins over the file."""
    env = os.environ if env is None else env
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: top level must be a mapping")
    mode = env.get("DBGUARD_MODE")
    if mode:
        raw["mode"] = mode
    return FleetConfig.model_validate(raw)
