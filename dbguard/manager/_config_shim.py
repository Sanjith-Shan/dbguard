"""Minimal stand-in for dbguard.config, used only until the fleet's config module lands.

Same model names and fields as docs/INTERFACES.md (deploy/fleet.yaml).
"""

from __future__ import annotations

from typing import Literal

import yaml
from pydantic import BaseModel, Field


class MysqlCreds(BaseModel):
    user: str = "dbguard"
    password: str = "dbguard"
    repl_user: str = "repl"
    repl_password: str = "repl"
    port: int = 3306


class SetConfig(BaseModel):
    nodes: list[str]
    spare: str | None = None


class FleetConfig(BaseModel):
    mode: Literal["dbguard", "naive"] = "dbguard"
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
    sets: dict[str, SetConfig] = Field(default_factory=dict)


def load_config(path) -> FleetConfig:
    with open(path) as f:
        return FleetConfig.model_validate(yaml.safe_load(f) or {})
