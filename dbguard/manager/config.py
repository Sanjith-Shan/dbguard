"""Config access for the manager: the shared dbguard.config when present, else the shim."""

from __future__ import annotations

try:  # the fleet agent owns dbguard/config.py
    from dbguard.config import FleetConfig, MysqlCreds, SetConfig, load_config
except ImportError:  # pragma: no cover - only before dbguard/config.py exists
    from dbguard.manager._config_shim import (  # noqa: F401
        FleetConfig,
        MysqlCreds,
        SetConfig,
        load_config,
    )
