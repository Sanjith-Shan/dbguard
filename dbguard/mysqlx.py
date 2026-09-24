"""MySQL helpers shared by every client of the fleet, the TLS context and replica-status parsing.

The auth decision lives here (docs/INTERFACES.md, accounts section, and docs/BUGS.md). Every
account is ``caching_sha2_password``, and over plain TCP the first login after a mysqld start
needs TLS or an RSA exchange that PyMySQL can only do with the ``cryptography`` package. mysqld
8.4 generates self-signed certificates at first start, so every DBGuard connection uses TLS
with verification off (``insecure_tls()``) and replication uses ``SOURCE_SSL=1``.
``parse_replica_status`` maps SHOW REPLICA STATUS onto the agent's ``/status`` ``replica``
object. The PyMySQL client built on these is ``dbguard.mysqlx_sync``.
"""

from __future__ import annotations

import ssl
from typing import Any


def insecure_tls() -> ssl.SSLContext:
    """TLS context for the fleet's self-signed server certs (encryption, no verification)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _none_if_empty(v: Any) -> Any:
    """None for NULL or a blank string, which SHOW REPLICA STATUS uses for 'not set'."""
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def gtid_text(v: Any) -> str | None:
    """SHOW output wraps GTID sets over lines ("uuid:1-5,\\nuuid2:1-3"). Normalise."""
    if v is None:
        return None
    return "".join(str(v).split())


def parse_replica_status(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Map one SHOW REPLICA STATUS row onto the agent /status ``replica`` object."""
    if not row:
        return None
    sbs = row.get("Seconds_Behind_Source")
    return {
        "configured": True,
        "source_host": _none_if_empty(row.get("Source_Host")),
        "io_running": _none_if_empty(row.get("Replica_IO_Running")),
        "sql_running": _none_if_empty(row.get("Replica_SQL_Running")),
        "seconds_behind_source": int(sbs) if sbs is not None else None,
        "retrieved_gtid_set": gtid_text(row.get("Retrieved_Gtid_Set")),
        "executed_gtid_set": gtid_text(row.get("Executed_Gtid_Set")),
        "last_io_error": _none_if_empty(row.get("Last_IO_Error")),
        "last_sql_error": _none_if_empty(row.get("Last_SQL_Error")),
    }
