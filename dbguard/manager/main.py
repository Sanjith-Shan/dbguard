"""`dbguard`, the manager daemon.

    dbguard --config deploy/fleet.yaml [--mode naive] [--rejoin manual]
            [--state-dir /var/lib/dbguard] [--listen 0.0.0.0:9090] [--host-ports]

Running on the host instead of in the compose network: ``--host-ports`` (or
``DBGUARD_HOST_PORTS=1``) reaches each node on its published ports (mysql-a1 is
127.0.0.1:13311 and agent 127.0.0.1:18011), and ``DBGUARD_HOST_MAP`` overrides single
nodes, ``mysql-a1=127.0.0.1:13311:18011,...``. Stop the in-fleet ``dbguard`` container
first: two managers acting on one fleet is exactly the split brain this avoids.

SIGTERM and SIGINT stop polling and exit. Shutdown never acts on the fleet.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

import click
import structlog
from aiohttp import web

from dbguard.manager.api import make_app
from dbguard.manager.client import Addressing
from dbguard.manager.config import load_config
from dbguard.manager.manager import Manager
from dbguard.manager.replacement import ComposeProvisioner, NullProvisioner


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout,
                        level=getattr(logging, level.upper(), logging.INFO))
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


async def serve(cfg, *, mode, rejoin, state_dir, listen, host_ports, provision) -> None:
    log = structlog.get_logger("dbguard.manager.main")
    addr = Addressing.from_env(cfg.agent_port, cfg.mysql.port, host_ports=host_ports)
    prov = ComposeProvisioner() if provision == "compose" else (
        NullProvisioner() if provision == "null" else None)
    mgr = Manager(cfg, addressing=addr, state_dir=state_dir, mode=mode, rejoin=rejoin,
                  provisioner=prov)
    app = make_app(mgr)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    host, _, port = listen.rpartition(":")
    site = web.TCPSite(runner, host or "0.0.0.0", int(port))
    await site.start()
    await mgr.start()
    log.info("listening", listen=listen, mode=mgr.mode, state_dir=state_dir,
             host_ports=host_ports, host_map=bool(addr.overrides))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    log.info("signal received, shutting down without touching the fleet")
    await mgr.stop()
    await runner.cleanup()


@click.command()
@click.option("--config", "config_path", default=lambda: os.environ.get(
    "DBGUARD_CONFIG", "/etc/dbguard/fleet.yaml"), show_default="$DBGUARD_CONFIG")
@click.option("--mode", type=click.Choice(["dbguard", "naive"]), default=None,
              help="override fleet.yaml mode")
@click.option("--rejoin", type=click.Choice(["auto", "manual"]), default=None,
              help="override fleet.yaml rejoin")
@click.option("--state-dir", default=lambda: os.environ.get("DBGUARD_STATE_DIR",
                                                            "/var/lib/dbguard"))
@click.option("--listen", default=lambda: os.environ.get("DBGUARD_LISTEN", "0.0.0.0:9090"))
@click.option("--host-ports", is_flag=True, default=False,
              help="reach nodes on their host-published ports (running outside compose)")
@click.option("--provision", type=click.Choice(["compose", "null", "off"]),
              default=lambda: os.environ.get("DBGUARD_PROVISION", "compose"),
              help="how replacement replicas are provisioned")
@click.option("--log-level", default="INFO")
def main(config_path, mode, rejoin, state_dir, listen, host_ports, provision, log_level):
    """DBGuard manager daemon."""
    setup_logging(log_level)
    cfg = load_config(config_path)
    asyncio.run(serve(cfg, mode=mode, rejoin=rejoin, state_dir=state_dir, listen=listen,
                      host_ports=host_ports, provision=provision))


if __name__ == "__main__":
    main()
