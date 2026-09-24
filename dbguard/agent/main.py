"""``dbguard-agent`` entry point, the container's main process under tini.

It builds the agent with a real SQL layer and, unless ``--no-supervise``, a supervisor that
starts mysqld as the agent's own child. Owning the process is what lets the fence fall back to
SIGKILL and what lets mysqld restart after CLONE. ``--no-supervise`` attaches to a mysqld the
agent did not start (local development, tests).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

import structlog
from aiohttp import web

from dbguard.agent.core import Agent
from dbguard.agent.db import MySQL
from dbguard.agent.http import make_app
from dbguard.agent.settings import AgentSettings
from dbguard.agent.supervisor import MysqldSupervisor, NullSupervisor


def setup_logging(level: str = "INFO") -> None:
    """structlog JSON lines on stdout, aiohttp access and aiomysql quietened."""
    logging.basicConfig(format="%(message)s", stream=sys.stdout,
                        level=getattr(logging, level.upper(), logging.INFO))
    for noisy in ("aiohttp.access", "aiomysql"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
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


def build_agent(settings: AgentSettings) -> Agent:
    """An Agent with its SQL layer and supervisor, the supervisor calling back on restarts."""
    db = MySQL(settings.mysql_host, settings.mysql_port, settings.mysql_user,
               settings.mysql_password,
               fallback=("root", settings.root_password) if settings.root_password else None,
               default_timeout=settings.sql_timeout_s)
    agent_ref: list[Agent] = []
    if settings.supervise:
        sup = MysqldSupervisor(settings.mysqld_cmd,
                               on_restart=lambda: agent_ref[0].on_mysqld_restart())
    else:
        sup = NullSupervisor()
    agent = Agent(settings, db, sup)
    agent_ref.append(agent)
    return agent


async def run(settings: AgentSettings) -> None:
    """Serve the API and run the agent until SIGTERM or SIGINT, then stop mysqld."""
    log = structlog.get_logger("dbguard.agent")
    agent = build_agent(settings)
    app = make_app(agent)
    runner = web.AppRunner(app, access_log=None, handle_signals=False)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.port, reuse_address=True)
    await site.start()
    log.info("agent_listening", port=settings.port, supervise=settings.supervise,
             cmd=settings.mysqld_cmd if settings.supervise else None)
    await agent.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    log.info("agent_shutdown")
    await agent.stop()          # SIGTERM to mysqld, waits for it
    await runner.cleanup()
    log.info("agent_exit")


def main(argv: list[str] | None = None) -> None:
    """Parse flags, read the environment, run."""
    p = argparse.ArgumentParser(prog="dbguard-agent")
    p.add_argument("--no-supervise", action="store_true",
                   help="attach to a running mysqld instead of starting one")
    p.add_argument("--port", type=int, default=None, help="HTTP port (env DBGUARD_AGENT_PORT)")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    setup_logging(args.log_level)
    settings = AgentSettings.from_env()
    if args.no_supervise:
        settings.supervise = False
    if args.port:
        settings.port = args.port
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
