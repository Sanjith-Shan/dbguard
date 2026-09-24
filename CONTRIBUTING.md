# Contributing to DBGuard

This file says how to run each part of the project and where a change belongs. `docs/INTERFACES.md` is the contract every component builds against, so a change to an API route, an event field, a result row key or a config knob starts there.

## Setup

Python 3.12, Docker Desktop with at least 8 GB of memory for the fleet, and `make`.

```sh
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
```

## Unit tests and lint

The unit and hypothesis suites need no Docker. They take about 75 seconds and run about 290 tests, including whole failovers against the simulated fleet in `dbguard/manager/fake.py`.

```sh
make lint                                # ruff check .
make test                                # pytest -q -m "not integration"
```

Both must be green before every commit.

## Integration tests

Tests marked `integration` talk to real MySQL and skip themselves when nothing answers. The GTID oracle test compares `dbguard/gtid.py` with `GTID_SUBSET` and `GTID_SUBTRACT` on `mysql-a1` at 127.0.0.1:13311, so it needs the fleet up. The schema-change test needs the throwaway `osc-dev` container on port 23306 described in `docs/OSC.md`.

```sh
make test-integration                    # pytest -q -m integration
```

Do not run them while a chaos campaign is using the fleet.

## The fleet

```sh
make build                               # images dbguard-node and dbguard-manager
make up                                  # semi-sync fleet, waits until both sets are HEALTHY
make up-naive                            # the asynchronous baseline instead
dbgctl status                            # state, primary and replicas of each set
dbgctl doctor rs1                        # the set explained in sentences
make down                                # removes every container and volume
```

The manager bootstraps the sets itself. `bin/bootstrap` is only for the Orchestrator profile, with the manager stopped.

## One chaos run

Each run appends one row to `results/<scenario>_<mode>.jsonl`, and a run that raised goes to `results/<scenario>_<mode>.errors.jsonl` instead. Those files are the measurements, so point `--out` at a scratch file when experimenting.

```sh
bin/chaos --scenario kill --runs 1 --out /tmp/kill-try.jsonl
bin/chaos --scenario kill --runs 30      # a real entry, appended to results/kill_dbguard.jsonl
bin/chaos --scenario kill --runs 30 --mode naive
```

The unattended campaign is `bin/campaign`, which works through `results/campaign-queue.json` and can be restarted at any point. Creating `results/campaign.pause` holds it at the next entry boundary, and `results/chaos.stop` ends the current scenario at the next run boundary.

## The report and the doc filler

```sh
bin/report                               # tables from results/*.jsonl, writes SUMMARY.md and summary.json
bin/report --no-write                    # print only
bin/fill-docs                            # every [[N: ...]] tag with its value and status, changes nothing
bin/fill-docs --write                    # replace every resolved tag in place
```

A tag reported as `unmapped` needs an entry in `MAP` in `dbguard/harness/fill.py`. Refilling after more runs means restoring the tags from git first, because `--write` removes the tags it fills.

## Branch and commit conventions

There is one long-lived branch, `main`, and commits go straight to it. Each commit is one logical change with ruff and the unit suite green.

- The subject is `area: what changed`, where the area is the module or directory touched, such as `agent`, `manager`, `rejoin`, `harness`, `campaign`, `results`, `docs` or `tests`.
- The body says why, with the measurement or the failing run that prompted the change.
- Stage files by name. Several people and agents commit to the same tree, so never `git add -A`, stash or reset.
- Result rows are committed separately from code.
- A change to the agent, the manager or `deploy/` after a campaign has started changes what is being measured. Record it in `results/CONFIG_HISTORY.md`, rerun the affected entries, and move the tag that pins the measured configuration (currently `campaign-config-2026-09-24`).

## Adding a chaos scenario

Everything lives in `dbguard/harness/chaos.py`.

1. Document the scenario and every new row key under the harness additions in `docs/INTERFACES.md` first.
2. Write `sc_<name>(run: Run)`, which injects the fault, waits for the outcome and fills `run.row`. `sc_kill` is the shortest example.
3. Register the name in `SCENARIOS`, `ALL_ORDER` and `SCENARIO_FN`.
4. Unit-test any pure analysis it adds in `tests/test_chaos.py`.
5. If it is a failover scenario, add it to `FAILOVER_SCENARIOS` in `dbguard/harness/report.py`. If the docs quote its numbers, add their tags to `MAP` in `dbguard/harness/fill.py`.
6. Add an entry to the campaign queue to measure it.
