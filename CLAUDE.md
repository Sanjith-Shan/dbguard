# DBGuard, notes for agents working in this repo

MySQL 8.4 replica-set manager with lossless semi-sync failover, measured by a chaos harness on a Docker fleet. `CONTRIBUTING.md` has every command.

## Repo map

```
dbguard/gtid.py        GTID sets as sets (never compare GTID strings)
dbguard/events.py      the event row the harness reads step timings from
dbguard/config.py      fleet.yaml models
dbguard/mysqlx*.py     TLS context, replica-status parsing, PyMySQL client
dbguard/agent/         dbguard-agent, one per node, the only process that changes a role
dbguard/manager/       dbguard, detection, failover, rejoin, switchover, replacement, API
dbguard/cli/           dbgctl
dbguard/osc/           online schema change (dbgctl osc)
dbguard/harness/       chaos, workload, checker, report, fill-docs
bin/                   entry scripts, including bin/chaos and bin/campaign
deploy/                compose fleet, my.cnf, HAProxy, Orchestrator
docs/                  DESIGN, INTERFACES, CAPACITY, RUNBOOK, BUGS, OSC
results/               one JSON row per chaos run, results/<scenario>_<mode>.jsonl
tests/                 pytest and hypothesis, tests/test_manager_sim.py runs whole failovers
```

## The binding contract

`docs/INTERFACES.md` is authoritative over any docstring. Change it first, then the code. Do not rename API routes, config keys, event fields, row keys or CLI flags that the harness or tests use.

## Results

Every number in README.md and docs/ comes from `results/*.jsonl` through `bin/report` or `bin/fill-docs`. Never type a measured number by hand, and never edit or delete rows. A number not measured yet stays a `[[N: what, source]]` tag.

## Measured configurations

The tag `campaign-config-2026-09-24` pins the code the current campaign rows were measured with. Any change to `dbguard/agent`, `dbguard/manager` or `deploy/` after it changes behaviour under test. Record it in `results/CONFIG_HISTORY.md`, rerun the affected entries and move or add a tag. While a campaign runs, do not touch Docker and do not run bin/chaos or bin/campaign unless asked.

## Writing rules for docs

- No em dashes, colons or semicolons in prose. Split the sentence instead. Code, tables and headings are exempt.
- Plain words and short sentences. Say what was measured and on what, a lab on one laptop.
- Unflattering numbers are published too.

## Before committing

```sh
source .venv/bin/activate
ruff check .
pytest -q -m "not integration"
```

Stage files by name, never `git add -A`, stash or reset. Others commit to the same tree. Subjects are `area: what changed`.
