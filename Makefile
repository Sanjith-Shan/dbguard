# DBGuard fleet targets. Run from the repo root with the venv active
# (source .venv/bin/activate) or with PY pointing at a python that has the dev deps.
PY       ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
COMPOSE  := docker compose -f deploy/docker-compose.yml
RS1      := mysql-a1:13311,mysql-a2:13312,mysql-a3:13313
RUN_ID   ?= smoke-$(shell date +%s)
DURATION ?= 60

.PHONY: build build-node build-manager up up-naive wait-healthy down ps bootstrap test \
        test-integration \
        lint workload check

build: build-node build-manager

build-node:
	docker build -f deploy/Dockerfile -t dbguard-node .

build-manager:
	docker build -f deploy/Dockerfile.manager -t dbguard-manager .

# The manager bootstraps the sets itself. Do NOT also run bin/bootstrap here: the two raced
# on the same nodes, killed mysql-a1's agent and triggered a failover. bin/bootstrap is for
# the Orchestrator profile only (the harness calls it explicitly with the manager stopped).
up:
	DBGUARD_SEMISYNC=1 DBGUARD_MODE=dbguard $(COMPOSE) up -d
	$(MAKE) --no-print-directory wait-healthy

up-naive:
	DBGUARD_SEMISYNC=0 DBGUARD_MODE=naive $(COMPOSE) up -d
	$(MAKE) --no-print-directory wait-healthy

# Poll the manager until every set is HEALTHY (timeout 180 s), then print dbgctl status.
wait-healthy:
	@$(PY) -c 'import json, sys, time, urllib.request; \
	deadline = time.time() + 180; last = None; \
	exec("while time.time() < deadline:\n try:\n  last = json.load(urllib.request.urlopen(\"http://127.0.0.1:19090/v1/status\", timeout=2))\n  st = {k: v[\"state\"] for k, v in last[\"sets\"].items()}\n  print(st, flush=True)\n  if st and all(x == \"HEALTHY\" for x in st.values()): sys.exit(0)\n except OSError as e:\n  print(\"manager not ready:\", e, flush=True)\n time.sleep(2)"); \
	sys.exit("sets not HEALTHY after 180 s: %s" % (last,))'
	$(PY) -m dbguard.cli.dbgctl status

down:
	$(COMPOSE) --profile spare --profile orchestrator down -v --remove-orphans

ps:
	$(COMPOSE) ps

# Orchestrator profile only, with the dbguard manager stopped (see bin/bootstrap).
bootstrap:
	$(PY) bin/bootstrap

test:
	$(PY) -m pytest -q -m "not integration"

test-integration:
	$(PY) -m pytest -q -m integration

lint:
	$(PY) -m ruff check .

# 60 s smoke through HAProxy rs1, then the checker on rs1.
workload:
	mkdir -p results/tmp
	$(PY) bin/workload --port 13306 --clients 8 --duration $(DURATION) --run-id $(RUN_ID) \
	    --ack-log results/tmp/$(RUN_ID).ack.jsonl --summary-file results/tmp/$(RUN_ID).summary.json
	@echo $(RUN_ID) > results/tmp/last_run_id

check:
	$(PY) bin/checker --run-id $$(cat results/tmp/last_run_id) \
	    --ack-log results/tmp/$$(cat results/tmp/last_run_id).ack.jsonl --nodes $(RS1)
