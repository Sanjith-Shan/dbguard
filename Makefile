# DBGuard fleet targets. Run from the repo root with the venv active
# (source .venv/bin/activate) or with PY pointing at a python that has the dev deps.
PY       ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
COMPOSE  := docker compose -f deploy/docker-compose.yml
RS1      := mysql-a1:13311,mysql-a2:13312,mysql-a3:13313
RUN_ID   ?= smoke-$(shell date +%s)
DURATION ?= 60

.PHONY: build build-node build-manager up up-naive down ps bootstrap test test-integration \
        lint workload check

build: build-node build-manager

build-node:
	docker build -f deploy/Dockerfile -t dbguard-node .

build-manager:
	docker build -f deploy/Dockerfile.manager -t dbguard-manager .

up:
	DBGUARD_SEMISYNC=1 DBGUARD_MODE=dbguard $(COMPOSE) up -d
	DBGUARD_SEMISYNC=1 $(PY) bin/bootstrap

up-naive:
	DBGUARD_SEMISYNC=0 DBGUARD_MODE=naive $(COMPOSE) up -d
	DBGUARD_SEMISYNC=0 $(PY) bin/bootstrap --naive

down:
	$(COMPOSE) --profile spare --profile orchestrator down -v --remove-orphans

ps:
	$(COMPOSE) ps

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
