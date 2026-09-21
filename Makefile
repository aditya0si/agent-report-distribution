# Agent-wise Report Distribution System - ops surface.
#
# Every target is a thin wrapper: the same commands are what CI runs (.github/workflows/ci.yml) and
# what VERIFY.md records. Nothing here tolerates failure.
#
# Environment (see docs/RUNBOOK.md):
#   JAVA_HOME   - required for the PySpark tests (Temurin 21 JRE or any Java 17+)
#   HADOOP_HOME - Windows only: directory containing bin/winutils.exe and bin/hadoop.dll
# The venv lives in .venv and is created by `make setup`.

SHELL := /bin/bash
PYTHON := .venv/Scripts/python.exe
ifeq ($(OS),)
PYTHON := .venv/bin/python
endif
UV := uv
TERRAFORM := terraform
TF_DIR := infra/terraform

export SPARK_LOCAL_IP ?= 127.0.0.1
export PYSPARK_PYTHON ?= $(PYTHON)
export PYSPARK_DRIVER_PYTHON ?= $(PYTHON)
export AGENT_REPORTS_LOG_LEVEL ?= INFO

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "setup        create .venv and install the package + dev requirements"
	@echo "lint         ruff check + ruff format --check"
	@echo "typecheck    mypy over src, scripts and tests"
	@echo "test         pytest with coverage (offline, moto; Spark runs when a JVM is present)"
	@echo "coverage     pytest with a coverage gate"
	@echo "e2e          scripts/e2e_local.py - whole pipeline offline, prints the report"
	@echo "demo         generate a 50k-row day and run the offline end-to-end"
	@echo "cost         recompute the docs/COST.md estimate from live AWS prices"
	@echo "tf-validate  terraform fmt -check + init -backend=false + validate"
	@echo "all          lint typecheck test tf-validate e2e"

.PHONY: setup
setup:
	$(UV) venv .venv --python 3.11
	$(UV) pip install --python $(PYTHON) -r requirements-dev.txt
	$(UV) pip install --python $(PYTHON) -e . --no-deps

.PHONY: lint
lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

.PHONY: typecheck
typecheck:
	$(PYTHON) -m mypy

.PHONY: test
test:
	$(PYTHON) -m pytest tests -q

.PHONY: coverage
coverage:
	$(PYTHON) -m pytest tests -q --cov=agent_reports --cov-report=term-missing --cov-fail-under=85

.PHONY: e2e
e2e:
	$(PYTHON) scripts/e2e_local.py --rows 5000 --shards 2

.PHONY: demo
demo:
	$(PYTHON) -m agent_reports.ingest.cli --out data/raw --report-date 2026-09-20 --rows 50000
	$(PYTHON) scripts/e2e_local.py --rows 5000 --shards 2 --json

.PHONY: cost
cost:
	$(PYTHON) scripts/cost_model.py

.PHONY: tf-validate
tf-validate:
	cd $(TF_DIR) && $(TERRAFORM) fmt -check -recursive
	cd $(TF_DIR) && $(TERRAFORM) init -backend=false -input=false -no-color
	cd $(TF_DIR) && $(TERRAFORM) validate -no-color

.PHONY: spark-smoke
spark-smoke:
	$(PYTHON) -m pytest tests/integration/test_spark_job.py -q

.PHONY: all
all: lint typecheck test tf-validate e2e
