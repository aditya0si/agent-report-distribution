# Progress log

Appended after each milestone. Newest entries at the bottom.

## 2026-09-21 - Milestone 1: environment + scaffold

- Provisioned the toolchain the repo needs (none of it lives in the repo):
  - Temurin 21 JRE (`C:/Users/oliad/tools/jdk-21.0.12.1+1-jre`), `JAVA_HOME` exported.
  - Terraform 1.9.8 (`C:/Users/oliad/tools/terraform_bin/terraform.exe`).
  - Hadoop `winutils.exe` + `hadoop.dll` 3.3.6 (`C:/Users/oliad/tools/hadoop/bin`) - Spark on Windows
    needs these for local file access (`UnsatisfiedLinkError: NativeIO$Windows.access0` without them).
  - Python env via `uv venv .venv` + `uv pip install -r requirements-dev.txt` (pyspark 3.5.9,
    boto3 1.43.98, moto 5.2.3, pytest 9.1.1, pyarrow 25.0.1).
- `git init`, `.gitignore`, MIT LICENSE (Aditya Singh), `.env.example`, `pyproject.toml`.
- Verified a real local Spark session (read CSV -> decimal aggregation -> `partitionBy` CSV write ->
  Hadoop FS rename to `report.csv`) before writing the job, so the design is known to work on this box.

## 2026-09-21 - Milestone 2: shared library (`agent_reports.common`)

- `settings.py` (env-driven, validated, local-mode switch), `keys.py` (raw/reports/state layout with
  round-trip parsers), `storage.py` (`S3Storage` / `LocalStorage` / `open_zones`),
  `errors.py` (retryable vs permanent taxonomy), `retry.py` (exponential backoff + full jitter),
  `logging_utils.py` (JSON logs + EMF side channel), `metrics.py` (EMF documents),
  `idempotency.py` (lease-based dispatch ledger), `report.py` (canonical CSV shape),
  `aggregation.py` (streaming free-tier aggregator), `roster.py` (raw readers + shard planning).
- Design decision recorded: the Lambda package is `lambda_handlers` because `lambda` is a Python
  keyword; Terraform function names keep the `-orchestrator` / `-dispatcher` / `-presign` / `-chunker`
  naming.

## 2026-09-21 - Milestone 3: ingest generator

- Deterministic synthetic dataset generator: 3 sources (agents / policies / claims), partitioned
  output, CSV + Parquet, streamed writes, documented schema, no PII (`example.com` addresses).
- `agent-reports generate --rows 50000` produced **52,471 rows in 0.83 s** locally (well inside the
  "couple of minutes" budget).
- Fixed a real bug found by running it: `claims_per_policy` was being ignored because per-product
  claim probabilities were used raw. Claim incidence is now normalised so the configured mean holds.

## 2026-09-21 - Milestone 4: handlers + Spark job + pipeline

- `lambda_handlers/chunker.py` (free-tier aggregation), `orchestrator.py` (fan-out with partial batch
  handling), `dispatcher.py` (presign + SES + idempotency + batchItemFailures), `presign.py`
  (API-Gateway-style with deny-by-default authz), `email_templates.py` + package templates.
- `emr/jobs/agent_report_job.py`: the EMR-deployable PySpark job, byte-compatible with the chunker.
- `pipeline.py` (single definition of the pipeline) + `testing.py` (moto provisioning/verification) +
  `scripts/e2e_local.py`.
- `scripts/e2e_local.py --rows 500 --shards 2` -> **E2E OK**: 544 rows in, 39 agents, 39 reports,
  39 emails, queue drained, pre-signed link returns the report bytes, 7 EMF documents, 10 CloudWatch
  metrics. Bugs fixed along the way: report-key vs agent-id mix-up in the orchestrator, SQS records
  not normalised into event-source-mapping shape, duplicate `message_id` kwarg in a log call, and
  telemetry capture being muted by the configured log level.
