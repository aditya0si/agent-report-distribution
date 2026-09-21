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

## 2026-09-21 - Milestone 5: tests, and three real bugs they found

- Unit + integration suites added: keys/settings/errors/retry/report shaping/generator/aggregation/
  idempotency/email templates/presign/orchestrator/dispatcher/spark shaping/terraform config, plus
  moto end-to-end and a real local PySpark run.
- The Spark-vs-chunker byte comparison found three genuine bugs that unit tests had missed:
  1. `sum_insured` was written as a copy of `premium` in the Spark job (copy/paste in the select);
  2. commission used the Decimal context default (`ROUND_HALF_EVEN`), so an exact half-paise tie
     rounded down in the chunker but up in Spark (`23380.45 x 0.10`);
  3. Spark's partitioned writer re-sorted by `agent_id` and scrambled the intra-agent row order, so
     DETAIL rows were not sorted by `policy_id`; the finalizer now enforces the contract.
- Also fixed: the dispatch marker key was double-prefixed (`state/state/...`), `format_money("")`
  raised, and `_RESERVED` logging keys did not include `message`/`asctime`.
- Result: **316 tests green, 92% coverage** (see VERIFY.md for the exact run).

## 2026-09-21 - Milestone 6: infrastructure, CI and docs

- Terraform stack under `infra/terraform`: three lifecycle-managed buckets, SQS + DLQ + redrive,
  four least-privilege Lambda roles (no wildcard actions, asserted by a test), log groups with
  retention, metric filters + alarms + dashboard, EventBridge schedules, SES identity/config set, and
  an optional EMR Serverless module (`enable_emr_module`). `terraform fmt -check` and `validate`
  clean.
- `Makefile` (setup/lint/typecheck/test/coverage/e2e/demo/tf-validate/all) and
  `.github/workflows/ci.yml` running the same gates on ubuntu with Java provisioned, plus a job that
  fails if a credential-shaped assignment is ever committed.
- Docs: README (architecture, API table, measured results), `docs/RUNBOOK.md` (local run, AWS
  deploy, SES sandbox, rollback, DLQ-storm and SES-throttling playbooks), `docs/COST.md` (prices
  pulled from the AWS Price List API with source URLs, everything else marked unverified),
  `docs/SCHEMA.md` (raw + report + state schemas).
- Tooling installed outside the repo for the gates: Temurin 21 JRE, Terraform 1.9.8, Hadoop
  winutils/hadoop.dll, GNU Make 3.81.

## 2026-09-21 - Milestone 7: full-size e2e run, two more real bugs, final gates

- Ran the e2e at the full demo size (`--rows 50000 --shards 4`, 52,471 rows / 3,805 agents). It
  **failed loudly**, which is exactly what it is for:
  1. the dispatch loop stopped after 2,000 of 3,805 emails because `max_dispatch_batches` was a fixed
     200 (200 batches x 10 messages); the budget is now derived from the fan-out size;
  2. `receive_records` passed SQS's PascalCase message attributes straight through, so the
     dispatcher's quarantine-date fallback (`stringValue`) never matched — records are now converted
     to the event source mapping's camelCase shape, with a test that goes through the real receive
     path.
- Moved the EMR module to `infra/emr/` (the path the spec names) and referenced it as `../emr` from
  the Terraform root; `fmt -check` + `validate` still clean.
- Final gate results: ruff clean, `ruff format --check` clean, mypy 0 errors (49 files), **333 tests
  green, 92% coverage**, terraform validate clean, secret grep empty, e2e green at 5,000 rows and at
  50,000 rows (see VERIFY.md for the transcripts).

## 2026-09-21 - Milestone 8: hardening pass, gates re-run on the final tree

- `LocalStorage` create-if-absent made atomic for *readers* as well as writers: `O_CREAT|O_EXCL`
  publishes the name before the bytes, so a racing reader could see a zero-byte dispatch marker. The
  bytes now go to a private temp file which is hard-linked into place (`os.link` is atomic and fails
  if the target exists), with an `O_EXCL` fallback for filesystems without hard links.
- `DispatchRecord.from_json` now raises `ConfigError` (with size/prefix context) instead of leaking a
  bare `JSONDecodeError` from a corrupt marker in `state/`.
- Split `build_spark_session` into a pure `session_config()` + a JVM-requiring `build_session()` so the
  Spark configuration is unit-testable without Spark. The split exposed a real bug: the local-master
  patience windows were `setdefault`-ed onto keys the cluster defaults had already set, so they were
  silent no-ops (comment said 600 s, config kept 120 s) - now assigned, with four new tests.
- The incomplete rename (`build_spark_session` still referenced in `__all__`, `main()` and
  `tests/conftest.py`) was caught by `ruff check` (F822/F821) and `mypy`, not by the tests - recorded
  in the VERIFY.md bug table.
- Gates re-run on this tree: ruff + format clean (55 files), mypy 0 errors (49 files), **339 tests
  green, 92% coverage** (2,396 statements, 136 missed), Spark module 5 passed, 5,000-row e2e `E2E OK`,
  50,000-row e2e re-run, terraform validate clean, secret grep empty. README/VERIFY.md numbers updated
  to match the new runs.
- The 50,000-row e2e on the hardened tree finished **`E2E OK`**: 52,471 rows in, 3,805 agents, 3,805
  reports, 3,805 emails, queue drained, DLQ empty, pre-signed link verified byte-for-byte. It took
  1,940 s wall (dispatch stage 1,480 s) because the host was busy with other work during it - the same
  run on a quiet host was 960 s. Both transcripts are in VERIFY.md, labelled with which is which.

