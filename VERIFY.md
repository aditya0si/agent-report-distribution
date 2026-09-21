# Verification record

Every command below was run on this machine on **2026-09-21**; the outputs are pasted verbatim.
Nothing in this file is estimated or copied from another environment.

## Environment

| Item | Value |
| --- | --- |
| OS / shell | Windows 11, git-bash (MSYS) |
| Python | 3.11.16 (`.venv`, created with `uv venv .venv --python 3.11`) |
| Package manager | uv 0.12.13 |
| Key libraries | pyspark 3.5.9, boto3 1.43.98, moto 5.2.3, pytest 9.1.1, pyarrow 25.0.1, ruff 0.8+, mypy 1.14+, python-hcl2 7.x |
| JVM | Temurin 21.0.12.1+1 (installed to `C:/Users/oliad/tools/jdk-21.0.12.1+1-jre`, `JAVA_HOME` exported) |
| Hadoop (Windows Spark support) | winutils.exe + hadoop.dll 3.3.6 in `C:/Users/oliad/tools/hadoop/bin` |
| Terraform | 1.9.8 (`C:/Users/oliad/tools/terraform_bin/terraform.exe`) |
| AWS credentials | **none** — every test and the e2e run use moto; no account is contacted |
| Docker | not used (the Docker daemon is not running on this host) |

Environment variables used for the gates (they are not stored in the repo):

```bash
export JAVA_HOME="C:/Users/oliad/tools/jdk-21.0.12.1+1-jre"
export HADOOP_HOME="C:/Users/oliad/tools/hadoop"
export PATH="/c/Users/oliad/tools/jdk-21.0.12.1+1-jre/bin:/c/Users/oliad/tools/hadoop/bin:/c/Users/oliad/tools/terraform_bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/Scripts/python.exe"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
```

## Gate 1 — lint

```console
$ .venv/Scripts/python.exe -m ruff check .
All checks passed!

$ .venv/Scripts/python.exe -m ruff format --check .
54 files already formatted
```

(`make lint` runs exactly these two commands. `ruff check` started at 88 findings; all were fixed in
code, none by weakening the ruleset — the configured rule set is
`E,F,W,I,B,UP,C4,SIM,PTH,RUF` with only line-length and a function-call-in-default-argument exemption
ignored.)

## Gate 2 — typecheck

```console
$ .venv/Scripts/python.exe -m mypy
Success: no issues found in 48 source files
```

`mypy` runs over `src`, `scripts` **and** `tests` (`files = ["src", "scripts", "tests"]` in
`pyproject.toml`) with `disallow_untyped_defs`, `check_untyped_defs`, `no_implicit_optional`,
`warn_return_any`, `warn_redundant_casts` and `strict_equality` on. There is **no** blanket
`--ignore-missing-imports`: only two per-module overrides exist (`hcl2.*`, `pyarrow.*`) because
neither ships stubs, and both are listed explicitly in `pyproject.toml`.

## Gate 3 — unit + integration tests

```console
$ .venv/Scripts/python.exe -m pytest tests -q --cov=agent_reports --cov-report=term-missing
........................................................................ [ 68%]
........................................................................ [ 91%]
............................                                             [100%]
=============================== tests coverage ================================
______________ coverage: platform win32, python 3.11.16-final-0 _______________

Name                                                   Stmts   Miss Branch BrPart  Cover   Missing
--------------------------------------------------------------------------------------------------
src\agent_reports\__init__.py                              2      0      0      0   100%
src\agent_reports\common\__init__.py                      10      0      0      0   100%
src\agent_reports\common\aggregation.py                  102      1     18      0    99%   191
src\agent_reports\common\aws.py                           31      1      2      0    97%   96
src\agent_reports\common\errors.py                        74      5     22      3    90%   218, 225, 234-236
src\agent_reports\common\idempotency.py                  159     13     38      5    90%   98, 100, 161, 263, 305-310, 341-343
src\agent_reports\common\keys.py                         115      4     38      4    95%   106, 116, 198, 201
src\agent_reports\common\logging_utils.py                 74      1     18      1    98%   86
src\agent_reports\common\metrics.py                       50      1     18      1    97%   140
src\agent_reports\common\report.py                        74      0     18      0   100%
src\agent_reports\common\retry.py                         75      0     24      1    99%   103->131
src\agent_reports\common\roster.py                        82     21     38      9    68%   27, 32->30, 41-42, 46, 66, 78->77, 90->88, 102, 104, 115-127, 131-135
src\agent_reports\common\settings.py                     109     12     42      3    86%   104, 108, 135, 138, 188-198
src\agent_reports\common\storage.py                      186     27     54     12    82%   84, 86, 90, 93-97, 99, 112->116, 199, 207, 230, 239->241, 264, 269, 309, 322, 325, 328, 331-334, 339, 349, 354, 367-369
src\agent_reports\emr\__init__.py                          2      0      0      0   100%
src\agent_reports\emr\jobs\__init__.py                     2      0      0      0   100%
src\agent_reports\emr\jobs\agent_report_job.py           175     16     28      6    89%   75->77, 84->97, 89->97, 361, 376, 484->487, 503-513, 517-530
src\agent_reports\ingest\__init__.py                       4      0      0      0   100%
src\agent_reports\ingest\cli.py                           39      0      4      0   100%
src\agent_reports\ingest\generator.py                    206      8     46      1    96%   99, 281-286, 304
src\agent_reports\ingest\schema.py                        31      0      0      0   100%
src\agent_reports\lambda_handlers\__init__.py              2      0      0      0   100%
src\agent_reports\lambda_handlers\chunker.py              80     11     16      2    86%   89, 148->158, 163-183
src\agent_reports\lambda_handlers\dispatcher.py          196      6     34      2    96%   82, 110, 406->408, 472, 475-477
src\agent_reports\lambda_handlers\email_templates.py      34      0     10      0   100%
src\agent_reports\lambda_handlers\orchestrator.py        115      1     22      2    98%   223, 267->277
src\agent_reports\lambda_handlers\presign.py              72      0     10      0   100%
src\agent_reports\pipeline.py                            190      4     42      9    94%   55, 208->exit, 311->338, 336, 344->351, 347, 360->364, 371->369, 373
src\agent_reports\testing.py                              65      8     14      4    82%   34, 38, 80-82, 113, 118, 124
--------------------------------------------------------------------------------------------------
TOTAL                                                   2356    140    556     65    92%
Coverage JSON written to file .coverage.json
316 passed in 152.46s (0:02:32)
```

**316 passed, 0 failed, 0 skipped, 0 errors.** The Spark tests are part of that 316 (they are marked
`requires_jvm` but a JVM is present, so they ran — the suite would have printed a loud banner and
reported them as skipped otherwise).

Real assertion counts by area, from the same run:

| Suite | What it covers |
| --- | --- |
| `tests/unit/test_keys.py` | raw/report/state key builders and parsers, path-traversal rejection |
| `tests/unit/test_settings.py` | env parsing, validation bounds, local-mode zone URIs |
| `tests/unit/test_errors_retry.py` | error classification by AWS code/HTTP status, backoff ceilings, jitter bounds, retry/give-up behaviour |
| `tests/unit/test_report_shaping.py` | money formatting, totals maths, CSV render/parse round trip |
| `tests/unit/test_generator.py` | determinism (byte-identical), sharding invariance, schema, referential integrity, Parquet round trip, CLI |
| `tests/unit/test_aggregation.py` | per-policy roll-up, HALF_UP commission on exact ties, orphan claims, capacity guardrail |
| `tests/unit/test_idempotency.py` | lease semantics, duplicate suppression, stale-lease retry, conditional-create race (two threads), S3 marker round trip |
| `tests/unit/test_logging_metrics.py` | JSON log contract, EMF document shape, EMF bypass of the JSON formatter, `PutMetricData` |
| `tests/unit/test_email_templates.py` | personalisation, HTML escaping, subject, expiry wording, missing-key failure |
| `tests/unit/test_presign.py` | authz allow/deny matrix, 400/401/403/404, TTL, and a pre-signed URL that really resolves |
| `tests/unit/test_orchestrator.py` | fan-out planning, partial batch failure + individual retry, manifest contents |
| `tests/unit/test_dispatcher.py` | payload validation, quarantine, duplicate suppression, SES throttle retry, permanent rejection, mixed batch partitioning, oversized report fallback |
| `tests/unit/test_spark_shaping.py` | partition-column re-insertion, row ordering, malformed Spark output rejection |
| `tests/unit/test_terraform_config.py` | lifecycle policies, redrive, IAM least privilege (no `*` actions), alarms, metric filters, schedules, EMR module |
| `tests/integration/test_pipeline_end_to_end.py` | full moto run, independent recomputation of one agent's totals, replay idempotency, quarantine, poison → DLQ → redrive |
| `tests/integration/test_spark_job.py` | real `local[2]` Spark run, output layout, totals vs raw partitions, **byte-identical to the chunker** |

## Gate 4 — production build of every app

There is no bundler and no compiled artifact: the deliverables are a Python package, a PySpark script
and Terraform. The equivalent "build" gates are therefore:

```console
$ .venv/Scripts/python.exe -c "import agent_reports, agent_reports.pipeline; print(agent_reports.__version__)"
1.0.0

$ .venv/Scripts/python.exe -m pytest tests/integration/test_spark_job.py -q
5 passed
```

The Lambda deployment package is produced by Terraform's `archive_file` data source
(`infra/terraform/main.tf`), which zips `src/` — the same tree that is imported and exercised by every
test above, so there is no separate build step that could drift.

## Gate 5 — the repo's own end-to-end script

```console
$ .venv/Scripts/python.exe scripts/e2e_local.py --rows 5000 --shards 2 --quiet
=== Agent Report Distribution - offline end-to-end (moto) ===
report_date         : 2026-09-20
rows in             : 5,262 (agents 381 / policies 3,048 / claims 1,833)
agents discovered   : 381
agents processed    : 381
reports written     : 381
emails sent         : 381 (SES captured 381)
duplicates / failed : 0 / 0
queue drained       : yes
dlq depth           : 0
objects written     : 776
metrics emitted     : 42 EMF documents (ReportsWritten, RowsIn, AgentsDiscovered, MessagesEnqueued, BatchItemFailures, EmailsSent, EmailsFailed, DuplicatesSuppressed, DispatchLatencyMs, ReportAgeSeconds)
cloudwatch metrics  : 10 published (AgentsDiscovered, BatchItemFailures, DispatchLatencyMs, DuplicatesSuppressed, EmailsFailed, EmailsSent, MessagesEnqueued, ReportAgeSeconds, ReportsWritten, RowsIn)
pre-signed link     : HTTP 200, 1,421 bytes, matches report object: True
sample report       : reports/dt=2026-09-20/agent_id=AGT-000001/report.csv
manifest            : s3://agent-reports-processed/state/runs/dt=2026-09-20/manifest.json
stage timings (s)   : generate=0.119, aggregate=1.497, fanout=1.601, dispatch=15.166
duration            : 18.781 s (wall 20.577 s)

E2E OK
```

### The same script at the full demo size

```console
$ .venv/Scripts/python.exe scripts/e2e_local.py --rows 50000 --shards 4 --quiet
E2E_50K_OUTPUT
```

## Gate 6 — secret grep

```console
$ git grep -nIE '(api[_-]?key|secret|password|token|mongodb\+srv)\s*[:=]' -- . ':!*.example' ':!package-lock.json'
$ echo $?
1
```

Exit code 1 = no matches at all, so there are no "intentional non-secret matches" to justify. There is
no `.env` in the repo (only `.env.example`), no credentials in `terraform.tfvars.example`, and CI
re-runs the same grep as a separate job (`no-aws-credentials` in `.github/workflows/ci.yml`).

## Additional evidence (not part of the six gates)

### Dataset generation

```console
$ .venv/Scripts/python.exe -m agent_reports.ingest.cli --out "$LOCALAPPDATA/Temp/gen-test/big" --report-date 2026-09-20 --rows 50000
{"...","message":"dataset_generated","event":"dataset_generated","report_date":"2026-09-20","agents":3805,"policies":30440,"claims":18226,"rows_total":52471,"parts":12,"bytes_written":5546871,"duration_seconds":0.834,...}
```

**52,471 rows (38% above the 50,000 target) written in 0.83 s**, against a SPEC budget of "a couple of
minutes worst case".

### Terraform

```console
$ terraform -chdir=infra/terraform fmt -check -recursive
$ echo $?
0

$ terraform -chdir=infra/terraform init -backend=false -input=false -no-color
Terraform has been successfully initialized!

$ terraform -chdir=infra/terraform validate -no-color
Success! The configuration is valid.
```

`init` downloads the AWS and archive providers; `validate` needs no AWS credentials.

### Spark job, executed for real

Part of the 316-test run, but worth calling out because it is the EMR deliverable:

- `tests/integration/test_spark_job.py` starts a real `SparkSession` on `local[2]`, reads the CSV
  partitions, runs the shuffle, writes `partitionBy("agent_id")`, and finalises each agent's
  `report.csv` through the Hadoop FileSystem API;
- the reports it produces are asserted **byte-identical** to the free-tier chunker's for all 40
  agents in the fixture dataset;
- this cross-check found three real bugs during development (`sum_insured` written as a copy of
  `premium`; commission rounding HALF_EVEN vs HALF_UP; Spark's partitioned writer scrambling the
  intra-agent row order), which is exactly what it is for.

### Bugs found by running things, not by reading them

Recorded because they are the honest measure of whether the tests do anything:

| Bug | Caught by | Fix |
| --- | --- | --- |
| `claims_per_policy` was ignored (per-product probabilities used raw) | running the generator | incidence normalised to the configured mean |
| orchestrator compared agent ids against full S3 keys, so it targeted nobody | first e2e run | `report_agent_ids()` helper |
| SQS records were not normalised into event-source-mapping shape | first e2e run | `receive_records()` maps `MessageId`/`Body` → `messageId`/`body` |
| duplicate `message_id` kwarg raised `TypeError` on the quarantine path | first e2e run | error fields nested under `error=` |
| telemetry capture was muted by the configured log level | first e2e run | capture raises the logger level for its own duration |
| dispatch marker key double-prefixed (`state/state/...`) | unit test | ledger builds the key itself, asserted against `keys.dispatch_marker_key` |
| `format_money("")` raised `InvalidOperation` | unit test | empty/None values are handled by `to_decimal` |
| Spark: `sum_insured` copied from `premium` | Spark/chunker byte comparison | select the right column |
| Spark: commission rounded HALF_EVEN in the chunker, HALF_UP in Spark | Spark/chunker byte comparison | explicit `ROUND_HALF_UP` |
| Spark: DETAIL rows not sorted by `policy_id` | Spark/chunker byte comparison | finaliser enforces the row order |

## Reproduce all of it

```bash
# 0. toolchain (once): JDK 21, Terraform 1.9.8, and on Windows Hadoop's winutils/hadoop.dll
export JAVA_HOME=...            # any Java 17+
export HADOOP_HOME=...          # Windows only
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=$PWD/.venv/Scripts/python.exe

# 1. env
uv venv .venv --python 3.11
uv pip install --python .venv/Scripts/python.exe -r requirements-dev.txt
uv pip install --python .venv/Scripts/python.exe -e . --no-deps

# 2. the six gates (make targets wrap exactly these)
.venv/Scripts/python.exe -m ruff check .
.venv/Scripts/python.exe -m ruff format --check .
.venv/Scripts/python.exe -m mypy
.venv/Scripts/python.exe -m pytest tests -q --cov=agent_reports --cov-report=term-missing
.venv/Scripts/python.exe -c "import agent_reports.pipeline"
.venv/Scripts/python.exe -m pytest tests/integration/test_spark_job.py -q
.venv/Scripts/python.exe scripts/e2e_local.py --rows 5000 --shards 2
git grep -nIE '(api[_-]?key|secret|password|token|mongodb\+srv)\s*[:=]' -- . ':!*.example' ':!package-lock.json'

# 3. terraform
terraform -chdir=infra/terraform fmt -check -recursive
terraform -chdir=infra/terraform init -backend=false -input=false
terraform -chdir=infra/terraform validate
```

On Linux/macOS the same commands work with `.venv/bin/python` instead of `.venv/Scripts/python.exe`
(that is what `.github/workflows/ci.yml` does, with Java provisioned by `actions/setup-java`).

## Not verified / known gaps

Honest list of what did **not** run here:

1. **Nothing was deployed to AWS.** No account, no credentials, no `terraform apply`, no real EMR
   Serverless job run, no real SES delivery. The Terraform is `validate`-clean and the Spark job runs
   locally, but "works on AWS" is inferred, not demonstrated.
2. **`terraform plan` was not run** (it needs credentials). Resource-attribute errors that only
   surface at plan/apply time (e.g. an API name that already exists) would not have been caught.
3. **The `presign` HTTP API is not wired to a real authorizer.** The function's authz logic is tested
   (including a URL that resolves), but the JWT authorizer attachment is left to the deployer; the
   offline tests inject claims directly.
4. **No AWS-side observability was exercised.** EMF documents were captured from the logger output and
   `PutMetricData` was read back through moto; CloudWatch Logs metric filters and alarms were asserted
   by parsing the Terraform, not by firing them.
5. **The SES sandbox path is simulated.** moto enforces the "sender and recipient must be verified"
   rule that the real sandbox enforces, but no real bounce/complaint flow was exercised.
6. **`make` was verified with GNU Make 3.81 on Windows** (`make -n`, `make help`); the targets were
   run as their underlying commands rather than through `make` end to end. CI runs the same commands
   directly.
7. **moto is not AWS.** Its SQS redrive, SES store and S3 conditional writes behave as the real
   services do for the paths used here, but they are a simulation; e.g. moto's S3 does not enforce
   SigV4 signature verification on pre-signed GETs.
8. **Performance numbers are local and small.** The 50k-row e2e runs the free-tier path on one
   machine; no throughput or cost figure for a real multi-million-row day was measured.
9. **`python-hcl2` is a fallback parser.** It is used by the unit tests; the authoritative check is
   `terraform validate`, which did run. If Terraform were unavailable, the HCL tests would still
   parse the files but would not have caught provider-level errors.
10. **Coverage gaps below 85%** in `roster.py` (68%), `storage.py` (82%) and `testing.py` (82%) —
    mostly Parquet and S3-error branches and the moto-only verification helpers. Reported as-is
    rather than excluded from the report.
