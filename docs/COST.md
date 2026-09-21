# Cost: what this pipeline costs, what the free tier actually covers, and what was avoided

Checked on **2026-09-21**. Prices are US East (N. Virginia) list prices in USD.

**How these numbers were obtained.** They are not hand-arithmetic. `scripts/cost_model.py` holds the
volume model and the price table, fetches the prices from the public AWS Price List API
(`https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<ServiceCode>/current/us-east-1/index.json`
— the same feed the pricing pages are built from) and prints every line below:

```console
$ .venv/Scripts/python.exe scripts/cost_model.py
```

`--recorded` reproduces the same figures without network access. Each price is printed with its
provenance (`AWS Price List API (live)`, `recorded in scripts/cost_model.py`, or `manual: <page>`), so
the two kinds of number can never be confused. Volumes come from commands that were actually run
against this repository; `MEASURED` in the script names the command for each one.

## Volume this estimate is for

| Quantity | Value | Source |
| --- | --- | --- |
| Agents | 4,000 | assumed steady state (the demo default generates 3,805 for a 50k-row day) |
| Policies per day | ~32,000 | 8 policies per agent |
| Raw rows per day | ~52,000 | measured: `agent-reports generate --rows 50000` → 52,471 rows |
| Raw bytes per day | 5,546,871 | measured: `bytes_written` for that run (12 part files) |
| Report objects per day | 4,000 (1,406 bytes each) | measured: `scripts/e2e_local.py` report size |
| Dispatch markers per day | 4,000 (~400 bytes each) | one JSON object per `(date, agent)` |
| Emails per month | 120,000 | 4,000 agents × 30 days |
| Presign API calls per month | ~6,000 | assumed: 5% of deliveries need a fresh link |

## The free tier, as it actually is

The free tier changed on **2025-07-15** and the old "12 months of S3/API Gateway/SES allowances"
story no longer applies to an account created today:

- a new account gets **$100 in credits immediately, and up to $100 more** by using services, over
  **6 months**; the Free plan closes itself after 6 months or when the credits run out, whichever
  comes first ([aws.amazon.com/free](https://aws.amazon.com/free/), read 2026-09-21);
- some services remain **always free** inside monthly usage limits on both plans. The one this
  pipeline actually relies on is Lambda: *"The free tier includes one million requests and 400,000
  GB-seconds per month"* ([aws.amazon.com/lambda/pricing](https://aws.amazon.com/lambda/pricing/),
  read 2026-09-21);
- **SES has no free tier for new accounts.** New SES accounts start on the **Essentials** plan at
  **$0.16 per 1,000 emails** for the first 10M emails/month
  ([aws.amazon.com/ses/pricing](https://aws.amazon.com/ses/pricing/), read 2026-09-21). The old
  "$0.0001 per recipient, 3,000 free" wording is gone.

This matters for the estimate: the 120,000 emails are **billed**, and SES is 71% of the bill.

## Prices used

| Price | Value | Source |
| --- | --- | --- |
| S3 Standard storage | `$0.023`/GB-month | Price List API, `TimedStorage-ByteHrs` |
| S3 Standard-IA storage | `$0.0125`/GB-month | Price List API, `TimedStorage-SIA-ByteHrs` |
| S3 Glacier IR storage | `$0.004`/GB-month | Price List API, `TimedStorage-GIR-ByteHrs` |
| S3 PUT/COPY/POST/LIST | `$0.005` per 1,000 | Price List API, `Requests-Tier1` |
| S3 GET/SELECT/other | `$0.0004` per 1,000 | Price List API, `Requests-Tier2` |
| CloudWatch custom metric | `$0.30`/metric-month (first 10,000) | Price List API, `CW:MetricMonitorUsage` |
| CloudWatch alarm metric | `$0.10`/alarm-metric-month (standard resolution) | Price List API, `CW:AlarmMonitorUsage` |
| CloudWatch Logs ingest | `$0.50`/GB | Price List API, `DataProcessing-Bytes` |
| SES (Essentials) | `$0.16` per 1,000 emails | aws.amazon.com/ses/pricing (manual) |
| SQS standard request | `$0.40` per 1M | Price List API, `AWSQueueService` |
| API Gateway HTTP API | `$1.00` per 1M requests | Price List API, `AmazonApiGateway` |
| Lambda | `$0.0000166667`/GB-s, `$0.20` per 1M requests | Price List API, `AWSLambda` |
| EMR Serverless | `$0.052624`/vCPU-h, `$0.0057785`/GB-h | Price List API, `ElasticMapReduce` |

## S3

**Storage** — the tiers only help above the 128 KB minimum the infrequent-access classes bill:

| Line | GB-month | Notes |
| --- | --- | --- |
| Reports | 0.629 | 4,000 × 1,406 B, 120-day expiry, **Standard** (no IA transition: see below) |
| State markers + manifests | 0.545 | 4,000 × 400 B, 365-day expiry, **Standard** |
| Raw, first 30 days | 0.155 | 5.5 MB/day |
| Raw, next 60 days | 0.310 | Standard-IA (part files are ~500 KB, above the minimum) |
| Raw, to the 400-day expiry | 1.601 | Glacier IR |
| **Storage total** | | **$0.0408/month** |

**Requests** — counted from the code, not guessed. Each delivery costs the dispatcher six GET-type
requests and two PUTs:

| Call | Where |
| --- | --- |
| `HEAD` reports zone | `dispatcher.dispatch_one` → `zones.reports.exists` |
| `HEAD` × 2 + `GET` | `_totals_for_report` → `size`, `last_modified`, `get_bytes` |
| `GET` | `DispatchLedger.claim` reads the marker with its version |
| `GET` | `DispatchLedger.mark_sent` re-reads before the compare-and-set |
| `PUT` × 2 | the claim and the `sent` write (both conditional) |
| `PUT` | the chunker writing the report object |

| Line | Count/month | Cost |
| --- | --- | --- |
| GET-type (6 per email + 1 download) | 840,000 | $0.3360 |
| PUT-type (3 per email + 12 raw parts/day + manifest) | 360,390 | $1.8020 |
| **Requests total** | | **$2.1380/month** |

**S3 total: $2.1788/month.**

### Why the reports zone no longer tiers to Standard-IA

The previous lifecycle moved a ~1.4 KB report to Standard-IA after 14 days. S3 bills a **128 KB
minimum** per object in the infrequent-access classes, so that transition billed 128 KB for a 1.4 KB
object: ~91× the storage, at a rate that is only ~1.8× cheaper, plus a transition request per object.
For 4,000 reports a day over their 120-day life that is **61 GB-month billed instead of 0.67
GB-month**, to save nothing. The transition is gone from the `reports` rule, and from the
`state-retention` rule in the `processed` bucket for the same reason (markers are ~400 B). The raw
zone keeps its tiering because its part files are ~500 KB.

## CloudWatch

| Line | Count | Cost |
| --- | --- | --- |
| Custom metrics | 17 metric-months | $5.10 |
| Log ingest | ~0.1 GB | $0.05 |
| Alarm metrics | 5 (4 alarms; the metric-math alarm reads 2 metrics) | $0.50 |
| **Total** | | **$5.65/month** |

The 17 metric-months are 12 EMF identities (3 from the chunker, 3 from the orchestrator, 6 from the
dispatcher) plus the 5 log metric filters.

**This was ~19× undercounted before.** Six of those metrics used to carry a per-day `ReportDate`
**dimension**. A CloudWatch metric is identified by namespace + name + the *full* dimension set, so
each day created a fresh metric-month for every one of them: 6 × 30 = 180 extra metric-months, about
**$54/month** for the same information. The date is now an EMF **property** (and still in the JSON log
line, where Logs Insights can slice on it), which also makes the metrics alarmable — an alarm cannot
wildcard a dimension value, so `MessagesEnqueued` under a per-day dimension could never be watched by
the "a fan-out ran but nothing was delivered" alarm. `tests/unit/test_terraform_config.py` fails if
any published identity grows a `ReportDate` dimension again.

## Everything else

| Service | Count/month | Cost | Notes |
| --- | --- | --- | --- |
| SES | 120,000 emails | **$19.20** | Essentials plan, $0.16/1,000 — this is the bill |
| SQS | 36,000 requests | $0.0144 | 400 batched sends + 400 batched receives + 400 batched deletes per day |
| API Gateway (HTTP API) | 6,000 requests | $0.0060 | re-issued links only |
| Lambda | 28,950 GB-s, 126,150 requests | $0.0000 | inside the always-free allowance (1M requests, 400,000 GB-s) |
| EventBridge | 2 scheduled rules/day | $0.0000 | scheduled rules are not billed per invocation |

## Free-tier path total (no EMR)

| Line | Monthly |
| --- | --- |
| SES | $19.20 |
| CloudWatch | $5.65 |
| S3 | $2.18 |
| SQS, API Gateway, Lambda, EventBridge | $0.02 |
| **Total** | **~$27.05/month** |

Two thirds of that is SES and it scales linearly with agents. Two levers: (a) send one email per agent
per *week* instead of per day where the contract allows it (saves ~$16/month here), or (b) switch to a
bulk/templated send (`SendBulkTemplatedEmail`) — same per-recipient price, fewer API calls, so it does
not change the bill. The other lever is the CloudWatch metric count: 5 of the 17 metric-months are log
metric filters. One of them (`DispatchFailures`) backs the `dispatcher-errors` alarm; the other four
(`EmailsSentFromLogs`, `MessagesEnqueuedFromLogs`, `BatchItemFailuresFromLogs`, `QuarantinedMessages`)
are the per-day view that Logs Insights cannot give you for free, and they cost $1.50/month — delete
them if you never look.

## The paid scale path (EMR Serverless)

| Item | Price (sourced) | Estimate at our volume |
| --- | --- | --- |
| vCPU-hour | `$0.052624` | 4 vCPU × 5 min/day = 0.33 vCPU-h/day → $0.0176/day |
| GB-hour | `$0.0057785` | 16 GB × 5 min/day = 1.33 GB-h/day → $0.0077/day |
| Pre-initialised driver | 2 vCPU / 8 GB for the job plus the 15-minute idle window | 0.33 h/day → $0.0505/day |
| Shuffle storage | `$0.000111` per GB-hour | negligible for a 5 MB shuffle |
| Per-job-run surcharge | **unverified** (not exposed in the feed) | assumed $0.00 |

**EMR path total: $0.0757/day ≈ $2.27/month**, plus the ~$27.05 above → **~$29.32/month**. The Spark
path is not the expensive part; email and CloudWatch metrics are.

## Cheaper alternative chosen for each paid service

| Paid service | Alternative we chose | Why |
| --- | --- | --- |
| **EMR Serverless** (only enabled with `enable_emr_module = true`) | the Lambda **chunker** for every day under `AGENT_REPORTS_CHUNKER_MAX_POLICIES` (2M policies per shard) | the measured 52k-row day aggregates in seconds on Lambda, which is inside the always-free allowance; EMR is enabled only when a shard would not fit in a Lambda. The chunker and the Spark job produce byte-identical reports for generator-shaped input (`tests/integration/test_spark_job.py`), so this is a config switch, not a migration. |
| **AWS Glue** (considered, not used) | EMR Serverless when Spark is needed at all | Glue is `$0.44` per DPU-hour (Flex: `$0.29`) — [AWSGlue feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSGlue/current/us-east-1/index.json). A 2-DPU, 10-minute job is ~$0.15/day (~$4.40/month) versus ~$0.076/day for EMR Serverless. |
| **Step Functions** (considered, not used) | EventBridge + SQS + Lambda | the pipeline already has a durable queue with redrive and per-message retries; Step Functions would add per-state-transition cost for orchestration the SQS/Lambda pair already provides. Transition pricing **unverified** (the feed returned 404 for the service code used). |
| **NAT Gateway** (avoided entirely) | Lambdas run outside a VPC and reach S3/SQS/SES over their public endpoints with IAM authorisation | a NAT gateway bills hourly plus per GB processed (price **unverified** — the `AmazonVPC` feed does not expose the SKU we need), i.e. tens of dollars a month, for no benefit here. |
| **API Gateway REST API** | HTTP API | HTTP API is `$1.00`/1M requests versus roughly 3.5× that for REST API (REST price not fetched, so **unverified**); the presign route is a single GET with no REST-only features. |
| **S3 requests** | presigned GETs instead of a public bucket or a proxy Lambda | presigned links cost one GET per download and keep the bucket private; a proxy Lambda would add invocation + data-transfer cost per download. |
| **CloudWatch Logs Insights** | metric filters + EMF instead of paying per query | Insights bills `$0.005` per GB scanned; the alarms read metrics, so nothing is scanned on the happy path. |
| **Standard-IA tiering for small objects** | expiry only (no transition) | 128 KB minimum billable per object makes tiering a 1.4 KB report ~91× more storage at ~1.8× lower price — a 50× increase in the storage line. |
| **Data transfer out** | nothing is served from the internet-facing bucket without a signature | only the agents' report downloads leave AWS (~5.6 MB/day); the first 100 GB/month of egress is free (**unverified**), so this is $0.00 here. |

## What is unverified

Everything in this list is stated as unverified on purpose — it was not confirmed against a source
this environment could fetch:

- the **S3/SQS/API Gateway/CloudWatch always-free allowances** for a new account: the free-tier page
  states the plan shape ($100 + up to $100 over 6 months, always-free services) but its per-service
  limits are rendered client-side and returned no figures when fetched;
- the **EMR Serverless per-job-run surcharge** (not in the price feed);
- **Step Functions** state-transition pricing (the offer file 404s);
- **NAT Gateway** hourly and per-GB pricing (SKU not present in the `AmazonVPC` feed);
- **data transfer out** allowances;
- **API Gateway REST API** per-request price (used only as a comparison ratio).

Re-run `python scripts/cost_model.py` after a price change; it prints the same
`usagetype`/description/price triples quoted above, and the arithmetic follows from them.
