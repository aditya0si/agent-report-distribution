# Cost: what this pipeline costs, what the free tier actually covers, and what was avoided

Checked on **2026-09-21**. Prices are US East (N. Virginia) list prices in USD.

**How these numbers were obtained.** Instead of copying figures from a pricing page, the prices below
were pulled from the public AWS Price List API with `scripts/fetch_aws_prices.py`
(`python scripts/fetch_aws_prices.py`), which reads
`https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<ServiceCode>/current/us-east-1/index.json`
— the same feed the pricing pages are built from. Each row in the tables links the exact feed it came
from. Anything that could **not** be sourced this way (the free-tier allowances and a few services
whose feed does not expose the SKU we need) is marked **unverified** rather than guessed.

## Volume this estimate is for

| Quantity | Value | Source |
| --- | --- | --- |
| Agents | 4,000 | assumed (the demo default generates 3,805 for a 50k-row day) |
| Policies per day | ~32,000 | 8 policies per agent |
| Raw rows per day | ~52,000 | measured: `agent-reports generate --rows 50000` → 52,471 rows |
| Raw bytes per day | ~5.5 MB | measured: 5,546,871 bytes for that run |
| Report objects per day | 4,000 (~1.4 KB each) | measured: e2e report size 1,406 bytes |
| Emails per month | 120,000 | 4,000 agents × 30 days |
| Presign API calls per month | ~20,000 | assumed: 5% of deliveries need a fresh link |

## Services used, free tier, and real prices

| Service | What we use it for | Free tier | List price (sourced) | Cost at our volume |
| --- | --- | --- | --- | --- |
| **Lambda** | chunker, orchestrator, dispatcher, presign | 1M requests + 400,000 GB-s per month (**unverified** — `aws.amazon.com/lambda/pricing` is JS-rendered and could not be fetched from this environment) | `$0.0000166667` per GB-s (x86, tier 1) and `$0.20` per 1M requests — [AWSLambda feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSLambda/current/us-east-1/index.json) | ~12,150 invocations + ~10,000 GB-s per month → **$0.00** (inside the free tier) |
| **S3** | raw / processed / reports zones | 5 GB storage + 20,000 GET + 2,000 PUT per month for 12 months (**unverified**, same reason) | `$0.023`/GB-mo Standard, `$0.0125` IA, `$0.004` Glacier IR; `$0.005` per 1,000 PUT; `$0.0004` per 1,000 GET — [AmazonS3 feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json) | ~330 MB stored → $0.01; ~243k PUT → $1.22; ~240k GET → $0.10 → **~$1.33** |
| **SQS** | fan-out queue + DLQ | 1M requests per month (**unverified**) | `$0.40` per 1M standard requests — [AWSQueueService feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSQueueService/current/us-east-1/index.json) | ~36,000 requests → **$0.01** |
| **SES** | report delivery | 3,000 message charges per month for the first 12 months, and the sandbox limits recipients (**unverified**) | `$0.0001` per recipient (`SendEmail`) — [AmazonSES feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonSES/current/us-east-1/index.json) | 120,000 emails → **$12.00** (this is the bill) |
| **CloudWatch** | EMF metrics, logs, alarms, dashboard | 10 custom metrics, 5 GB log ingest, 3 dashboards (**unverified**) | `$0.30`/metric-month (first 10k), `$0.50`/GB ingested, `$0.03`/GB-mo stored — [AmazonCloudWatch feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonCloudWatch/current/us-east-1/index.json) | ~10 metrics → $3.00; ~100 MB logs → $0.05 → **~$3.05** (alarm pricing **unverified**) |
| **API Gateway (HTTP API)** | `GET /reports` for re-issued links | 1M requests/month for 12 months (**unverified**) | `$1.00` per 1M requests — [AmazonApiGateway feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonApiGateway/current/us-east-1/index.json) | 20,000 calls → **$0.02** |
| **EventBridge** | the two daily schedules | all state-change events free; scheduled rules free (**unverified**) | not sourced (scheduled rules are not billed per invocation) | **$0.00** |

### Free-tier path total (no EMR)

| Line | Monthly |
| --- | --- |
| S3 | $1.33 |
| SES | $12.00 |
| CloudWatch | $3.05 |
| SQS | $0.01 |
| API Gateway | $0.02 |
| Lambda, EventBridge | $0.00 |
| **Total** | **~$16.41/month** |

Roughly three quarters of that is SES, and it scales linearly with agents: SES is the only line worth
optimising. Two levers: (a) send one email per agent per *week* instead of per day where the contract
allows it (saves ~$10/month here), or (b) switch to a bulk/templated send (`SendBulkTemplatedEmail`)
— same per-recipient price, but fewer API calls, so it does not change the bill.

## The paid scale path (EMR Serverless)

| Item | Price (sourced) | Estimate at our volume |
| --- | --- | --- |
| vCPU-hour | `$0.052624` — [ElasticMapReduce feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/ElasticMapReduce/current/us-east-1/index.json) | 4 vCPU × ~5 min/day = 0.33 vCPU-h/day → $0.0176/day |
| GB-hour | `$0.0057785` (same feed) | 16 GB × ~5 min/day = 1.33 GB-h/day → $0.0077/day |
| Shuffle storage | `$0.000111` per GB-hour (same feed) | negligible for a 5 MB shuffle |
| Pre-initialised driver | 2 vCPU / 8 GB, running for the job plus the 15-minute idle window | ~0.33 h/day → $0.050/day |
| Per-job-run surcharge | **unverified** (not exposed in the feed) | assumed $0.00 |

**EMR path total: ~$0.075/day ≈ $2.26/month**, plus the ~$16.41 above → **~$18.67/month**. The Spark
path is not the expensive part; email is.

## Cheaper alternative chosen for each paid service

| Paid service | Alternative we chose | Why |
| --- | --- | --- |
| **EMR Serverless** (only enabled with `enable_emr_module = true`) | the Lambda **chunker** for every day under `AGENT_REPORTS_CHUNKER_MAX_POLICIES` (2M policies per shard) | the measured 52k-row day aggregates in seconds on Lambda, which is inside the free tier; EMR is enabled only when a shard would not fit in a Lambda. The chunker and the Spark job produce **byte-identical** reports (`tests/integration/test_spark_job.py`), so this is a config switch, not a migration. |
| **AWS Glue** (considered, not used) | EMR Serverless when Spark is needed at all | Glue is `$0.44` per DPU-hour (Flex: `$0.29`) — [AWSGlue feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSGlue/current/us-east-1/index.json). A 2-DPU, 10-minute job is ~$0.15/day (~$4.40/month) versus ~$0.075/day for EMR Serverless, so Glue is the more expensive option for this job shape. |
| **Step Functions** (considered, not used) | EventBridge + SQS + Lambda | the pipeline already has a durable queue with redrive and per-message retries; Step Functions would add per-state-transition cost for orchestration the SQS/Lambda pair already provides. Transition pricing **unverified** (the feed returned 404 for the service code used). |
| **NAT Gateway** (avoided entirely) | Lambdas run outside a VPC and reach S3/SQS/SES over their public endpoints with IAM authorisation | a NAT gateway bills hourly plus per GB processed (price **unverified** — the `AmazonVPC` feed does not expose the SKU we need), i.e. tens of dollars a month, for no benefit here. Nothing in this pipeline needs private networking. |
| **API Gateway REST API** | HTTP API | HTTP API is `$1.00`/1M requests versus roughly 3.5× that for REST API (REST price not fetched, so **unverified**); the presign route is a single GET with no REST-only features. |
| **S3 requests** | presigned GETs instead of a public bucket or a proxy Lambda | presigned links cost one GET per download and keep the bucket private; a proxy Lambda would add invocation + data-transfer cost per download. |
| **CloudWatch Logs Insights** | metric filters + EMF instead of paying per query | Insights bills `$0.005` per GB scanned ([AmazonCloudWatch feed](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonCloudWatch/current/us-east-1/index.json)); the alarms read metrics, so nothing is scanned on the happy path. |
| **Data transfer out** | nothing is served from the internet-facing bucket without a signature | only the agents' report downloads leave AWS (~5.6 MB/day); the first 100 GB/month of egress is free (**unverified**), so this is $0.00 here. |

## What is unverified

Everything in this list is stated as unverified on purpose — it was not confirmed against a source
this environment could fetch:

- every **free-tier allowance** (Lambda, S3, SQS, SES, CloudWatch, API Gateway, EventBridge): the
  `aws.amazon.com/*/pricing` pages are rendered client-side and returned no price data when fetched;
- the **EMR Serverless per-job-run surcharge** (not in the price feed);
- **CloudWatch alarm** and **dashboard** pricing;
- **Step Functions** state-transition pricing (the offer file 404s);
- **NAT Gateway** hourly and per-GB pricing (SKU not present in the `AmazonVPC` feed);
- **data transfer out** allowances;
- **API Gateway REST API** per-request price (used only as a comparison ratio).

Re-run `python scripts/fetch_aws_prices.py` after a price change; the script prints the same
`usagetype`/description/price triples quoted above.
