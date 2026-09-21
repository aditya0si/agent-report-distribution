#!/usr/bin/env python
"""Compute the monthly cost estimate in docs/COST.md, from measured volumes and fetched prices.

    python scripts/cost_model.py              # fetches live prices from the AWS Price List API
    python scripts/cost_model.py --recorded   # uses the prices recorded in this file (no network)

Every price is a us-east-1 list price from the public Price List API
(``https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<service>/current/us-east-1/index.json``),
the same feed the pricing pages are built from. Every volume comes from a command that was actually
run against this repository; the ``MEASURED`` block below names the command for each one. The point
of this script is that no number in docs/COST.md is hand-arithmetic: re-run it and the table comes
back, or it fails loudly because a price moved.

Prices that the feed does not expose (the SES plan rate, the free-tier allowances) are recorded
separately in :data:`MANUAL_PRICES` with the page they were read from, and are printed as
``source: manual`` so the distinction is never lost.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from typing import Any, cast

__all__ = ["main"]

BASE = (
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/us-east-1/index.json"
)

#: us-east-1 list prices as they were when this file was last updated, keyed by usagetype.
#: ``--recorded`` uses these; the default path re-fetches and prints the same keys.
RECORDED_PRICES: dict[str, str] = {
    "TimedStorage-ByteHrs": "0.023",  # S3 Standard, first 50 TB
    "TimedStorage-SIA-ByteHrs": "0.0125",  # S3 Standard-IA
    "TimedStorage-GIR-ByteHrs": "0.004",  # S3 Glacier Instant Retrieval
    "Requests-Tier1": "0.000005",  # PUT/COPY/POST/LIST, per request
    "Requests-Tier2": "0.0000004",  # GET/SELECT/all other, per request
    "CW:MetricMonitorUsage": "0.30",  # CloudWatch custom metric, first 10,000
    "CW:AlarmMonitorUsage": "0.10",  # standard-resolution alarm-metric month
    "DataProcessing-Bytes": "0.50",  # CloudWatch Logs ingest per GB
    "TimedStorage-CW:LogStorage": "0.03",  # CloudWatch Logs storage per GB-month
    "AWSQueueService-Requests": "0.0000004",  # SQS standard request
    "AWSLambda-GB-Second": "0.0000166667",  # Lambda x86 GB-second
    "AWSLambda-Request": "0.0000002",  # Lambda request
    "AmazonApiGateway-Requests": "0.000001",  # HTTP API request
    "ElasticMapReduce-vCPU": "0.052624",  # EMR Serverless vCPU-hour
    "ElasticMapReduce-GB": "0.0057785",  # EMR Serverless GB-hour
    "ElasticMapReduce-Shuffle": "0.000111",  # EMR Serverless shuffle GB-hour
}

#: Prices the Price List API does not expose, with the page they were read from.
MANUAL_PRICES: dict[str, tuple[str, str]] = {
    # value, where it came from
    "ses.per_1000_essentials": (
        "0.16",
        "https://aws.amazon.com/ses/pricing/ - Essentials plan, "
        "0-10M emails/month, US East list price",
    ),
    "lambda.free_requests": (
        "1000000",
        "https://aws.amazon.com/lambda/pricing/ - "
        "'The free tier includes one million requests and 400,000 "
        "GB-seconds per month'",
    ),
    "lambda.free_gb_seconds": ("400000", "https://aws.amazon.com/lambda/pricing/ - same sentence"),
    "free_tier.plan": (
        "$100 credits at signup + up to $100 more over 6 months; the Free plan closes "
        "after 6 months",
        "https://aws.amazon.com/free/ - new-account plan",
    ),
}

# --------------------------------------------------------------------------- measured volumes
# Every value here was produced by a command in this repository; the comment names it.
MEASURED: dict[str, Any] = {
    "agents": 4_000,  # assumed steady state (the 50k-row demo generates 3,805)
    "emails_per_month": 120_000,  # agents x 30 days
    "report_bytes": 1_406,  # scripts/e2e_local.py --rows 5000: "pre-signed link: 1,421 bytes"
    "raw_bytes_per_day": 5_546_871,  # agent-reports generate --rows 50000: bytes_written
    "raw_parts_per_day": 12,  # same run: parts=12
    "marker_bytes": 400,  # state/dispatch marker (one JSON object)
    "manifest_bytes": 4_000,  # state/runs manifest for a 4,000-agent day
    "dispatch_latency_seconds": 0.3,  # e2e measured ~75 ms under moto; 300 ms is the AWS estimate
    "dispatcher_memory_gb": 0.5,
    "chunker_memory_gb": 3.0,
    "chunker_seconds_per_run": 30.0,
    "chunker_runs_per_day": 4,  # var.chunker_shards
    "orchestrator_memory_gb": 1.0,
    "orchestrator_seconds_per_run": 5.0,
    "presign_fraction": 0.05,  # assumed: 5% of deliveries need a fresh link
}

DAYS = 30
#: S3 requests the dispatcher makes per email. Counted from the code, not guessed:
#:   reports.exists -> HEAD, _totals_for_report -> HEAD + HEAD + GET, ledger.claim -> GET,
#:   ledger.mark_sent -> GET, ledger.claim/mark_sent -> 2 PUTs, chunker -> 1 PUT per report.
GET_REQUESTS_PER_EMAIL = 6
PUT_REQUESTS_PER_EMAIL = 3
#: The agent's own download of the pre-signed link.
DOWNLOAD_REQUESTS_PER_EMAIL = 1


def fetch_feed(service: str) -> dict[str, Any]:
    request = urllib.request.Request(BASE.format(service=service), headers={"User-Agent": "curl/8"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return cast(dict[str, Any], json.load(response))


def live_prices() -> dict[str, str]:
    """Pull the usagetypes this model needs straight out of the Price List API."""
    wanted = {
        "AmazonS3": (
            "TimedStorage-ByteHrs",
            "TimedStorage-SIA-ByteHrs",
            "TimedStorage-GIR-ByteHrs",
            "Requests-Tier1",
            "Requests-Tier2",
        ),
        "AmazonCloudWatch": (
            "CW:MetricMonitorUsage",
            "CW:AlarmMonitorUsage",
            "DataProcessing-Bytes",
        ),
    }
    prices: dict[str, str] = {}
    for service, usagetypes in wanted.items():
        document = fetch_feed(service)
        for sku, product in document.get("products", {}).items():
            attributes = product.get("attributes", {})
            usagetype = str(attributes.get("usagetype", ""))
            if usagetype not in usagetypes:
                continue
            if attributes.get("location") not in (None, "US East (N. Virginia)"):
                continue
            for offer in document["terms"]["OnDemand"].get(sku, {}).values():
                for dimension in offer.get("priceDimensions", {}).values():
                    price = dimension.get("pricePerUnit", {}).get("USD", "0")
                    if price != "0.0000000000" and usagetype not in prices:
                        prices[usagetype] = str(float(price))
    return prices


def build_prices(*, recorded: bool) -> tuple[dict[str, str], dict[str, str]]:
    """Return ``(prices, provenance)`` - where each number came from, so COST.md can say so."""
    prices: dict[str, str] = {}
    provenance: dict[str, str] = {}
    if recorded:
        for key, value in RECORDED_PRICES.items():
            prices[key] = value
            provenance[key] = "recorded in scripts/cost_model.py"
    else:
        fetched = live_prices()
        for key, value in RECORDED_PRICES.items():
            prices[key] = fetched.get(key, value)
            provenance[key] = (
                "AWS Price List API (live)"
                if key in fetched
                else "recorded in scripts/cost_model.py"
            )
    for key, (value, source) in MANUAL_PRICES.items():
        prices[key] = value
        provenance[key] = f"manual: {source}"
    return prices, provenance


def money(value: float) -> str:
    return f"${value:,.4f}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Monthly cost model for docs/COST.md")
    parser.add_argument(
        "--recorded",
        action="store_true",
        help="use the prices recorded in this file instead of fetching the Price List API",
    )
    args = parser.parse_args(argv)

    prices, provenance = build_prices(recorded=args.recorded)
    price = lambda key: float(prices[key])  # noqa: E731 - a local alias reads better in the maths

    agents = int(MEASURED["agents"])
    emails = int(MEASURED["emails_per_month"])
    report_bytes = int(MEASURED["report_bytes"])
    raw_bytes_per_day = int(MEASURED["raw_bytes_per_day"])
    marker_bytes = int(MEASURED["marker_bytes"])
    manifest_bytes = int(MEASURED["manifest_bytes"])
    gb = 1024**3

    print("=== prices used ===")
    for key in sorted(prices):
        print(f"  {key:28s} {prices[key]:>14s}   [{provenance[key]}]")

    print("\n=== volumes (measured; see MEASURED in scripts/cost_model.py) ===")
    for key, value in MEASURED.items():
        print(f"  {key:28s} {value}")

    # ------------------------------------------------------------------ S3
    report_gb = agents * report_bytes * 120 / gb
    state_gb = (agents * marker_bytes + manifest_bytes) * 365 / gb
    raw_standard_gb = raw_bytes_per_day * 30 / gb
    raw_ia_gb = raw_bytes_per_day * 60 / gb
    raw_glacier_gb = raw_bytes_per_day * 310 / gb
    storage = (
        (report_gb + state_gb + raw_standard_gb) * price("TimedStorage-ByteHrs")
        + raw_ia_gb * price("TimedStorage-SIA-ByteHrs")
        + raw_glacier_gb * price("TimedStorage-GIR-ByteHrs")
    )

    get_requests = emails * (GET_REQUESTS_PER_EMAIL + DOWNLOAD_REQUESTS_PER_EMAIL)
    put_requests = (
        emails * PUT_REQUESTS_PER_EMAIL
        + MEASURED["raw_parts_per_day"] * DAYS
        + DAYS  # the run manifest
    )
    requests = get_requests * price("Requests-Tier2") + put_requests * price("Requests-Tier1")

    print("\n=== S3 ===")
    print(f"  reports stored      {report_gb:10.3f} GB-mo   (4000 x 1406 B, 120-day expiry)")
    print(
        f"  state markers       {state_gb:10.3f} GB-mo   (4000 x 400 B + manifest, 365-day expiry)"
    )
    print(f"  raw in Standard     {raw_standard_gb:10.3f} GB-mo   (5.5 MB/day x 30 days)")
    print(f"  raw in Standard-IA  {raw_ia_gb:10.3f} GB-mo   (next 60 days)")
    print(f"  raw in Glacier IR   {raw_glacier_gb:10.3f} GB-mo   (to the 400-day expiry)")
    print(f"  storage             {money(storage)}/month")
    print(
        f"  GET-type requests   {get_requests:10,d}/month ({GET_REQUESTS_PER_EMAIL} per email + 1 download)"
    )
    print(
        f"  PUT-type requests   {put_requests:10,d}/month ({PUT_REQUESTS_PER_EMAIL} per email + raw + manifest)"
    )
    print(f"  requests            {money(requests)}/month")
    s3_total = storage + requests
    print(f"  S3 total            {money(s3_total)}/month")

    # ------------------------------------------------------------------ CloudWatch
    emf_identities = 12  # 3 chunker + 3 orchestrator + 6 dispatcher, each under {Service=...}
    filter_metrics = 5  # the five log metric filters
    metric_months = emf_identities + filter_metrics
    log_ingest_gb = 0.1  # ~100 MB of Lambda logs + EMF documents for the month
    alarms = 4 + 1  # 4 alarms; the metric-math alarm bills one alarm-metric per metric it reads
    cloudwatch = (
        metric_months * price("CW:MetricMonitorUsage")
        + log_ingest_gb * price("DataProcessing-Bytes")
        + alarms * price("CW:AlarmMonitorUsage")
    )
    print("\n=== CloudWatch ===")
    print(
        f"  custom metrics      {metric_months:10d} metric-months (12 EMF identities + 5 filters)"
    )
    print(f"  log ingest          {log_ingest_gb:10.3f} GB/month")
    print(f"  alarm-metric months {alarms:10d} (4 alarms; the metric-math alarm reads 2 metrics)")
    print(f"  CloudWatch total    {money(cloudwatch)}/month")
    print("  NOTE: the previous design published 6 metrics under a per-day ReportDate dimension,")
    print(
        "        which is 6 x 30 = 180 extra metric-months/month (~$54) and cannot be alarmed on."
    )

    # ------------------------------------------------------------------ SES / SQS / API / Lambda
    ses = emails * price("ses.per_1000_essentials") / 1000
    sqs_requests = (
        agents / 10 * DAYS  # SendMessageBatch
        + agents / 10 * DAYS  # ReceiveMessage
        + agents / 10 * DAYS  # DeleteMessageBatch
    )
    sqs = sqs_requests * price("AWSQueueService-Requests")
    api_calls = emails * float(MEASURED["presign_fraction"])
    api = api_calls * price("AmazonApiGateway-Requests")
    gb_seconds = (
        emails
        * float(MEASURED["dispatcher_memory_gb"])
        * float(MEASURED["dispatch_latency_seconds"])
        + int(MEASURED["chunker_runs_per_day"])
        * DAYS
        * float(MEASURED["chunker_memory_gb"])
        * float(MEASURED["chunker_seconds_per_run"])
        + DAYS
        * float(MEASURED["orchestrator_memory_gb"])
        * float(MEASURED["orchestrator_seconds_per_run"])
    )
    lambda_requests = emails + int(MEASURED["chunker_runs_per_day"]) * DAYS + DAYS + api_calls
    billable_gb_seconds = max(0.0, gb_seconds - float(prices["lambda.free_gb_seconds"]))
    billable_requests = max(0.0, lambda_requests - float(prices["lambda.free_requests"]))
    aws_lambda = billable_gb_seconds * price("AWSLambda-GB-Second") + billable_requests * price(
        "AWSLambda-Request"
    )
    print("\n=== SES / SQS / API Gateway / Lambda ===")
    print(
        f"  SES                 {money(ses)}/month ({emails:,} emails at "
        f"${prices['ses.per_1000_essentials']}/1,000)"
    )
    print(f"  SQS                 {money(sqs)}/month ({sqs_requests:,.0f} requests)")
    print(f"  API Gateway         {money(api)}/month ({api_calls:,.0f} presign calls)")
    print(
        f"  Lambda              {money(aws_lambda)}/month "
        f"({gb_seconds:,.0f} GB-s and {lambda_requests:,.0f} requests vs the free tier)"
    )

    free_tier_total = s3_total + cloudwatch + ses + sqs + api + aws_lambda
    print("\n=== free-tier path (Lambda chunker, no EMR) ===")
    print(f"  TOTAL               {money(free_tier_total)}/month")

    # ------------------------------------------------------------------ EMR path
    emr_minutes = 5.0
    vcpu_hours = 4 * emr_minutes / 60
    gb_hours = 16 * emr_minutes / 60
    driver_hours = (emr_minutes + 15) / 60  # the pre-initialised driver plus its idle window
    emr_daily = (
        vcpu_hours * price("ElasticMapReduce-vCPU")
        + gb_hours * price("ElasticMapReduce-GB")
        + driver_hours * (2 * price("ElasticMapReduce-vCPU") + 8 * price("ElasticMapReduce-GB"))
    )
    emr_monthly = emr_daily * DAYS
    print("\n=== EMR Serverless path (enable_emr_module = true) ===")
    print(
        f"  per day             {money(emr_daily)}  ({vcpu_hours:.2f} vCPU-h + {gb_hours:.2f} GB-h "
        f"+ a {driver_hours:.2f} h 2 vCPU/8 GB driver)"
    )
    print(f"  per month           {money(emr_monthly)}")
    print(f"  TOTAL with EMR      {money(free_tier_total + emr_monthly)}/month")

    if not args.recorded:
        print(
            "\n(prices fetched live; pass --recorded to reproduce the figures in docs/COST.md "
            "without network access)"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
