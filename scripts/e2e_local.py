#!/usr/bin/env python
"""Offline end-to-end run of the whole pipeline against moto.

    python scripts/e2e_local.py --rows 5000

Generates the synthetic day, aggregates it with the free-tier chunker, fans out over SQS, sends the
emails through SES, then opens the pre-signed link from a delivered email and compares the bytes
with the report object. Prints a report and exits non-zero if any invariant is violated.

Requires no AWS credentials and no network access.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_reports.common.logging_utils import configure_logging, get_logger
from agent_reports.common.settings import Settings, load_settings
from agent_reports.common.storage import open_zones
from agent_reports.pipeline import PipelineOptions, run_local_pipeline
from agent_reports.testing import (
    provision_local_resources,
    sent_messages,
    verify_presigned_delivery,
)

_LOG = get_logger("scripts.e2e_local")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline end-to-end pipeline run (moto).")
    parser.add_argument("--rows", type=int, default=5_000, help="target raw rows to generate")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--report-date", default="2026-09-20")
    parser.add_argument("--shards", type=int, default=2, help="parallel chunker shards")
    parser.add_argument("--json", action="store_true", help="print the raw result as JSON")
    parser.add_argument("--quiet", action="store_true", help="suppress the JSON log lines")
    return parser


def settings_for(region: str) -> Settings:
    """Bucket/queue names for the offline run (no real AWS resources are touched)."""
    return load_settings(
        {
            "AGENT_REPORTS_REGION": region,
            "AGENT_REPORTS_RAW_BUCKET": "agent-reports-raw",
            "AGENT_REPORTS_PROCESSED_BUCKET": "agent-reports-processed",
            "AGENT_REPORTS_REPORTS_BUCKET": "agent-reports-out",
            "AGENT_REPORTS_AGENT_QUEUE_URL": (
                f"https://sqs.{region}.amazonaws.com/000000000000/agent-reports-fanout"
            ),
            "AGENT_REPORTS_DLQ_URL": (
                f"https://sqs.{region}.amazonaws.com/000000000000/agent-reports-fanout-dlq"
            ),
            "AGENT_REPORTS_SES_SENDER": "reports@example.com",
            "AGENT_REPORTS_SES_CONFIGURATION_SET": "agent-reports",
            "AGENT_REPORTS_PRESIGN_TTL_SECONDS": "900",
            "AGENT_REPORTS_LOG_LEVEL": "WARNING",
        }
    )


def _fail(problems: list[str]) -> int:
    print("\nE2E FAILED:")
    for problem in problems:
        print(f"  - {problem}")
    return 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.quiet:
        # Keep the run's own log lines off stdout but still let the telemetry capture see them.
        # The handler holds this stream for the rest of the process, so it is not a context manager.
        sink = Path(os.devnull).open("w", encoding="utf-8")  # noqa: SIM115
        configure_logging(stream=sink, force=True)
    else:
        configure_logging("WARNING")

    settings = settings_for("us-east-1")
    options = PipelineOptions(
        report_date=args.report_date,
        rows=args.rows,
        seed=args.seed,
        shards=max(1, args.shards),
    )

    from moto import mock_aws

    started = time.perf_counter()
    with mock_aws():
        provision_local_resources(settings)
        result = run_local_pipeline(settings, options)
        zones = open_zones(settings)
        delivery = verify_presigned_delivery(
            settings,
            region=settings.region,
            recipient=f"{result.sample_agent_id.lower()}@example.com",
            agent_id=result.sample_agent_id,
            report_date=args.report_date,
            zones=zones,
        )
        captured = len(sent_messages(settings.region))
        cloudwatch_metrics = sorted(
            {
                metric["MetricName"]
                for metric in boto3.client("cloudwatch", region_name=settings.region)
                .list_metrics(Namespace="AgentReports")
                .get("Metrics", [])
            }
        )
    wall_seconds = time.perf_counter() - started

    payload = result.as_dict()
    if args.json:
        print(
            json.dumps(
                {**payload, "delivery": delivery, "wall_seconds": round(wall_seconds, 3)}, indent=2
            )
        )

    problems: list[str] = []
    if result.rows_in < args.rows:
        problems.append(f"rows in ({result.rows_in}) below the requested {args.rows}")
    if result.reports_written != result.agents_reported:
        problems.append("reports written does not match agents reported")
    if result.agents_reported != result.emails_sent:
        problems.append(
            f"emails sent ({result.emails_sent}) != agents reported ({result.agents_reported})"
        )
    if result.duplicates != 0 or result.failed != 0 or result.quarantined != 0:
        problems.append(
            f"unexpected duplicates/failures: dup={result.duplicates} "
            f"failed={result.failed} quarantined={result.quarantined}"
        )
    if not result.queue_drained:
        problems.append("SQS queue did not drain")
    if captured != result.emails_sent:
        problems.append(
            f"SES captured {captured} messages but the dispatcher reported {result.emails_sent}"
        )
    if result.metrics_emitted == 0:
        problems.append("no CloudWatch EMF metrics were emitted")
    if delivery["http_status"] != 200 or not delivery["matches_report_object"]:
        problems.append("pre-signed link did not return the report object")

    print("=== Agent Report Distribution - offline end-to-end (moto) ===")
    print(f"report_date         : {result.report_date}")
    print(
        f"rows in             : {result.rows_in:,} "
        f"(agents {result.dataset.get('agents', 0):,} / policies {result.dataset.get('policies', 0):,} "
        f"/ claims {result.dataset.get('claims', 0):,})"
    )
    print(f"agents discovered   : {result.agents_discovered:,}")
    print(f"agents processed    : {result.agents_reported:,}")
    print(f"reports written     : {result.reports_written:,}")
    print(f"emails sent         : {result.emails_sent:,} (SES captured {captured:,})")
    print(f"duplicates / failed : {result.duplicates} / {result.failed}")
    print(f"queue drained       : {'yes' if result.queue_drained else 'NO'}")
    print(f"dlq depth           : {result.dlq_messages}")
    print(f"objects written     : {result.objects_written:,}")
    print(
        f"metrics emitted     : {result.metrics_emitted} EMF documents "
        f"({', '.join(result.metric_names)})"
    )
    print(
        f"cloudwatch metrics  : {len(cloudwatch_metrics)} published "
        f"({', '.join(cloudwatch_metrics)})"
    )
    print(
        f"pre-signed link     : HTTP {delivery['http_status']}, {delivery['bytes']:,} bytes, "
        f"matches report object: {delivery['matches_report_object']}"
    )
    print(f"sample report       : {result.sample_report_key}")
    print(f"manifest            : {result.manifest_key}")
    print(
        "stage timings (s)   : "
        + ", ".join(f"{name}={value:.3f}" for name, value in result.stages.items())
    )
    print(f"duration            : {result.duration_seconds:.3f} s (wall {wall_seconds:.3f} s)")

    if problems:
        return _fail(problems)
    print("\nE2E OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
