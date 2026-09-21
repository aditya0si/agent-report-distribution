"""The pipeline as one function: generate -> aggregate -> fan out -> dispatch.

``scripts/e2e_local.py`` and the integration tests call this so there is exactly one definition of
"the pipeline", and both report the same numbers. Everything it touches goes through boto3, so it
runs unchanged against moto (offline, free), LocalStack, or real AWS - only the endpoint differs.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .common.aws import ses_client, sqs_client
from .common.keys import parse_report_key, report_key, validate_report_date
from .common.logging_utils import configure_logging, get_logger
from .common.settings import Settings
from .common.storage import Zones, open_zones
from .ingest.generator import DatasetConfig, DatasetStats, generate_dataset
from .lambda_handlers.chunker import ChunkerResult, run_chunker
from .lambda_handlers.dispatcher import run_dispatcher
from .lambda_handlers.orchestrator import OrchestratorResult, run_orchestrator

__all__ = [
    "PipelineOptions",
    "PipelineResult",
    "capture_telemetry",
    "extract_download_url",
    "queue_depth",
    "receive_records",
    "run_local_pipeline",
]

_LOG = get_logger(__name__)


@dataclass
class PipelineOptions:
    """Knobs for a local run."""

    report_date: str
    rows: int = 5_000
    seed: int = 7
    partitions: int = 4
    shards: int = 1
    agents: int | None = None
    max_dispatch_batches: int = 0
    visibility_timeout: int = 0
    dlq_wait_receives: int = 0

    def dispatch_batch_budget(self, messages_enqueued: int, batch_size: int) -> int:
        """How many receive/dispatch rounds to allow.

        ``max_dispatch_batches`` is a runaway guard, not a work limit: if it were fixed, a large day
        would silently stop part-way through and leave the queue half-full (which is exactly what
        happened the first time this was run at 50k rows). So the budget is derived from the fan-out
        size, with headroom for redeliveries, unless the caller sets an explicit cap.
        """
        if self.max_dispatch_batches:
            return self.max_dispatch_batches
        per_batch = max(1, batch_size)
        return (messages_enqueued // per_batch) * 3 + 50

    def dataset_config(self) -> DatasetConfig:
        if self.agents is not None:
            return DatasetConfig(
                report_date=self.report_date,
                agents=self.agents,
                seed=self.seed,
                partitions=self.partitions,
            )
        return DatasetConfig.for_total_rows(
            self.rows, self.report_date, seed=self.seed, partitions=self.partitions
        )


@dataclass
class PipelineResult:
    """Everything the e2e report and the integration tests assert on."""

    report_date: str
    rows_in: int = 0
    agents_discovered: int = 0
    agents_reported: int = 0
    reports_written: int = 0
    messages_enqueued: int = 0
    emails_sent: int = 0
    duplicates: int = 0
    failed: int = 0
    quarantined: int = 0
    dlq_messages: int = 0
    objects_written: int = 0
    metrics_emitted: int = 0
    metric_names: list[str] = field(default_factory=list)
    metric_identities: list[str] = field(default_factory=list)
    log_events: dict[str, int] = field(default_factory=dict)
    queue_drained: bool = False
    duration_seconds: float = 0.0
    stages: dict[str, float] = field(default_factory=dict)
    dataset: dict[str, Any] = field(default_factory=dict)
    sample_agent_id: str = ""
    sample_report_key: str = ""
    sample_ses_message_id: str = ""
    manifest_key: str = ""
    failed_agents: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "report_date": self.report_date,
            "rows_in": self.rows_in,
            "agents_discovered": self.agents_discovered,
            "agents_reported": self.agents_reported,
            "reports_written": self.reports_written,
            "messages_enqueued": self.messages_enqueued,
            "emails_sent": self.emails_sent,
            "duplicates": self.duplicates,
            "failed": self.failed,
            "quarantined": self.quarantined,
            "dlq_messages": self.dlq_messages,
            "objects_written": self.objects_written,
            "metrics_emitted": self.metrics_emitted,
            "metric_names": self.metric_names,
            "metric_identities": self.metric_identities,
            "log_events": self.log_events,
            "queue_drained": self.queue_drained,
            "duration_seconds": round(self.duration_seconds, 3),
            "stages": {name: round(value, 3) for name, value in self.stages.items()},
            "dataset": self.dataset,
            "sample_agent_id": self.sample_agent_id,
            "sample_report_key": self.sample_report_key,
            "sample_ses_message_id": self.sample_ses_message_id,
            "manifest_key": self.manifest_key,
            "failed_agents": self.failed_agents,
        }


# --------------------------------------------------------------------------- helpers
def receive_records(
    sqs: Any,
    queue_url: str,
    *,
    max_messages: int = 10,
    visibility_timeout: int = 0,
    wait_seconds: int = 0,
) -> list[dict[str, Any]]:
    """Pull messages and shape them exactly like an SQS->Lambda event source mapping would."""
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max(1, min(max_messages, 10)),
        VisibilityTimeout=visibility_timeout,
        WaitTimeSeconds=wait_seconds,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
    )
    records: list[dict[str, Any]] = []
    for message in response.get("Messages", []):
        records.append(
            {
                "messageId": str(message.get("MessageId", "")),
                "receiptHandle": str(message.get("ReceiptHandle", "")),
                "body": str(message.get("Body", "")),
                "attributes": dict(message.get("Attributes", {})),
                "messageAttributes": _event_message_attributes(
                    message.get("MessageAttributes", {})
                ),
                "eventSource": "aws:sqs",
                "awsRegion": str(message.get("Attributes", {}).get("SenderId", "")),
            }
        )
    return records


def _event_message_attributes(raw: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Convert boto3's PascalCase message attributes to the event source mapping's camelCase shape.

    The Lambda SQS event uses ``{"stringValue": ..., "dataType": ...}`` while the SQS API returns
    ``{"StringValue": ..., "DataType": ...}``. Skipping this conversion is a real bug, not a cosmetic
    one: the dispatcher reads ``stringValue`` when it has to quarantine an unparseable message.
    """
    converted: dict[str, dict[str, Any]] = {}
    for name, value in raw.items():
        if not isinstance(value, Mapping):
            continue
        entry: dict[str, Any] = {"dataType": str(value.get("DataType", "String"))}
        if "StringValue" in value:
            entry["stringValue"] = str(value["StringValue"])
        if "BinaryValue" in value:
            entry["binaryValue"] = value["BinaryValue"]
        if "StringListValues" in value:
            entry["stringListValues"] = [str(item) for item in value["StringListValues"]]
        if "BinaryListValues" in value:
            entry["binaryListValues"] = list(value["BinaryListValues"])
        converted[str(name)] = entry
    return converted


def queue_depth(sqs: Any, queue_url: str) -> dict[str, int]:
    """Visible / in-flight message counts for a queue."""
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"]
    return {
        "visible": int(attributes.get("ApproximateNumberOfMessages", 0)),
        "in_flight": int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attributes.get("ApproximateNumberOfMessagesDelayed", 0)),
    }


def extract_download_url(text: str) -> str:
    """Pull the pre-signed link out of a rendered email body (plain or HTML)."""
    import re

    match = re.search(r"https?://[^\s\"'<>]+", text)
    if match is None:
        raise ValueError("no download URL found in the email body")
    return match.group(0).replace("&amp;", "&")


class _TelemetryCapture(logging.Handler):
    """Collects the EMF documents and event names this package logs during a run."""

    def __init__(self) -> None:
        super().__init__()
        self.metric_documents: list[dict[str, Any]] = []
        self.metric_names: list[str] = []
        self.metric_identities: set[str] = set()
        self.events: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, "emf", False):
            try:
                document = json.loads(record.getMessage())
            except ValueError:  # pragma: no cover - malformed EMF is a bug worth seeing
                return
            self.metric_documents.append(document)
            for entry in document.get("_aws", {}).get("CloudWatchMetrics", []):
                namespace = str(entry.get("Namespace", ""))
                names = [str(metric.get("Name", "")) for metric in entry.get("Metrics", [])]
                for name in names:
                    if name and name not in self.metric_names:
                        self.metric_names.append(name)
                for dimension_names in entry.get("Dimensions", []):
                    label = ",".join(
                        f"{dimension}={document.get(dimension, '')}"
                        for dimension in sorted(dimension_names)
                    )
                    for name in names:
                        if name:
                            self.metric_identities.add(f"{namespace}/{name}{{{label}}}")
            return
        event = record.__dict__.get("event")
        if isinstance(event, str):
            self.events.append(event)


class capture_telemetry:
    """Context manager: capture EMF metrics + structured events emitted by this package.

    The package logger level is temporarily lowered to ``DEBUG`` so telemetry is captured even when
    the run itself is configured quiet - otherwise an ``ERROR`` level would hide the very metrics
    this capture exists to prove were emitted.
    """

    def __init__(self) -> None:
        self.capture = _TelemetryCapture()
        self._logger = logging.getLogger("agent_reports")
        self._previous_level = self._logger.level

    def __enter__(self) -> _TelemetryCapture:
        configure_logging()
        self._previous_level = self._logger.level
        self._logger.setLevel(logging.DEBUG)
        self._logger.addHandler(self.capture)
        return self.capture

    def __exit__(self, *exc_info: Any) -> None:
        self._logger.removeHandler(self.capture)
        self._logger.setLevel(self._previous_level)


def _count_objects(zones: Zones) -> int:
    total = 0
    for store in (zones.raw, zones.processed, zones.reports):
        total += len(store.list_keys(""))
    return total


# --------------------------------------------------------------------------- pipeline
def run_local_pipeline(
    settings: Settings,
    options: PipelineOptions,
    *,
    zones: Zones | None = None,
    sqs: Any = None,
    ses: Any = None,
    before_dispatch: Callable[[Zones, str], Any] | None = None,
) -> PipelineResult:
    """Run the whole pipeline once and report what happened.

    Stages, in order: generate the synthetic day -> chunker aggregation (free-tier path) ->
    orchestrator fan-out -> dispatcher batches until the queue drains. Retryable failures are left
    on the queue exactly as SQS would, so a poison message ends up in the DLQ after
    ``maxReceiveCount`` receives.

    ``before_dispatch`` is a seam for the offline runner: ``scripts/e2e_local.py`` uses it to verify
    every roster recipient with SES (the sandbox requirement) between aggregation and the first
    send. Production callers pass nothing.
    """
    validate_report_date(options.report_date)
    started = time.perf_counter()
    active_zones = zones or open_zones(settings)
    active_sqs = sqs or sqs_client(settings)
    active_ses = ses or ses_client(settings)
    queue_url = settings.require_queue()
    result = PipelineResult(report_date=options.report_date)

    with capture_telemetry() as telemetry:
        stage_started = time.perf_counter()
        dataset: DatasetStats = generate_dataset(options.dataset_config(), active_zones.raw)
        result.stages["generate"] = time.perf_counter() - stage_started
        result.rows_in = dataset.rows_total
        result.dataset = dataset.as_dict()

        stage_started = time.perf_counter()
        chunker_results: list[ChunkerResult] = []
        for shard_index in range(max(1, options.shards)):
            chunker_results.append(
                run_chunker(
                    settings,
                    report_date=options.report_date,
                    shard_index=shard_index,
                    shard_count=max(1, options.shards),
                    zones=active_zones,
                    emit_metrics=True,
                )
            )
        result.stages["aggregate"] = time.perf_counter() - stage_started
        result.reports_written = sum(item.reports_written for item in chunker_results)
        result.agents_reported = sum(item.agents_reported for item in chunker_results)

        stage_started = time.perf_counter()
        fanout: OrchestratorResult = run_orchestrator(
            settings,
            report_date=options.report_date,
            zones=active_zones,
            sqs=active_sqs,
        )
        result.stages["fanout"] = time.perf_counter() - stage_started
        result.agents_discovered = fanout.agents_discovered
        result.messages_enqueued = fanout.messages_enqueued
        result.manifest_key = fanout.manifest_uri
        result.failed_agents = list(fanout.failed_agent_ids)

        if before_dispatch is not None:
            result.stages["before_dispatch"] = 0.0
            hook_started = time.perf_counter()
            before_dispatch(active_zones, options.report_date)
            result.stages["before_dispatch"] = time.perf_counter() - hook_started

        stage_started = time.perf_counter()
        batches = 0
        budget = options.dispatch_batch_budget(result.messages_enqueued, settings.sqs_batch_size)
        while batches < budget:
            records = receive_records(
                active_sqs,
                queue_url,
                max_messages=settings.sqs_batch_size,
                visibility_timeout=options.visibility_timeout,
            )
            if not records:
                break
            batches += 1
            batch = run_dispatcher(
                settings,
                records=records,
                zones=active_zones,
                ses=active_ses,
            )
            result.emails_sent += batch.sent
            result.duplicates += batch.duplicates
            result.failed += batch.failed
            result.quarantined += batch.quarantined
            failed_ids = set(batch.batch_item_failures)
            for record in records:
                message_id = str(record.get("messageId", ""))
                if message_id in failed_ids:
                    continue  # left for redelivery, exactly like the event source mapping
                active_sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=record["receiptHandle"])
        result.stages["dispatch"] = time.perf_counter() - stage_started

        depth = queue_depth(active_sqs, queue_url)
        result.queue_drained = depth["visible"] == 0 and depth["in_flight"] == 0

        dlq_url = settings.dlq_url
        if dlq_url:
            for _ in range(max(0, options.dlq_wait_receives)):
                if queue_depth(active_sqs, dlq_url)["visible"]:
                    break
                time.sleep(0.05)
            result.dlq_messages = queue_depth(active_sqs, dlq_url)["visible"]

        result.objects_written = _count_objects(active_zones)
        result.metrics_emitted = len(telemetry.metric_documents)
        result.metric_names = list(telemetry.metric_names)
        result.metric_identities = sorted(telemetry.metric_identities)
        events: dict[str, int] = {}
        for name in telemetry.events:
            events[name] = events.get(name, 0) + 1
        result.log_events = dict(sorted(events.items()))

    sample = _first_report_key(active_zones, options.report_date)
    if sample is not None:
        result.sample_agent_id = sample[0]
        result.sample_report_key = sample[1]

    result.duration_seconds = time.perf_counter() - started
    return result


def _first_report_key(zones: Zones, report_date: str) -> tuple[str, str] | None:
    for key in zones.reports.list_keys(f"reports/dt={report_date}/"):
        parsed = parse_report_key(key)
        if parsed is not None:
            return parsed.agent_id, report_key(parsed.report_date, parsed.agent_id)
    return None
