"""Orchestrator Lambda: discover the day's agents, fan out one SQS message per agent, write a manifest.

Steps, in order:

1. resolve the report date (event -> ``AGENT_REPORTS_REPORT_DATE`` -> today, UTC);
2. read the day's roster from ``raw/dt=<date>/source=agents/`` and the set of agents that actually
   have a report in ``reports/dt=<date>/``;
3. send ``SendMessageBatch`` requests of ``sqs_batch_size`` (max 10). **Partial** failures - SQS
   returns a ``Failed`` list per batch - are retried individually with backoff, and anything that
   still fails is recorded in the manifest instead of being silently dropped;
4. write the run manifest to ``state/runs/dt=<date>/manifest.json``;
5. emit EMF metrics and (optionally) CloudWatch ``PutMetricData``.

The handler is deliberately side-effect free at import time and takes its AWS clients from the
shared factory, so a warm container reuses connections.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from typing import Any, cast

from ..common.aws import cloudwatch_client, sqs_client
from ..common.errors import AgentReportsError
from ..common.keys import manifest_key, validate_agent_id, validate_report_date
from ..common.logging_utils import configure_logging, get_logger, log_event
from ..common.metrics import METRIC_NAMES, Metric, emit_emf, put_metric_data
from ..common.retry import RetryPolicy, call_with_retry
from ..common.roster import read_roster, report_agent_ids
from ..common.settings import Settings, load_settings
from ..common.storage import Zones, open_zones

__all__ = ["OrchestratorResult", "handler", "plan_fanout", "run_orchestrator"]

_LOG = get_logger(__name__)

MAX_BATCH_SIZE = 10
BATCH_SEND_POLICY = RetryPolicy(max_attempts=4, base_delay=0.05, max_delay=1.0)


@dataclass
class OrchestratorResult:
    """Summary of one fan-out run (also what the manifest stores)."""

    report_date: str
    agents_discovered: int = 0
    agents_with_reports: int = 0
    agents_targeted: int = 0
    agents_without_reports: list[str] = field(default_factory=list)
    messages_enqueued: int = 0
    batches_sent: int = 0
    partial_batch_failures: int = 0
    failed_agent_ids: list[str] = field(default_factory=list)
    manifest_uri: str = ""
    duration_seconds: float = 0.0
    started_at: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "report_date": self.report_date,
            "agents_discovered": self.agents_discovered,
            "agents_with_reports": self.agents_with_reports,
            "agents_targeted": self.agents_targeted,
            "agents_without_reports": self.agents_without_reports,
            "messages_enqueued": self.messages_enqueued,
            "batches_sent": self.batches_sent,
            "partial_batch_failures": self.partial_batch_failures,
            "failed_agent_ids": self.failed_agent_ids,
            "manifest_uri": self.manifest_uri,
            "duration_seconds": round(self.duration_seconds, 3),
            "started_at": self.started_at,
        }


def plan_fanout(
    roster: Mapping[str, Mapping[str, str]],
    report_agent_ids: Sequence[str],
    *,
    require_report: bool = True,
    requested_agent_ids: Sequence[str] | None = None,
) -> tuple[list[str], list[str]]:
    """Return ``(targets, agents_without_reports)``.

    ``require_report`` keeps the invariant "never email a link to an object that does not exist";
    it is only relaxed for deliberate replay runs.
    """
    available = set(report_agent_ids)
    candidates = sorted(set(requested_agent_ids)) if requested_agent_ids else sorted(roster)
    targets: list[str] = []
    missing: list[str] = []
    for agent_id in candidates:
        if agent_id not in roster:
            continue
        if agent_id in available:
            targets.append(agent_id)
        else:
            missing.append(agent_id)
    if not require_report:
        targets = sorted(set(targets) | set(missing))
    return targets, missing


def build_message(agent_id: str, report_date: str, roster_row: Mapping[str, str]) -> dict[str, Any]:
    """The SQS payload the dispatcher consumes."""
    return {
        "agent_id": agent_id,
        "report_date": report_date,
        "recipient": roster_row.get("email", ""),
        "agent_name": roster_row.get("agent_name", ""),
        "region": roster_row.get("region", ""),
        "branch": roster_row.get("branch", ""),
    }


def _chunk(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _send_batch(sqs: Any, queue_url: str, entries: list[dict[str, Any]]) -> dict[str, Any]:
    """One SendMessageBatch call (typed wrapper so the retry driver stays generic)."""
    response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    return cast(dict[str, Any], response)


def _send_single(
    sqs: Any, queue_url: str, body: str, agent_id: str, report_date: str
) -> dict[str, Any]:
    """Send one message individually (used to retry entries SQS rejected in a batch)."""
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=body,
        MessageAttributes={
            "report_date": {"DataType": "String", "StringValue": report_date},
            "agent_id": {"DataType": "String", "StringValue": agent_id},
        },
    )
    return cast(dict[str, Any], response)


def run_orchestrator(
    settings: Settings,
    *,
    report_date: str,
    zones: Zones | None = None,
    sqs: Any = None,
    cloudwatch: Any = None,
    requested_agent_ids: Sequence[str] | None = None,
    require_report: bool = True,
    emit_metrics: bool = True,
) -> OrchestratorResult:
    """Run the fan-out. Returns a summary; never raises for a single agent's failure."""
    validate_report_date(report_date)
    started = time.perf_counter()
    active_zones = zones or open_zones(settings)
    queue_url = settings.require_queue()
    sqs = sqs or sqs_client(settings)

    roster = read_roster(active_zones.raw, report_date)
    report_ids = report_agent_ids(active_zones.reports, report_date)
    targets, missing = plan_fanout(
        roster,
        report_ids,
        require_report=require_report,
        requested_agent_ids=requested_agent_ids,
    )

    result = OrchestratorResult(
        report_date=report_date,
        agents_discovered=len(roster),
        agents_with_reports=len(report_ids),
        agents_targeted=len(targets),
        agents_without_reports=missing[:100],
        started_at=datetime.now(tz=UTC).isoformat(),
    )

    for batch in _chunk(targets, min(settings.sqs_batch_size, MAX_BATCH_SIZE)):
        entries = [
            {
                "Id": agent_id,
                "MessageBody": json.dumps(
                    build_message(agent_id, report_date, roster.get(agent_id, {})),
                    separators=(",", ":"),
                ),
                "MessageAttributes": {
                    "report_date": {"DataType": "String", "StringValue": report_date},
                    "agent_id": {"DataType": "String", "StringValue": agent_id},
                },
            }
            for agent_id in batch
        ]
        result.batches_sent += 1
        response = call_with_retry(
            partial(_send_batch, sqs, queue_url, entries),
            policy=BATCH_SEND_POLICY,
            operation="send_message_batch",
        )
        successful = response.get("Successful", [])
        failed = response.get("Failed", [])
        result.messages_enqueued += len(successful)
        if failed:
            result.partial_batch_failures += len(failed)
            log_event(
                _LOG,
                "partial_batch_failure",
                level=30,
                report_date=report_date,
                failed=[entry.get("Id") for entry in failed],
                codes=sorted({str(entry.get("Code")) for entry in failed}),
            )
            # Retry the individual messages SQS rejected (typically throttling on one shard).
            for entry in failed:
                agent_id = str(entry.get("Id", ""))
                retry_body = next(
                    (str(item["MessageBody"]) for item in entries if str(item["Id"]) == agent_id),
                    None,
                )
                if retry_body is None:
                    continue
                try:
                    call_with_retry(
                        partial(_send_single, sqs, queue_url, retry_body, agent_id, report_date),
                        policy=BATCH_SEND_POLICY,
                        operation="send_message",
                    )
                    result.messages_enqueued += 1
                except AgentReportsError as exc:
                    log_event(
                        _LOG,
                        "message_enqueue_failed",
                        level=40,
                        agent_id=agent_id,
                        error=exc.as_log_fields(),
                    )
                    result.failed_agent_ids.append(agent_id)

    result.duration_seconds = time.perf_counter() - started
    manifest = {
        **result.as_dict(),
        "settings": {
            "region": settings.region,
            "raw_bucket": settings.raw_bucket,
            "reports_bucket": settings.reports_bucket,
            "sqs_batch_size": settings.sqs_batch_size,
            "require_report": require_report,
        },
    }
    key = manifest_key(report_date)
    result.manifest_uri = active_zones.processed.put_json(key, manifest)

    log_event(_LOG, "fanout_completed", **result.as_dict())

    if emit_metrics:
        dimensions = {"Service": "orchestrator", "ReportDate": report_date}
        emit_emf(
            [
                Metric(METRIC_NAMES["agents_discovered"], float(result.agents_discovered)),
                Metric(METRIC_NAMES["messages_enqueued"], float(result.messages_enqueued)),
                Metric(METRIC_NAMES["batch_item_failures"], float(result.partial_batch_failures)),
            ],
            dimensions,
        )
        if cloudwatch is not None:
            put_metric_data(
                cloudwatch,
                [
                    Metric(METRIC_NAMES["agents_discovered"], float(result.agents_discovered)),
                    Metric(METRIC_NAMES["messages_enqueued"], float(result.messages_enqueued)),
                ],
                dimensions,
            )

    return result


def handler(event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
    """Lambda entry point (EventBridge schedule or manual replay)."""
    configure_logging()
    settings = load_settings()
    report_date = str(
        event.get("report_date") or settings.report_date or datetime.now(tz=UTC).date().isoformat()
    )
    raw_agent_ids = event.get("agent_ids")
    requested: list[str] | None = None
    if raw_agent_ids:
        requested = [validate_agent_id(str(agent_id)) for agent_id in raw_agent_ids]
    require_report = bool(event.get("require_report", True))

    zones = open_zones(settings)
    result = run_orchestrator(
        settings,
        report_date=report_date,
        zones=zones,
        sqs=sqs_client(settings),
        cloudwatch=cloudwatch_client(settings),
        requested_agent_ids=requested,
        require_report=require_report,
    )
    return result.as_dict()
