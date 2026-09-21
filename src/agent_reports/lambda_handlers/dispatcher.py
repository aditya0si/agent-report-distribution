"""Dispatcher Lambda: SQS-triggered, one message per agent, sends the pre-signed report link by SES.

Per message:

1. parse + validate the payload (an unparseable payload is *quarantined* to
   ``state/quarantine/...`` and acknowledged - looping on a corrupt message just burns money);
2. take the idempotency lease for ``(report_date, agent_id)`` - if the marker says ``sent``, or a
   live lease exists, the message is dropped as a duplicate and counted;
3. confirm the report object exists, then read its TOTAL row for per-agent personalisation
   (skipped, with a metric, when the object is larger than ``max_report_bytes``);
4. mint a short-TTL pre-signed S3 URL;
5. send the email through SES with the configuration set, tagged with agent/date for cost
   attribution, retrying only throttling/5xx failures;
6. mark the marker ``sent`` with the SES message id.

Failures are classified, and **both** classes are returned in ``batchItemFailures`` so SQS redelivers
them and finally moves them to the DLQ - an acknowledged message is deleted, so acknowledging a
failure is how a report gets silently dropped. Retryable failures (throttling, a report that is not
in S3 yet, SES sending paused) are expected to clear; permanent ones (an unverified recipient) need a
human, and the DLQ is where the runbook finds them. The only message acknowledged without a send is
an unparseable payload, and that one is quarantined to ``state/quarantine/...`` first.

A message whose agent is already ``sent`` is the one safe suppression: it is acknowledged, because
the agent already has the email.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ..common.aws import ses_client
from ..common.errors import (
    AgentReportsError,
    InvalidMessageError,
    MissingReportError,
    classify,
)
from ..common.idempotency import DispatchLedger, DispatchRecord
from ..common.keys import report_key, validate_agent_id, validate_report_date
from ..common.logging_utils import configure_logging, get_logger, log_event
from ..common.metrics import METRIC_NAMES, Metric, emit_emf
from ..common.report import AgentTotals, parse_report_totals
from ..common.retry import RetryPolicy, call_with_retry
from ..common.settings import Settings, load_settings
from ..common.storage import Storage, Zones, open_zones
from .email_templates import render_email

__all__ = [
    "DispatchOutcome",
    "DispatchResult",
    "handler",
    "parse_message",
    "run_dispatcher",
]

_LOG = get_logger(__name__)

SES_SEND_POLICY = RetryPolicy(max_attempts=4, base_delay=0.05, max_delay=1.0)
STATUS_SENT = "sent"
STATUS_DUPLICATE = "duplicate"
STATUS_DEFERRED = "deferred"
STATUS_FAILED = "failed"
STATUS_QUARANTINED = "quarantined"

REQUIRED_FIELDS = ("agent_id", "report_date", "recipient")


@dataclass
class DispatchOutcome:
    """Result for one SQS message."""

    message_id: str
    agent_id: str = ""
    status: str = STATUS_FAILED
    reason: str = ""
    report_key: str = ""
    ses_message_id: str | None = None
    error_code: str | None = None
    retryable: bool = False
    latency_ms: float = 0.0
    report_age_seconds: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "agent_id": self.agent_id,
            "status": self.status,
            "reason": self.reason,
            "report_key": self.report_key,
            "ses_message_id": self.ses_message_id,
            "error_code": self.error_code,
            "retryable": self.retryable,
            "latency_ms": round(self.latency_ms, 3),
            "report_age_seconds": self.report_age_seconds,
        }


@dataclass
class DispatchResult:
    """Aggregate outcome of one dispatcher invocation."""

    processed: int = 0
    sent: int = 0
    duplicates: int = 0
    deferred: int = 0
    failed: int = 0
    quarantined: int = 0
    batch_item_failures: list[str] = field(default_factory=list)
    outcomes: list[DispatchOutcome] = field(default_factory=list)
    duration_seconds: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "processed": self.processed,
            "sent": self.sent,
            "duplicates": self.duplicates,
            "deferred": self.deferred,
            "failed": self.failed,
            "quarantined": self.quarantined,
            "batch_item_failures": self.batch_item_failures,
            "outcomes": [outcome.as_dict() for outcome in self.outcomes],
            "duration_seconds": round(self.duration_seconds, 3),
        }


# --------------------------------------------------------------------------- payload
def parse_message(body: str, message_id: str = "") -> dict[str, Any]:
    """Parse and validate an SQS body; raises :class:`InvalidMessageError`."""
    try:
        payload = json.loads(body)
    except (TypeError, ValueError) as exc:
        raise InvalidMessageError(
            "message body is not valid JSON",
            context={"message_id": message_id, "body_prefix": str(body)[:120]},
        ) from exc
    if not isinstance(payload, dict):
        raise InvalidMessageError(
            "message body must be a JSON object", context={"message_id": message_id}
        )
    missing = [name for name in REQUIRED_FIELDS if not payload.get(name)]
    if missing:
        raise InvalidMessageError(
            "message is missing required fields",
            context={"message_id": message_id, "missing": missing},
        )
    try:
        validate_agent_id(str(payload["agent_id"]))
        validate_report_date(str(payload["report_date"]))
    except ValueError as exc:
        raise InvalidMessageError(
            "message fields are malformed",
            context={"message_id": message_id, "error": str(exc)},
        ) from exc
    recipient = str(payload["recipient"])
    if "@" not in recipient or recipient.startswith("@") or recipient.endswith("@"):
        raise InvalidMessageError(
            "recipient is not an email address",
            context={"message_id": message_id, "recipient": recipient},
        )
    return payload


def quarantine_message(
    storage: Storage,
    *,
    report_date: str,
    message_id: str,
    body: str,
    error: AgentReportsError,
    received_at: datetime | None = None,
) -> str:
    """Persist a message we refuse to process, so a human can look at it later."""
    moment = received_at or datetime.now(tz=UTC)
    digest = hashlib.sha256(f"{message_id}:{body}".encode()).hexdigest()[:16]
    key = f"state/quarantine/dt={report_date}/message-{digest}.json"
    storage.put_json(
        key,
        {
            "message_id": message_id,
            "report_date": report_date,
            "quarantined_at": moment.isoformat(),
            "error_code": error.code,
            "error_message": error.message,
            "error_context": {k: str(v) for k, v in error.context.items()},
            "body": body[:4096],
        },
    )
    return key


# --------------------------------------------------------------------------- one message
def _totals_for_report(
    settings: Settings, storage: Storage, key: str
) -> tuple[AgentTotals | None, float | None]:
    """Read the report's TOTAL row, respecting ``max_report_bytes``."""
    size = storage.size(key)
    modified = storage.last_modified(key)
    age = (datetime.now(tz=UTC) - modified).total_seconds() if modified else None
    if size > settings.max_report_bytes:
        log_event(
            _LOG,
            "report_too_large_for_inline_summary",
            level=30,
            report_key=key,
            size_bytes=size,
            max_report_bytes=settings.max_report_bytes,
        )
        return None, age
    text = storage.get_bytes(key).decode("utf-8")
    return parse_report_totals(text), age


def dispatch_one(
    settings: Settings,
    *,
    payload: Mapping[str, Any],
    message_id: str,
    zones: Zones,
    ses: Any,
    ledger: DispatchLedger,
    now: datetime | None = None,
) -> DispatchOutcome:
    """Send one agent's report. Raises a classified error on failure."""
    started = time.perf_counter()
    agent_id = str(payload["agent_id"])
    report_date = str(payload["report_date"])
    recipient = str(payload["recipient"])
    outcome = DispatchOutcome(message_id=message_id, agent_id=agent_id)

    claim = ledger.claim(report_date, agent_id, recipient=recipient, now=now)
    if not claim.claimed:
        outcome.reason = claim.reason
        if claim.redeliver:
            # Another worker may still be sending (a live lease), or the marker moved under us.
            # The message is NOT deleted: if that worker died, this message is the only thing that
            # will ever deliver the report. SQS redelivers it and the lease is re-claimed as
            # ``stale_lease`` once it expires.
            outcome.status = STATUS_DEFERRED
            outcome.retryable = True
        else:
            outcome.status = STATUS_DUPLICATE
        outcome.latency_ms = (time.perf_counter() - started) * 1000
        return outcome

    record: DispatchRecord = claim.record
    key = report_key(report_date, agent_id)
    outcome.report_key = key
    try:
        if not zones.reports.exists(key):
            raise MissingReportError(
                "report object is not in the reports bucket",
                context={"agent_id": agent_id, "report_date": report_date, "report_key": key},
            )
        totals, age = _totals_for_report(settings, zones.reports, key)
        outcome.report_age_seconds = age
        expires_in = settings.presign_ttl_seconds
        download_url = zones.reports.presign_get(
            key, expires_in=expires_in, filename=f"{agent_id}-{report_date}.csv"
        )
        content = render_email(
            agent_id=agent_id,
            agent_name=str(payload.get("agent_name", "")),
            region=str(payload.get("region", "")),
            branch=str(payload.get("branch", "")),
            report_date=report_date,
            recipient=recipient,
            download_url=download_url,
            expires_at=datetime.now(tz=UTC) + timedelta(seconds=expires_in),
            expires_in_seconds=expires_in,
            sender=settings.ses_sender,
            totals=totals,
            now=now,
        )
        response = call_with_retry(
            lambda: ses.send_email(
                Source=settings.ses_sender,
                Destination={"ToAddresses": [recipient]},
                Message={
                    "Subject": {"Data": content.subject, "Charset": "UTF-8"},
                    "Body": {
                        "Text": {"Data": content.text_body, "Charset": "UTF-8"},
                        "Html": {"Data": content.html_body, "Charset": "UTF-8"},
                    },
                },
                ConfigurationSetName=settings.ses_configuration_set,
                Tags=[
                    {"Name": "agent_id", "Value": agent_id},
                    {"Name": "report_date", "Value": report_date},
                ],
            ),
            policy=SES_SEND_POLICY,
            operation="ses.send_email",
        )
    except BaseException as exc:
        error = classify(exc)
        ledger.mark_failed(
            report_date,
            agent_id,
            error_code=error.code,
            error_message=error.message,
            now=now,
            record=record,
        )
        outcome.status = STATUS_FAILED
        outcome.reason = error.message
        outcome.error_code = error.code
        outcome.retryable = error.retryable
        outcome.latency_ms = (time.perf_counter() - started) * 1000
        raise error from exc

    ses_message_id = str(response.get("MessageId", ""))
    ledger.mark_sent(
        report_date,
        agent_id,
        ses_message_id=ses_message_id,
        recipient=recipient,
        now=now,
        record=record,
    )
    outcome.status = STATUS_SENT
    outcome.reason = "delivered"
    outcome.ses_message_id = ses_message_id
    outcome.latency_ms = (time.perf_counter() - started) * 1000
    return outcome


# --------------------------------------------------------------------------- batch
def run_dispatcher(
    settings: Settings,
    *,
    records: Sequence[Mapping[str, Any]],
    zones: Zones | None = None,
    ses: Any = None,
    ledger: DispatchLedger | None = None,
    emit_metrics: bool = True,
    now: datetime | None = None,
) -> DispatchResult:
    """Process a batch of SQS records the way the Lambda event source mapping would."""
    started = time.perf_counter()
    active_zones = zones or open_zones(settings)
    active_ses = ses or ses_client(settings)
    active_ledger = ledger or DispatchLedger(active_zones.processed)
    result = DispatchResult()

    for record in records:
        message_id = str(record.get("messageId", ""))
        body = str(record.get("body", ""))
        result.processed += 1
        try:
            payload = parse_message(body, message_id)
        except InvalidMessageError as exc:
            report_date = _quarantine_date(record, body, settings)
            key = quarantine_message(
                active_zones.processed,
                report_date=report_date,
                message_id=message_id,
                body=body,
                error=exc,
                received_at=now,
            )
            result.quarantined += 1
            result.outcomes.append(
                DispatchOutcome(
                    message_id=message_id,
                    status=STATUS_QUARANTINED,
                    reason=exc.message,
                    error_code=exc.code,
                    report_key=key,
                )
            )
            log_event(
                _LOG,
                "message_quarantined",
                level=40,
                message_id=message_id,
                quarantine_key=key,
                error=exc.as_log_fields(),
            )
            continue

        try:
            outcome = dispatch_one(
                settings,
                payload=payload,
                message_id=message_id,
                zones=active_zones,
                ses=active_ses,
                ledger=active_ledger,
                now=now,
            )
        except AgentReportsError as exc:
            outcome = DispatchOutcome(
                message_id=message_id,
                agent_id=str(payload.get("agent_id", "")),
                status=STATUS_FAILED,
                reason=exc.message,
                error_code=exc.code,
                retryable=exc.retryable,
                report_key=report_key(str(payload["report_date"]), str(payload["agent_id"])),
            )
            result.failed += 1
            # Every failed send goes back to SQS, retryable or not: an acknowledged message is
            # deleted, so a "permanent" failure that a human can fix (an unverified recipient, a
            # resumed SES account) would be lost with no DLQ entry to redrive. maxReceiveCount
            # bounds the retries and the DLQ is the surface the runbook works from.
            result.batch_item_failures.append(message_id)
            log_event(
                _LOG,
                "dispatch_failed",
                level=40,
                message_id=message_id,
                agent_id=outcome.agent_id,
                error=exc.as_log_fields(),
            )
            result.outcomes.append(outcome)
            continue

        if outcome.status == STATUS_SENT:
            result.sent += 1
        elif outcome.status == STATUS_DUPLICATE:
            result.duplicates += 1
        elif outcome.status == STATUS_DEFERRED:
            result.deferred += 1
            result.batch_item_failures.append(message_id)
        result.outcomes.append(outcome)

    result.duration_seconds = time.perf_counter() - started

    log_event(
        _LOG,
        "dispatcher_batch_completed",
        processed=result.processed,
        sent=result.sent,
        duplicates=result.duplicates,
        deferred=result.deferred,
        failed=result.failed,
        quarantined=result.quarantined,
        batch_item_failures=len(result.batch_item_failures),
        duration_seconds=round(result.duration_seconds, 3),
    )

    if emit_metrics:
        dimensions = {"Service": "dispatcher"}
        latencies = [o.latency_ms for o in result.outcomes if o.status == STATUS_SENT]
        metrics = [
            Metric(METRIC_NAMES["emails_sent"], float(result.sent)),
            Metric(METRIC_NAMES["emails_failed"], float(result.failed)),
            Metric(METRIC_NAMES["duplicates_suppressed"], float(result.duplicates)),
            Metric(METRIC_NAMES["batch_item_failures"], float(len(result.batch_item_failures))),
        ]
        if latencies:
            metrics.append(
                Metric(
                    METRIC_NAMES["dispatch_latency_ms"],
                    sum(latencies) / len(latencies),
                    unit="Milliseconds",
                )
            )
        ages = [o.report_age_seconds for o in result.outcomes if o.report_age_seconds is not None]
        if ages:
            metrics.append(Metric(METRIC_NAMES["report_age_seconds"], max(ages), unit="Seconds"))
        emit_emf(metrics, dimensions)

    return result


def handler(event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
    """SQS event source mapping entry point (partial batch response)."""
    configure_logging()
    settings = load_settings()
    records = list(event.get("Records", []))
    result = run_dispatcher(
        settings,
        records=records,
        zones=open_zones(settings),
        ses=ses_client(settings),
    )
    return {"batchItemFailures": [{"itemIdentifier": mid} for mid in result.batch_item_failures]}


def _quarantine_date(record: Mapping[str, Any], body: str, settings: Settings) -> str:
    """Best-effort date partition for a message we could not parse.

    Tries the message attributes first, then the body, then falls back to the configured run date:
    a quarantined message must always land somewhere a human can find it, even when every field in
    it is garbage.
    """
    attributes = record.get("messageAttributes") or {}
    for source in (attributes.get("report_date", {}), attributes.get("ReportDate", {})):
        value = source.get("stringValue") if isinstance(source, Mapping) else None
        if isinstance(value, str) and len(value) == 10:
            return value
    candidate = _report_date_from_body(body)
    if candidate is not None:
        return candidate
    return settings.report_date or datetime.now(tz=UTC).date().isoformat()


def _report_date_from_body(body: str) -> str | None:
    """``report_date`` out of a body we already know is unparseable, when it happens to be readable."""
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return None
    if not isinstance(parsed, Mapping):
        return None
    candidate = str(parsed.get("report_date", ""))
    return candidate if len(candidate) == 10 else None
