"""CloudWatch metrics: Embedded Metric Format (EMF) plus a direct ``PutMetricData`` path.

EMF is the cheap path - the Lambda writes one structured log line and CloudWatch Logs extracts the
metric, so there is no API call on the hot path and no IAM permission needed. The direct
``put_metric_data`` path is kept for values that must be published even when the log line is not
shipped (e.g. the orchestrator's fan-out counters) and for local runs where a test wants to read the
metric back through the CloudWatch API.

Metric names used across the pipeline live in :data:`METRIC_NAMES` so dashboards, alarms
(``infra/terraform/cloudwatch.tf``) and code cannot drift apart.
"""

from __future__ import annotations

import json  # noqa: F401 - kept for JSON round-trip helpers used by callers
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping, Sequence

from .logging_utils import get_logger, log_emf

__all__ = [
    "DEFAULT_NAMESPACE",
    "METRIC_NAMES",
    "Metric",
    "emf_document",
    "emit_emf",
    "put_metric_data",
]

DEFAULT_NAMESPACE = "AgentReports"

METRIC_NAMES = {
    "agents_discovered": "AgentsDiscovered",
    "messages_enqueued": "MessagesEnqueued",
    "emails_sent": "EmailsSent",
    "emails_failed": "EmailsFailed",
    "duplicates_suppressed": "DuplicatesSuppressed",
    "batch_item_failures": "BatchItemFailures",
    "presign_issued": "PresignIssued",
    "presign_denied": "PresignDenied",
    "reports_written": "ReportsWritten",
    "rows_in": "RowsIn",
    "report_age_seconds": "ReportAgeSeconds",
    "dispatch_latency_ms": "DispatchLatencyMs",
}

_LOG = get_logger(__name__)


@dataclass(frozen=True)
class Metric:
    """One EMF metric datum."""

    name: str
    value: float
    unit: str = "Count"

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("metric name must not be empty")
        if self.unit not in ("Count", "Seconds", "Milliseconds", "Bytes", "Percent", "None"):
            raise ValueError(f"unsupported EMF unit: {self.unit!r}")


def emf_document(
    metrics: Sequence[Metric],
    dimensions: Mapping[str, str],
    *,
    namespace: str = DEFAULT_NAMESPACE,
    timestamp: datetime | None = None,
    properties: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the single log-event payload CloudWatch Logs understands as EMF."""
    if not metrics:
        raise ValueError("at least one metric is required")
    if not dimensions:
        raise ValueError("at least one dimension is required")
    moment = timestamp or datetime.now(tz=UTC)
    if moment.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")

    document: dict[str, Any] = {
        "_aws": {
            "Timestamp": int(moment.timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [list(dimensions.keys())],
                    "Metrics": [
                        {"Name": metric.name, "Unit": metric.unit} for metric in metrics
                    ],
                }
            ],
        }
    }
    if properties:
        document.update({f"property_{k}": v for k, v in properties.items()})
    document.update(dimensions)
    for metric in metrics:
        document[metric.name] = metric.value
    return document


def emit_emf(
    metrics: Iterable[Metric],
    dimensions: Mapping[str, str],
    *,
    logger: Any = None,
    namespace: str = DEFAULT_NAMESPACE,
    timestamp: datetime | None = None,
    properties: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Log one EMF document and return it (tests assert on the returned object)."""
    metric_list = list(metrics)
    document = emf_document(
        metric_list,
        dimensions,
        namespace=namespace,
        timestamp=timestamp,
        properties=properties,
    )
    active_logger = logger or _LOG
    # The EMF contract: the log event message is the JSON document itself, with `_aws` at the root.
    log_emf(active_logger, document)
    return document


def put_metric_data(
    client: Any,
    metrics: Iterable[Metric],
    dimensions: Mapping[str, str],
    *,
    namespace: str = DEFAULT_NAMESPACE,
) -> int:
    """Publish metrics through the CloudWatch API; returns how many datapoints were sent."""
    metric_list = list(metrics)
    if not metric_list:
        return 0
    if not dimensions:
        raise ValueError("at least one dimension is required")
    client.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                "MetricName": metric.name,
                "Value": float(metric.value),
                "Unit": metric.unit,
                "Dimensions": [
                    {"Name": key, "Value": value} for key, value in dimensions.items()
                ],
            }
            for metric in metric_list
        ],
    )
    return len(metric_list)
