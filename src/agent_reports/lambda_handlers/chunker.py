"""Free-tier aggregation Lambda: raw partitions -> per-agent report CSVs, no EMR, no Spark.

This is the path the whole offline test suite exercises, and the one that costs nothing: for a day
whose raw volume fits under ``chunker_max_policies`` policies per shard, a Lambda does the
aggregation the Spark job would otherwise do on EMR. The output is byte-identical to the Spark
job's (asserted in ``tests/integration/test_spark_job.py``), so switching between the two paths is a
config change rather than a migration.

Event shape::

    {"report_date": "2026-09-20"}                      # whole day, one invocation
    {"report_date": "2026-09-20", "shard": {"index": 0, "of": 4}}   # one of N parallel shards
    {"report_date": "2026-09-20", "agent_ids": ["AGT-000001"]}      # explicit replay
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..common.aggregation import RawAggregator
from ..common.keys import report_key, validate_report_date
from ..common.logging_utils import configure_logging, get_logger, log_event
from ..common.metrics import METRIC_NAMES, Metric, emit_emf, put_metric_data
from ..common.roster import iter_source_rows, plan_shards, read_roster
from ..common.settings import Settings, load_settings
from ..common.storage import Zones, open_zones

__all__ = ["ChunkerResult", "handler", "run_chunker"]

_LOG = get_logger(__name__)


@dataclass
class ChunkerResult:
    """What one chunker invocation did."""

    report_date: str
    shard_index: int
    shard_count: int
    agents_planned: int = 0
    agents_reported: int = 0
    agents_skipped: list[str] = field(default_factory=list)
    reports_written: int = 0
    report_keys: list[str] = field(default_factory=list)
    rows_read: int = 0
    rows_in_scope: int = 0
    policies: int = 0
    claims: int = 0
    bytes_written: int = 0
    duration_seconds: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "report_date": self.report_date,
            "shard_index": self.shard_index,
            "shard_count": self.shard_count,
            "agents_planned": self.agents_planned,
            "agents_reported": self.agents_reported,
            "agents_skipped": self.agents_skipped,
            "reports_written": self.reports_written,
            "report_keys": self.report_keys,
            "rows_read": self.rows_read,
            "rows_in_scope": self.rows_in_scope,
            "policies": self.policies,
            "claims": self.claims,
            "bytes_written": self.bytes_written,
            "duration_seconds": round(self.duration_seconds, 3),
        }


def run_chunker(
    settings: Settings,
    *,
    report_date: str,
    shard_index: int = 0,
    shard_count: int = 1,
    agent_ids: Sequence[str] | None = None,
    zones: Zones | None = None,
    emit_metrics: bool = True,
    cloudwatch: Any = None,
) -> ChunkerResult:
    """Aggregate the day's raw partitions into per-agent reports."""
    validate_report_date(report_date)
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    active_zones = zones or open_zones(settings)
    started = time.perf_counter()

    roster = read_roster(active_zones.raw, report_date)
    if agent_ids is not None:
        planned = [agent_id for agent_id in sorted(set(agent_ids)) if agent_id in roster]
    else:
        planned = plan_shards(list(roster), shards=shard_count, index=shard_index)

    aggregator = RawAggregator(agent_ids=set(planned), max_policies=settings.chunker_max_policies)
    for row in iter_source_rows(active_zones.raw, report_date, "agents"):
        aggregator.add_agent_row(row)
    for row in iter_source_rows(active_zones.raw, report_date, "policies"):
        aggregator.add_policy_row(row)
    for row in iter_source_rows(active_zones.raw, report_date, "claims"):
        aggregator.add_claim_row(row)

    result = ChunkerResult(
        report_date=report_date,
        shard_index=shard_index,
        shard_count=shard_count,
        agents_planned=len(planned),
        rows_read=aggregator.stats.rows_read,
        rows_in_scope=aggregator.stats.rows_in_scope,
        policies=aggregator.stats.policies,
        claims=aggregator.stats.claims,
    )

    reported = set(aggregator.agents_with_policies)
    result.agents_skipped = sorted(set(planned) - reported)

    for agent_id, csv_text in aggregator.iter_reports():
        key = report_key(report_date, agent_id)
        payload = csv_text.encode("utf-8")
        active_zones.reports.put_bytes(key, payload, content_type="text/csv")
        result.reports_written += 1
        result.report_keys.append(key)
        result.bytes_written += len(payload)

    result.agents_reported = result.reports_written
    result.duration_seconds = time.perf_counter() - started

    log_event(
        _LOG,
        "chunker_completed",
        **{k: v for k, v in result.as_dict().items() if k != "report_keys"},
    )

    if emit_metrics:
        dimensions = {"Service": "chunker", "ReportDate": report_date}
        emit_emf(
            [
                Metric(METRIC_NAMES["reports_written"], float(result.reports_written)),
                Metric(METRIC_NAMES["rows_in"], float(result.rows_in_scope)),
                Metric(METRIC_NAMES["agents_discovered"], float(result.agents_planned)),
            ],
            dimensions,
        )
        if cloudwatch is not None:
            put_metric_data(
                cloudwatch,
                [
                    Metric(METRIC_NAMES["reports_written"], float(result.reports_written)),
                    Metric(METRIC_NAMES["rows_in"], float(result.rows_in_scope)),
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
    shard = event.get("shard") or {}
    shard_index = int(shard.get("index", event.get("shard_index", 0)))
    shard_count = int(shard.get("of", event.get("shard_count", settings.chunker_shards)))
    raw_agent_ids = event.get("agent_ids")
    agent_ids = [str(a) for a in raw_agent_ids] if raw_agent_ids else None

    result = run_chunker(
        settings,
        report_date=report_date,
        shard_index=shard_index,
        shard_count=shard_count,
        agent_ids=agent_ids,
        zones=open_zones(settings),
        cloudwatch=None,
    )
    return result.as_dict()
