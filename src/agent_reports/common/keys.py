"""S3 key conventions for the whole pipeline.

Layout::

    raw/dt=YYYY-MM-DD/source=agents/part-00000.csv          # generator output
    raw/dt=YYYY-MM-DD/source=policies/part-00003.csv
    raw/dt=YYYY-MM-DD/source=claims/part-00001.csv
    reports/dt=YYYY-MM-DD/agent_id=AGT-000123/report.csv     # final agent report
    state/runs/dt=YYYY-MM-DD/manifest.json                   # orchestrator run manifest
    state/dispatch/dt=YYYY-MM-DD/agent_id=AGT-000123.json    # delivery + idempotency marker

Every builder/parser pair is round-trip tested, and every parser returns ``None`` (never raises) for
keys that do not belong to the layout, so listing a prefix full of unrelated objects is safe.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date as _date

__all__ = [
    "SOURCES",
    "AgentReportKey",
    "RawKey",
    "agent_ids_from_report_keys",
    "dispatch_marker_key",
    "manifest_key",
    "parse_dispatch_marker_key",
    "parse_raw_key",
    "parse_report_key",
    "raw_key",
    "raw_prefix",
    "report_key",
    "report_partition_prefix",
    "report_prefix_for",
    "validate_agent_id",
    "validate_report_date",
]

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
AGENT_ID_RE = re.compile(r"^AGT-\d{6}$")
PART_RE = re.compile(r"^part-(\d{5})\.(csv|parquet)$")

SOURCES = ("agents", "policies", "claims")

#: Number of agents per shard file (keeps individual parts small and parallelisable).
DEFAULT_PARTITIONS = 4


@dataclass(frozen=True)
class RawKey:
    report_date: str
    source: str
    part: int
    extension: str


@dataclass(frozen=True)
class AgentReportKey:
    report_date: str
    agent_id: str


# --------------------------------------------------------------------------- validation
def validate_report_date(value: str) -> str:
    """Return *value* if it is an ISO date, else raise ``ValueError``."""
    if not isinstance(value, str) or not DATE_RE.match(value):
        raise ValueError(f"report_date must be formatted YYYY-MM-DD (got {value!r})")
    try:
        _date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"report_date is not a real calendar date: {value!r}") from exc
    return value


def validate_agent_id(value: str) -> str:
    """Return *value* if it matches ``AGT-\\d{6}``, else raise ``ValueError``.

    The generator mints agent ids itself, so anything else is a client bug or an
    injection attempt; rejecting it before it reaches an S3 key keeps the prefix
    traversal-safe.
    """
    if not isinstance(value, str) or not AGENT_ID_RE.match(value):
        raise ValueError(f"agent_id must look like AGT-000123 (got {value!r})")
    return value


# --------------------------------------------------------------------------- raw zone
def raw_prefix(report_date: str, source: str | None = None) -> str:
    validate_report_date(report_date)
    prefix = f"raw/dt={report_date}"
    if source is None:
        return prefix + "/"
    _validate_source(source)
    return f"{prefix}/source={source}/"


def raw_key(report_date: str, source: str, part: int, extension: str = "csv") -> str:
    validate_report_date(report_date)
    _validate_source(source)
    if not isinstance(part, int) or part < 0:
        raise ValueError(f"part must be a non-negative int (got {part!r})")
    if extension not in ("csv", "parquet"):
        raise ValueError(f"extension must be csv or parquet (got {extension!r})")
    return f"{raw_prefix(report_date, source)}part-{part:05d}.{extension}"


def parse_raw_key(key: str) -> RawKey | None:
    parts = key.split("/")
    if len(parts) != 4 or parts[0] != "raw":
        return None
    dt_part, source_part, filename = parts[1], parts[2], parts[3]
    if not dt_part.startswith("dt=") or not source_part.startswith("source="):
        return None
    report_date = dt_part[3:]
    source = source_part[7:]
    if not DATE_RE.match(report_date) or source not in SOURCES:
        return None
    match = PART_RE.match(filename)
    if match is None:
        return None
    return RawKey(
        report_date=report_date,
        source=source,
        part=int(match.group(1)),
        extension=match.group(2),
    )


# ----------------------------------------------------------------------- reports zone
def report_prefix_for(report_date: str) -> str:
    """``reports/dt=YYYY-MM-DD/`` - the whole day's report tree."""
    validate_report_date(report_date)
    return f"reports/dt={report_date}/"


def report_partition_prefix(report_date: str, agent_id: str) -> str:
    """``reports/dt=YYYY-MM-DD/agent_id=AGT-000123/`` - one agent's partition."""
    validate_report_date(report_date)
    validate_agent_id(agent_id)
    return f"{report_prefix_for(report_date)}agent_id={agent_id}/"


def report_key(report_date: str, agent_id: str) -> str:
    """Exact object key of one agent's CSV report."""
    return report_partition_prefix(report_date, agent_id) + "report.csv"


def parse_report_key(key: str) -> AgentReportKey | None:
    parts = key.split("/")
    if len(parts) != 4 or parts[0] != "reports" or parts[3] != "report.csv":
        return None
    dt_part, agent_part = parts[1], parts[2]
    if not dt_part.startswith("dt=") or not agent_part.startswith("agent_id="):
        return None
    report_date = dt_part[3:]
    agent_id = agent_part[len("agent_id=") :]
    if not DATE_RE.match(report_date) or not AGENT_ID_RE.match(agent_id):
        return None
    return AgentReportKey(report_date=report_date, agent_id=agent_id)


def agent_ids_from_report_keys(keys: Iterable[str]) -> list[str]:
    """Extract the sorted, de-duplicated agent ids from an iterable of object keys."""
    found: set[str] = set()
    for key in keys:
        parsed = parse_report_key(str(key))
        if parsed is not None:
            found.add(parsed.agent_id)
    return sorted(found)


# ------------------------------------------------------------------------- state zone
def manifest_key(report_date: str) -> str:
    """Run manifest written by the orchestrator (and read by ops tooling)."""
    validate_report_date(report_date)
    return f"state/runs/dt={report_date}/manifest.json"


def dispatch_marker_key(report_date: str, agent_id: str) -> str:
    """Idempotency + delivery-state marker: one object per agent per day."""
    validate_report_date(report_date)
    validate_agent_id(agent_id)
    return f"state/dispatch/dt={report_date}/agent_id={agent_id}.json"


def parse_dispatch_marker_key(key: str) -> AgentReportKey | None:
    parts = key.split("/")
    if len(parts) != 4 or parts[0] != "state" or parts[1] != "dispatch":
        return None
    dt_part, filename = parts[2], parts[3]
    if not dt_part.startswith("dt=") or not filename.startswith("agent_id="):
        return None
    report_date = dt_part[3:]
    if not filename.endswith(".json"):
        return None
    agent_id = filename[len("agent_id=") : -len(".json")]
    if not DATE_RE.match(report_date) or not AGENT_ID_RE.match(agent_id):
        return None
    return AgentReportKey(report_date=report_date, agent_id=agent_id)


def _validate_source(source: str) -> None:
    if source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES} (got {source!r})")
