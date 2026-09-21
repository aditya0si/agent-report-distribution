"""Reading the raw zone: source rows, the day's agent roster, and shard planning."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from .keys import SOURCES, parse_raw_key, raw_prefix, validate_report_date
from .storage import Storage

__all__ = [
    "agent_report_keys",
    "iter_source_rows",
    "plan_shards",
    "read_roster",
    "report_agent_ids",
    "source_part_keys",
]

DEFAULT_ROSTER_COLUMNS = ("agent_id", "email", "agent_name", "region", "branch")


def source_part_keys(storage: Storage, report_date: str, source: str) -> list[str]:
    """All ``part-*`` keys for one source/date, in deterministic order."""
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}; expected one of {SOURCES}")
    prefix = raw_prefix(report_date, source)
    keys = []
    for key in storage.list_keys(prefix):
        parsed = parse_raw_key(key)
        if parsed is not None and parsed.source == source and parsed.report_date == report_date:
            keys.append(key)
    return sorted(keys)


def iter_source_rows(storage: Storage, report_date: str, source: str) -> Iterator[dict[str, str]]:
    """Stream one source's rows (header-aware, file by file, never whole-file in memory)."""
    for key in source_part_keys(storage, report_date, source):
        if key.endswith(".parquet"):
            yield from _iter_parquet_rows(storage, key)
            continue
        lines = storage.iter_lines(key)
        header_line = next(iter(lines), None)
        if header_line is None:
            continue
        reader = csv.DictReader(_chained(header_line, lines))
        for row in reader:
            yield {k: (v if v is not None else "") for k, v in row.items() if k is not None}


def read_roster(
    storage: Storage,
    report_date: str,
    *,
    agent_filter: Iterable[str] | None = None,
    columns: Sequence[str] = DEFAULT_ROSTER_COLUMNS,
) -> dict[str, dict[str, str]]:
    """``agent_id -> selected columns`` for the day's roster (optionally filtered)."""
    validate_report_date(report_date)
    wanted = set(agent_filter) if agent_filter is not None else None
    roster: dict[str, dict[str, str]] = {}
    for row in iter_source_rows(storage, report_date, "agents"):
        agent_id = row.get("agent_id", "")
        if not agent_id or (wanted is not None and agent_id not in wanted):
            continue
        roster[agent_id] = {column: row.get(column, "") for column in columns}
    return roster


def agent_report_keys(storage: Storage, report_date: str, prefix: str = "reports") -> list[str]:
    """Keys of the finished per-agent reports for a date."""
    from .keys import parse_report_key  # local import keeps the module import graph shallow

    day_prefix = f"{prefix.strip('/')}/dt={report_date}/"
    keys = []
    for key in storage.list_keys(day_prefix):
        if parse_report_key(key) is not None:
            keys.append(key)
    return sorted(keys)


def report_agent_ids(storage: Storage, report_date: str, prefix: str = "reports") -> list[str]:
    """Agent ids that have a finished report for a date (sorted, de-duplicated)."""
    from .keys import parse_report_key  # local import keeps the module import graph shallow

    found: set[str] = set()
    for key in agent_report_keys(storage, report_date, prefix):
        parsed = parse_report_key(key)
        if parsed is not None:
            found.add(parsed.agent_id)
    return sorted(found)


def plan_shards(agent_ids: Sequence[str], *, shards: int, index: int) -> list[str]:
    """Deterministic round-robin split of the roster (stable across invocations).

    ``shards`` parallel chunker invocations each take one shard; the assignment is a pure function of
    the sorted roster, so two invocations can never claim the same agent.
    """
    if shards < 1:
        raise ValueError("shards must be >= 1")
    if not 0 <= index < shards:
        raise ValueError(f"shard index must be in [0, {shards})")
    ordered = sorted(agent_ids)
    return [agent for position, agent in enumerate(ordered) if position % shards == index]


def _chained(header_line: str, lines: Iterable[str]) -> Iterator[str]:
    yield header_line
    yield from lines


def _iter_parquet_rows(storage: Storage, key: str) -> Iterator[dict[str, str]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - pyarrow is a declared dev/spark dependency
        raise RuntimeError("reading parquet requires pyarrow") from exc

    import io as _io

    table = pq.read_table(_io.BytesIO(storage.get_bytes(key)))
    for batch in table.to_batches():
        data: dict[str, list[Any]] = batch.to_pydict()
        columns = list(data)
        for row_index in range(batch.num_rows):
            yield {column: _stringify(data[column][row_index]) for column in columns}


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)
