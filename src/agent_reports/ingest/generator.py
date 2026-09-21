"""Deterministic synthetic dataset generator.

Writes the three raw sources described in :mod:`agent_reports.ingest.schema` as partitioned files
under ``raw/dt=YYYY-MM-DD/source=<source>/part-NNNNN.<ext>`` in any :class:`~agent_reports.common.storage.Storage`
(a local directory, a real S3 bucket, or moto).

Properties that matter for a pipeline you can trust:

* **Deterministic** - the same :class:`DatasetConfig` always produces byte-identical files
  (one seeded ``random.Random`` stream, fixed row order, fixed sharding rule ``index % partitions``).
* **Streamed** - rows are written as they are generated, so memory stays flat as the dataset grows;
  only the optional Parquet path buffers per-partition batches (documented in ``docs/SCHEMA.md``).
* **Configurable** - :meth:`DatasetConfig.for_total_rows` scales the whole thing from a few hundred
  rows (tests) to 50k+ (the default demo size) to millions (the EMR path).
"""

from __future__ import annotations

import csv
import io
import math
import random
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from ..common.errors import ConfigError
from ..common.keys import raw_key, raw_prefix, validate_report_date
from ..common.logging_utils import get_logger, log_event
from ..common.storage import Storage
from .schema import (
    CLAIM_STATUS_NAMES,
    CLAIM_STATUSES,
    CLAIM_TYPES,
    DIAGNOSIS_CHAPTERS,
    FIRST_NAMES,
    HOSPITAL_TIERS,
    LAST_NAMES,
    POLICY_STATUSES,
    PRODUCTS,
    REGIONS,
    SOURCE_COLUMNS,
    ProductSpec,
)

__all__ = ["DatasetConfig", "DatasetStats", "generate_dataset", "iter_rows", "render_rows"]

_LOG = get_logger(__name__)

DEFAULT_POLICIES_PER_AGENT = 8
DEFAULT_CLAIMS_PER_POLICY = 0.6
#: Slack applied by :meth:`DatasetConfig.for_total_rows` so claim variance cannot undershoot the target.
ROW_MARGIN = 1.05


@dataclass(frozen=True)
class DatasetConfig:
    """Shape of the dataset to generate."""

    report_date: str
    agents: int = 400
    policies_per_agent: int = DEFAULT_POLICIES_PER_AGENT
    claims_per_policy: float = DEFAULT_CLAIMS_PER_POLICY
    seed: int = 7
    partitions: int = 4
    extension: str = "csv"
    lookback_days: int = 365

    def __post_init__(self) -> None:
        try:
            validate_report_date(self.report_date)
        except ValueError as exc:
            raise ConfigError(str(exc), context={"report_date": self.report_date}) from exc
        if self.agents < 1:
            raise ConfigError("agents must be >= 1", context={"agents": self.agents})
        if self.policies_per_agent < 1:
            raise ConfigError("policies_per_agent must be >= 1")
        if not 0 <= self.claims_per_policy <= 4:
            raise ConfigError("claims_per_policy must be between 0 and 4")
        if self.partitions < 1:
            raise ConfigError("partitions must be >= 1")
        if self.extension not in ("csv", "parquet"):
            raise ConfigError(f"unsupported extension {self.extension!r}")

    # ------------------------------------------------------------------ derived
    @property
    def policies(self) -> int:
        return self.agents * self.policies_per_agent

    @property
    def estimated_rows(self) -> int:
        return self.agents + self.policies + round(self.policies * self.claims_per_policy)

    @property
    def row_ratio(self) -> float:
        """Rows per agent at this shape - used to scale to a target row count."""
        return 1 + self.policies_per_agent * (1 + self.claims_per_policy)

    @classmethod
    def for_total_rows(
        cls,
        total_rows: int,
        report_date: str,
        *,
        seed: int = 7,
        partitions: int = 4,
        extension: str = "csv",
        policies_per_agent: int = DEFAULT_POLICIES_PER_AGENT,
        claims_per_policy: float = DEFAULT_CLAIMS_PER_POLICY,
    ) -> DatasetConfig:
        """Scale the shape so the generated dataset has at least ``total_rows`` rows.

        Claim incidence is stochastic, so the agent count carries a small margin
        (:data:`ROW_MARGIN`) - the realised count is reported by the generator and asserted in
        ``tests/unit/test_generator.py``.
        """
        if total_rows < 1:
            raise ConfigError("total_rows must be >= 1")
        ratio = 1 + policies_per_agent * (1 + claims_per_policy)
        agents = max(1, math.ceil(total_rows * ROW_MARGIN / ratio))
        return cls(
            report_date=report_date,
            agents=agents,
            policies_per_agent=policies_per_agent,
            claims_per_policy=claims_per_policy,
            seed=seed,
            partitions=partitions,
            extension=extension,
        )

    def describe(self) -> dict[str, Any]:
        return {
            "report_date": self.report_date,
            "agents": self.agents,
            "policies": self.policies,
            "claims_per_policy": self.claims_per_policy,
            "estimated_rows": self.estimated_rows,
            "seed": self.seed,
            "partitions": self.partitions,
            "extension": self.extension,
        }


@dataclass
class DatasetStats:
    """What was actually written."""

    report_date: str
    agents: int = 0
    policies: int = 0
    claims: int = 0
    parts: int = 0
    bytes_written: int = 0
    duration_seconds: float = 0.0
    output_uri: str = ""
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def rows_total(self) -> int:
        return self.agents + self.policies + self.claims

    def as_dict(self) -> dict[str, Any]:
        return {
            "report_date": self.report_date,
            "agents": self.agents,
            "policies": self.policies,
            "claims": self.claims,
            "rows_total": self.rows_total,
            "parts": self.parts,
            "bytes_written": self.bytes_written,
            "duration_seconds": round(self.duration_seconds, 3),
            "output_uri": self.output_uri,
            "config": self.config,
        }


# --------------------------------------------------------------------------- row builders
def iter_rows(config: DatasetConfig) -> Iterator[tuple[str, int, dict[str, str]]]:
    """Yield ``(source, index, row)`` in a deterministic order."""
    rng = random.Random(config.seed)
    anchor = date.fromisoformat(config.report_date)

    agent_ids: list[str] = []
    for index in range(config.agents):
        agent_id = f"AGT-{index + 1:06d}"
        agent_ids.append(agent_id)
        region = rng.choice(tuple(REGIONS))
        branch = rng.choice(REGIONS[region])
        joined = anchor - timedelta(days=rng.randint(30, 2200))
        manager = (
            agent_ids[rng.randrange(len(agent_ids))] if index and rng.random() < 0.9 else agent_id
        )
        yield (
            "agents",
            index,
            {
                "agent_id": agent_id,
                "agent_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "email": f"{agent_id.lower()}@example.com",
                "region": region,
                "branch": branch,
                "manager_id": manager,
                "joined_on": joined.isoformat(),
            },
        )

    policy_index = 0
    claim_index = 0
    for agent_id in agent_ids:
        for _ in range(config.policies_per_agent):
            product = rng.choice(PRODUCTS)
            start = anchor - timedelta(days=rng.randint(0, config.lookback_days))
            end = start + timedelta(days=365)
            sum_insured = (
                rng.randrange(
                    product.sum_insured_min // 100_000, product.sum_insured_max // 100_000 + 1
                )
                * 100_000
            )
            rate = rng.uniform(product.premium_rate_min, product.premium_rate_max)
            premium = round(sum_insured * rate, 2)
            policy_id = f"POL-{policy_index + 1:010d}"
            yield (
                "policies",
                policy_index,
                {
                    "policy_id": policy_id,
                    "agent_id": agent_id,
                    "customer_id": f"CUS-{rng.randrange(1, 10**9):09d}",
                    "product": product.name,
                    "policy_start": start.isoformat(),
                    "policy_end": end.isoformat(),
                    "sum_insured": f"{sum_insured:.2f}",
                    "premium": f"{premium:.2f}",
                    "commission_rate": f"{product.commission_rate:.4f}",
                    "status": rng.choices(POLICY_STATUSES, weights=(0.70, 0.20, 0.10))[0],
                },
            )
            for _claim_no in range(
                _claim_count(
                    rng,
                    scaled_claim_probability(product, config.claims_per_policy),
                    product.max_claims_per_policy,
                )
            ):
                claim_date = min(start + timedelta(days=rng.randint(0, 365)), anchor)
                status = rng.choices(CLAIM_STATUS_NAMES, weights=CLAIM_STATUSES)[0]
                claimed = _claim_amount(
                    rng, product.severity_mean, product.severity_sigma, sum_insured
                )
                settled = (
                    0.0
                    if status in ("Rejected", "Pending")
                    else round(claimed * rng.uniform(0.8, 1.0), 2)
                )
                yield (
                    "claims",
                    claim_index,
                    {
                        "claim_id": f"CLM-{claim_index + 1:010d}",
                        "policy_id": policy_id,
                        "agent_id": agent_id,
                        "claim_date": claim_date.isoformat(),
                        "claim_type": rng.choice(CLAIM_TYPES),
                        "diagnosis_chapter": rng.choice(DIAGNOSIS_CHAPTERS),
                        "claimed_amount": f"{claimed:.2f}",
                        "settled_amount": f"{settled:.2f}",
                        "status": status,
                        "tat_days": str(rng.randint(1, 45)),
                        "hospital_tier": rng.choice(HOSPITAL_TIERS),
                    },
                )
                claim_index += 1
            policy_index += 1


def render_rows(source: str, rows: Iterable[dict[str, str]]) -> list[str]:
    """Render rows of one source as CSV lines (header + body), used by tests and the chunker."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(SOURCE_COLUMNS[source]), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().splitlines(keepends=True)


def _claim_count(rng: random.Random, probability: float, cap: int) -> int:
    if rng.random() >= probability:
        return 0
    extra = 1 if rng.random() < 0.15 else 0
    return min(cap, 1 + extra)


def scaled_claim_probability(product: ProductSpec, claims_per_policy: float) -> float:
    """Per-product claim incidence, normalised so the mean matches ``claims_per_policy``.

    The product specs carry the *relative* risk between lines (Group Health claims far more often
    than Term Life); the config knob sets the absolute level. Values saturate at 1.0.
    """
    mean_probability = sum(p.claim_probability for p in PRODUCTS) / len(PRODUCTS)
    if mean_probability <= 0:
        return 0.0
    return float(min(1.0, product.claim_probability * (claims_per_policy / mean_probability)))


def _claim_amount(rng: random.Random, mean: float, sigma: float, sum_insured: int) -> float:
    """Lognormal severity as a share of the sum insured, capped at 100% of cover."""
    mu = math.log(mean) - (sigma**2) / 2
    share = min(1.0, rng.lognormvariate(mu, sigma))
    return round(sum_insured * share, 2)


# --------------------------------------------------------------------------- writer
class _PartitionWriter:
    """Writes rows of one source across ``partitions`` files (``index % partitions``)."""

    def __init__(
        self, storage: Storage, report_date: str, source: str, partitions: int, extension: str
    ) -> None:
        self.storage = storage
        self.report_date = report_date
        self.source = source
        self.partitions = partitions
        self.extension = extension
        self._handles: list[Any] = []
        self._writers: list[csv.DictWriter[str]] = []
        self._buffers: list[list[dict[str, str]]] = []
        self._counts = [0] * partitions
        self._opened = False

    def _open(self) -> None:
        if self._opened:
            return
        self._opened = True
        if self.extension == "csv":
            for _part in range(self.partitions):
                handle = io.StringIO()
                writer = csv.DictWriter(
                    handle, fieldnames=list(SOURCE_COLUMNS[self.source]), lineterminator="\n"
                )
                writer.writeheader()
                self._handles.append(handle)
                self._writers.append(writer)
        else:
            self._buffers = [[] for _ in range(self.partitions)]

    def add(self, index: int, row: dict[str, str]) -> None:
        self._open()
        part = index % self.partitions
        self._counts[part] += 1
        if self.extension == "csv":
            self._writers[part].writerow(row)
        else:
            self._buffers[part].append(row)

    def flush(self) -> tuple[int, int]:
        """Persist every partition; returns ``(parts_written, bytes_written)``."""
        self._open()
        parts = 0
        written = 0
        for part in range(self.partitions):
            key = raw_key(self.report_date, self.source, part, self.extension)
            if self._counts[part] == 0:
                continue
            if self.extension == "csv":
                payload = self._handles[part].getvalue().encode("utf-8")
            else:
                payload = _parquet_bytes(SOURCE_COLUMNS[self.source], self._buffers[part])
            self.storage.put_bytes(
                key,
                payload,
                content_type="text/csv" if self.extension == "csv" else "application/octet-stream",
            )
            parts += 1
            written += len(payload)
        return parts, written


def _parquet_bytes(columns: Sequence[str], rows: Sequence[dict[str, str]]) -> bytes:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - exercised only without pyarrow installed
        raise ConfigError(
            "parquet output requires pyarrow: uv pip install pyarrow",
            context={"error": str(exc)},
        ) from exc

    table = pa.table({column: [row[column] for row in rows] for column in columns})
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()


def generate_dataset(
    config: DatasetConfig,
    storage: Storage,
    *,
    output_prefix: str = "",
    include_manifest: bool = True,
) -> DatasetStats:
    """Generate and write the dataset; returns the realised row counts."""
    started = time.perf_counter()
    prefix = output_prefix.strip("/")
    writers: dict[str, _PartitionWriter] = {
        source: _PartitionWriter(
            storage, config.report_date, source, config.partitions, config.extension
        )
        for source in SOURCE_COLUMNS
    }
    counts = {"agents": 0, "policies": 0, "claims": 0}

    for source, index, row in iter_rows(config):
        writers[source].add(index, row)
        counts[source] += 1

    parts = 0
    written = 0
    for writer in writers.values():
        source_parts, source_bytes = writer.flush()
        parts += source_parts
        written += source_bytes

    stats = DatasetStats(
        report_date=config.report_date,
        agents=counts["agents"],
        policies=counts["policies"],
        claims=counts["claims"],
        parts=parts,
        bytes_written=written,
        duration_seconds=time.perf_counter() - started,
        output_uri=storage.uri_for(
            f"{prefix}/{raw_prefix(config.report_date)}"
            if prefix
            else raw_prefix(config.report_date)
        ),
        config=config.describe(),
    )

    if include_manifest:
        manifest_key = (
            f"{prefix}/{raw_prefix(config.report_date)}_dataset_manifest.json"
            if prefix
            else f"{raw_prefix(config.report_date)}_dataset_manifest.json"
        )
        storage.put_json(manifest_key, stats.as_dict())

    log_event(
        _LOG,
        "dataset_generated",
        **{k: v for k, v in stats.as_dict().items() if k != "config"},
    )
    return stats
