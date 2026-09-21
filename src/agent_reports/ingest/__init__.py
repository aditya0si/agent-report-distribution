"""Synthetic dataset generation (no PII, no real data, deterministic seed)."""

from __future__ import annotations

from .generator import DatasetConfig, DatasetStats, generate_dataset, iter_rows, render_rows
from .schema import (
    AGENT_COLUMNS,
    CLAIM_COLUMNS,
    POLICY_COLUMNS,
    PRODUCTS,
    REGIONS,
    SOURCE_COLUMNS,
    ProductSpec,
)

__all__ = [
    "AGENT_COLUMNS",
    "CLAIM_COLUMNS",
    "POLICY_COLUMNS",
    "PRODUCTS",
    "REGIONS",
    "SOURCE_COLUMNS",
    "DatasetConfig",
    "DatasetStats",
    "ProductSpec",
    "generate_dataset",
    "iter_rows",
    "render_rows",
]
