"""Shared building blocks: config, key conventions, storage, retry, idempotency, metrics, errors."""

from __future__ import annotations

from .errors import (
    AgentReportsError,
    AuthzDeniedError,
    ConfigError,
    DependencyError,
    InvalidMessageError,
    MissingReportError,
    PermanentError,
    ReportNotFoundError,
    RetryableError,
    ThrottlingError,
    TransientError,
    classify,
    is_retryable,
)
from .idempotency import (
    STATUS_DISPATCHING,
    STATUS_FAILED,
    STATUS_SENT,
    ClaimResult,
    DispatchLedger,
    DispatchRecord,
)
from .keys import (
    report_key,
    report_prefix_for,
    raw_key,
    raw_prefix,
    validate_agent_id,
    validate_report_date,
)
from .logging_utils import configure_logging, get_logger, log_emf, log_event
from .metrics import DEFAULT_NAMESPACE, METRIC_NAMES, Metric, emit_emf, put_metric_data
from .retry import RetryPolicy, RetryStats, call_with_retry, compute_delay
from .settings import S3_MAX_PRESIGN_SECONDS, Settings, load_settings
from .storage import LocalStorage, S3Storage, Storage, open_store

__all__ = [
    "DEFAULT_NAMESPACE",
    "METRIC_NAMES",
    "S3_MAX_PRESIGN_SECONDS",
    "STATUS_DISPATCHING",
    "STATUS_FAILED",
    "STATUS_SENT",
    "AgentReportsError",
    "AuthzDeniedError",
    "ClaimResult",
    "ConfigError",
    "DependencyError",
    "DispatchLedger",
    "DispatchRecord",
    "InvalidMessageError",
    "LocalStorage",
    "Metric",
    "MissingReportError",
    "PermanentError",
    "ReportNotFoundError",
    "RetryPolicy",
    "RetryStats",
    "RetryableError",
    "S3Storage",
    "Settings",
    "Storage",
    "ThrottlingError",
    "TransientError",
    "call_with_retry",
    "classify",
    "compute_delay",
    "configure_logging",
    "emit_emf",
    "get_logger",
    "is_retryable",
    "load_settings",
    "log_emf",
    "log_event",
    "open_store",
    "put_metric_data",
    "report_key",
    "report_prefix_for",
    "raw_key",
    "raw_prefix",
    "validate_agent_id",
    "validate_report_date",
]
