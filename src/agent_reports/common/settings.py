"""Environment-driven configuration.

Every value has a safe default so the pipeline imports cleanly in tests, but production paths call
:meth:`Settings.validate` (or :func:`load_settings`) which fails fast with a single aggregated
message - a Lambda that is missing its queue URL should die at cold start, not mid fan-out.

Environment variables use the ``AGENT_REPORTS_`` prefix (see ``.env.example``).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

from .errors import ConfigError

__all__ = [
    "ENV_PREFIX",
    "S3_MAX_PRESIGN_SECONDS",
    "Settings",
    "load_settings",
]

ENV_PREFIX = "AGENT_REPORTS_"

#: S3 SigV4 pre-signed URLs cannot outlive 7 days.
S3_MAX_PRESIGN_SECONDS = 604_800

_BOOL_TRUE = frozenset({"1", "true", "yes", "on"})
_BOOL_FALSE = frozenset({"0", "false", "no", "off", ""})


@dataclass(frozen=True)
class Settings:
    """Resolved runtime configuration."""

    region: str = "us-east-1"
    raw_bucket: str = "agent-reports-raw"
    processed_bucket: str = "agent-reports-processed"
    reports_bucket: str = "agent-reports-out"
    agent_queue_url: str = "https://sqs.us-east-1.amazonaws.com/000000000000/agent-reports-fanout"
    dlq_url: str = "https://sqs.us-east-1.amazonaws.com/000000000000/agent-reports-fanout-dlq"
    ses_sender: str = "reports@example.com"
    ses_configuration_set: str = "agent-reports"
    presign_ttl_seconds: int = 3600
    max_report_bytes: int = 2_000_000
    sqs_batch_size: int = 10
    log_level: str = "INFO"
    endpoint_url: str | None = None
    emr_row_threshold: int = 5_000_000
    report_date: str | None = None
    reports_prefix: str = "reports"
    state_prefix: str = "state"
    chunker_max_policies: int = 2_000_000
    chunker_shards: int = 1
    local_root: str | None = None
    #: DEV ONLY, and off unless explicitly set. When true the presign handler accepts the
    #: ``X-Caller-Agent-Id`` / ``X-Caller-Role`` request headers as the caller identity. That is a
    #: *spoofable* identity - any caller can claim to be any agent or ``reports-admin`` - so it is
    #: only meaningful for the offline/local path where no API Gateway JWT authorizer exists. The
    #: deployed stack never sets it; see ``infra/terraform/main.tf`` and docs/RUNBOOK.md §6.
    allow_caller_header_fallback: bool = False

    # ------------------------------------------------------------------ loading
    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Build settings from environment variables (defaults win for unset keys)."""
        source = os.environ if env is None else env
        values: dict[str, Any] = {}
        for field in fields(cls):
            name = field.name
            raw = source.get(ENV_PREFIX + name.upper())
            if raw is None:
                continue
            if raw == "" and _is_optional(field.type):
                values[name] = None
                continue
            if raw == "":
                # Empty string for a non-optional field means "keep the default".
                continue
            values[name] = _coerce(name, raw, field.type)
        return cls(**values)

    # ---------------------------------------------------------------- validation
    def validate(self) -> Settings:
        """Return self if the configuration is usable, else raise :class:`ConfigError`."""
        problems: list[str] = []

        for bucket_field in ("raw_bucket", "processed_bucket", "reports_bucket"):
            bucket = getattr(self, bucket_field)
            if not bucket or len(bucket) < 3 or len(bucket) > 63:
                problems.append(
                    f"{bucket_field} must be a 3-63 character S3 bucket name (got {bucket!r})"
                )

        if self.sqs_batch_size < 1 or self.sqs_batch_size > 10:
            problems.append(f"sqs_batch_size must be between 1 and 10 (got {self.sqs_batch_size})")
        if self.presign_ttl_seconds < 60:
            problems.append(
                f"presign_ttl_seconds must be at least 60 (got {self.presign_ttl_seconds})"
            )
        if self.presign_ttl_seconds > S3_MAX_PRESIGN_SECONDS:
            problems.append(
                f"presign_ttl_seconds must not exceed {S3_MAX_PRESIGN_SECONDS} "
                f"(S3 hard limit) - got {self.presign_ttl_seconds}"
            )
        if self.max_report_bytes < 1024:
            problems.append(f"max_report_bytes must be >= 1024 (got {self.max_report_bytes})")
        if "@" not in self.ses_sender:
            problems.append(f"ses_sender must look like an email address (got {self.ses_sender!r})")
        if not self.region:
            problems.append("region must be set")

        if problems:
            raise ConfigError(
                "invalid configuration: " + "; ".join(problems),
                context={"problems": len(problems)},
            )

        if self.endpoint_url and not self.endpoint_url.startswith(("http://", "https://")):
            raise ConfigError(
                "endpoint_url must be an http(s) URL when set",
                context={"endpoint_url": self.endpoint_url},
            )

        return self

    # ------------------------------------------------------------------ helpers
    def require_queue(self) -> str:
        """Queue URL, validated - the orchestrator cannot run without it."""
        if not self.agent_queue_url.startswith("https://sqs."):
            raise ConfigError(
                "agent_queue_url is not an SQS URL",
                context={"agent_queue_url": self.agent_queue_url},
            )
        return self.agent_queue_url

    def report_key_prefix(self) -> str:
        return self.reports_prefix.strip("/") + "/"

    def state_key_prefix(self) -> str:
        return self.state_prefix.strip("/") + "/"

    def zone_uris(self) -> dict[str, str]:
        """Where each zone lives: real S3 buckets, or local directories in local mode.

        ``AGENT_REPORTS_LOCAL_ROOT`` switches the whole pipeline to the filesystem
        (``<root>/raw``, ``<root>/processed``, ``<root>/reports``) so the demo runs without moto and
        without AWS credentials. Pre-signed links are impossible in that mode - the dispatcher says
        so explicitly instead of pretending (see :meth:`LocalStorage.presign_get`).
        """
        if self.local_root:
            root = self.local_root.rstrip("/\\")
            return {
                "raw": f"{root}/raw",
                "processed": f"{root}/processed",
                "reports": f"{root}/reports",
            }
        return {
            "raw": f"s3://{self.raw_bucket}",
            "processed": f"s3://{self.processed_bucket}",
            "reports": f"s3://{self.reports_bucket}",
        }

    def redacted(self) -> dict[str, Any]:
        """Config safe to log (there are no credentials here by design)."""
        return {f.name: getattr(self, f.name) for f in fields(self)}


def load_settings(env: Mapping[str, str] | None = None, *, validate: bool = True) -> Settings:
    """Convenience wrapper used by handlers and scripts."""
    settings = Settings.from_env(env)
    return settings.validate() if validate else settings


def _is_optional(type_hint: Any) -> bool:
    return "None" in str(type_hint)


def _coerce(name: str, raw: str, type_hint: Any) -> Any:
    text = str(type_hint)
    if text in ("str", "str | None"):
        return raw
    if text in ("int", "int | None"):
        try:
            return int(raw)
        except ValueError as exc:
            raise ConfigError(
                f"{ENV_PREFIX}{name.upper()} must be an integer",
                context={"value": raw},
            ) from exc
    if text == "bool":
        lowered = raw.strip().lower()
        if lowered in _BOOL_TRUE:
            return True
        if lowered in _BOOL_FALSE:
            return False
        raise ConfigError(
            f"{ENV_PREFIX}{name.upper()} must be a boolean",
            context={"value": raw},
        )
    return raw
