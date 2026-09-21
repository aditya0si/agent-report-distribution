"""boto3 client factory.

Two rules this module exists to enforce:

1. Clients are created lazily and cached per (service, region, endpoint) - Lambda containers are
   reused, so re-creating a client on every invocation wastes ~50 ms of billable time.
2. Retry behaviour is configured once, explicitly (``standard`` mode, adaptive is opt-in per call
   site), instead of relying on whatever the SDK defaults happen to be that week.

Nothing here runs at import time.
"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.config import Config

from .settings import Settings

__all__ = [
    "CLIENT_CONFIG",
    "client",
    "cloudwatch_client",
    "logs_client",
    "reset_client_cache",
    "s3_client",
    "ses_client",
    "sqs_client",
]

CLIENT_CONFIG = Config(
    retries={"max_attempts": 5, "mode": "standard"},
    connect_timeout=5,
    read_timeout=20,
    # SigV4 everywhere: SigV2 pre-signed URLs are deprecated, and the 7-day expiry limit enforced by
    # Settings.validate() is a SigV4 rule.
    signature_version="s3v4",
    user_agent_extra="agent-reports/1.0.0",
)

_CLIENTS: dict[tuple[str, str, str | None], Any] = {}


def client(
    service: str,
    settings: Settings | None = None,
    *,
    region: str | None = None,
    endpoint_url: str | None = None,
) -> Any:
    """Return a cached boto3 client for *service*."""
    resolved_region = region or (settings.region if settings else None) or "us-east-1"
    resolved_endpoint = (
        endpoint_url if endpoint_url is not None else (settings.endpoint_url if settings else None)
    )
    key = (service, resolved_region, resolved_endpoint)
    existing = _CLIENTS.get(key)
    if existing is not None:
        return existing
    created = boto3.client(
        service,
        region_name=resolved_region,
        endpoint_url=resolved_endpoint or None,
        config=CLIENT_CONFIG,
    )
    _CLIENTS[key] = created
    return created


def reset_client_cache() -> None:
    """Drop cached clients (used by tests that switch mock endpoints or regions)."""
    _CLIENTS.clear()


def s3_client(settings: Settings) -> Any:
    return client("s3", settings)


def sqs_client(settings: Settings) -> Any:
    return client("sqs", settings)


def ses_client(settings: Settings) -> Any:
    return client("ses", settings)


def cloudwatch_client(settings: Settings) -> Any:
    return client("cloudwatch", settings)


def logs_client(settings: Settings) -> Any:
    return client("logs", settings)
