"""Presign Lambda: API-Gateway-style handler that mints a fresh pre-signed report URL.

Why this exists at all: emails are a bad place to keep a secret. The link in the email expires; if
an agent opens the mail a week later, this endpoint issues a new link instead of forcing a re-send.

Authorisation is explicit and deny-by-default:

* the caller identity comes from the API Gateway JWT claims (``requestContext.authorizer.claims.sub``
  plus ``custom:role``) or, for the offline/moto path, from the ``X-Caller-Agent-Id`` /
  ``X-Caller-Role`` headers;
* an agent may read **their own** report (``sub == agent_id``);
* ``reports-admin`` / ``ops`` / ``finance`` roles may read any report;
* everything else is a 403, and a missing identity is a 401. There is no "default allow" branch.

Request:  ``GET /reports?agent_id=AGT-000123&date=2026-09-20``
Response: 200 ``{"url", "expires_at", "expires_in", "agent_id", "report_date", "report_key"}``
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from ..common.errors import AgentReportsError, AuthzDeniedError, InvalidMessageError
from ..common.keys import report_key, validate_agent_id, validate_report_date
from ..common.logging_utils import configure_logging, get_logger, log_event
from ..common.metrics import METRIC_NAMES, Metric, emit_emf
from ..common.settings import Settings, load_settings
from ..common.storage import Zones, open_zones

__all__ = ["PRIVILEGED_ROLES", "authorize", "caller_identity", "handler", "presign_report"]

_LOG = get_logger(__name__)

PRIVILEGED_ROLES = frozenset({"reports-admin", "ops", "finance"})
JSON_HEADERS = {"Content-Type": "application/json", "Cache-Control": "no-store"}


def caller_identity(event: Mapping[str, Any]) -> tuple[str, str]:
    """Return ``(caller_id, role)`` from API Gateway claims or fallback headers."""
    request_context = event.get("requestContext") or {}
    authorizer = request_context.get("authorizer") or {}
    claims = authorizer.get("claims") or authorizer.get("jwt", {}).get("claims") or {}
    caller_id = str(claims.get("sub", "") or "")
    role = str(claims.get("custom:role", "") or "")
    if not caller_id:
        headers = {str(k).lower(): v for k, v in (event.get("headers") or {}).items()}
        caller_id = str(headers.get("x-caller-agent-id", "") or "")
        role = role or str(headers.get("x-caller-role", "") or "")
    return caller_id, role


def authorize(caller_id: str, role: str, agent_id: str) -> None:
    """Raise unless this caller may read this agent's report."""
    if not caller_id:
        raise AuthzDeniedError("no caller identity on the request", context={"agent_id": agent_id})
    if role in PRIVILEGED_ROLES:
        return
    if caller_id == agent_id:
        return
    raise AuthzDeniedError(
        "caller may not read this agent's report",
        context={"caller_id": caller_id, "agent_id": agent_id, "role": role or "none"},
    )


def presign_report(
    settings: Settings,
    *,
    agent_id: str,
    report_date: str,
    zones: Zones,
) -> dict[str, Any]:
    """Mint a pre-signed URL for an existing report (raises if it is missing)."""
    key = report_key(report_date, agent_id)
    if not zones.reports.exists(key):
        raise InvalidMessageError(
            "no report for that agent and date",
            context={"agent_id": agent_id, "report_date": report_date},
        )
    expires_in = settings.presign_ttl_seconds
    url = zones.reports.presign_get(
        key, expires_in=expires_in, filename=f"{agent_id}-{report_date}.csv"
    )
    expires_at = datetime.now(tz=UTC) + timedelta(seconds=expires_in)
    return {
        "url": url,
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        "expires_in": expires_in,
        "agent_id": agent_id,
        "report_date": report_date,
        "report_key": key,
    }


def _response(status: int, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": dict(JSON_HEADERS),
        "body": json.dumps(payload, separators=(",", ":")),
    }


def handler(event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
    """API Gateway proxy-integration entry point."""
    configure_logging()
    settings = load_settings()
    params = event.get("queryStringParameters") or {}
    agent_id_raw = str(params.get("agent_id", "") or "")
    date_raw = str(params.get("date", "") or "")
    caller_id, role = caller_identity(event)

    try:
        agent_id = validate_agent_id(agent_id_raw)
        report_date = validate_report_date(date_raw)
    except ValueError as exc:
        log_event(_LOG, "presign_bad_request", level=30, error=str(exc))
        return _response(400, {"error": "invalid_request", "detail": str(exc)})

    try:
        authorize(caller_id, role, agent_id)
    except AuthzDeniedError as exc:
        emit_emf(
            [Metric(METRIC_NAMES["presign_denied"], 1.0)],
            {"Service": "presign"},
        )
        log_event(_LOG, "presign_denied", level=30, **exc.as_log_fields())
        status = 401 if not caller_id else 403
        return _response(status, {"error": "forbidden" if caller_id else "unauthenticated"})

    try:
        payload = presign_report(
            settings, agent_id=agent_id, report_date=report_date, zones=open_zones(settings)
        )
    except AgentReportsError as exc:
        log_event(_LOG, "presign_failed", level=30, **exc.as_log_fields())
        return _response(404, {"error": "not_found", "detail": exc.message})

    emit_emf(
        [
            Metric(METRIC_NAMES["presign_issued"], 1.0),
            Metric(METRIC_NAMES["report_age_seconds"], 0.0, unit="Seconds"),
        ],
        {"Service": "presign"},
    )
    log_event(
        _LOG,
        "presign_issued",
        agent_id=agent_id,
        report_date=report_date,
        caller_id=caller_id,
        role=role or "agent",
        expires_in=payload["expires_in"],
    )
    return _response(200, payload)
