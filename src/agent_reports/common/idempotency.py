"""Idempotency ledger: an agent+date report is never emailed twice.

One marker object per ``(report_date, agent_id)`` under
``state/dispatch/dt=YYYY-MM-DD/agent_id=AGT-000123.json`` records what happened. The record is a
short state machine::

    (absent) --claim--> dispatching --mark_sent--> sent        (terminal: never sent again)
                              |
                              +--(retryable failure)--> failed --claim--> dispatching ...
                              +--(permanent failure)--> failed (terminal for the day)

Two writers must never both email the same agent:

* the first write of a fresh marker uses a conditional create (``IfNoneMatch: *`` on S3,
  ``O_EXCL`` locally) so a race is decided by the store, not by application timing;
* a ``dispatching`` marker is a **lease**. If a Lambda dies mid-send the lease expires
  (``lease_seconds``, default 15 min) and the next delivery is allowed to retry - a stale lease is
  reported as ``stale_lease`` so the retry is visible in metrics rather than silent.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

from botocore.exceptions import ClientError

from . import keys
from .errors import AgentReportsError, ConfigError
from .storage import Storage

__all__ = [
    "STATUS_DISPATCHING",
    "STATUS_FAILED",
    "STATUS_SENT",
    "ClaimResult",
    "DispatchLedger",
    "DispatchRecord",
]

STATUS_DISPATCHING = "dispatching"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"

DEFAULT_LEASE_SECONDS = 900


@dataclass(frozen=True)
class DispatchRecord:
    """Serialised state of one agent's delivery attempt(s)."""

    report_date: str
    agent_id: str
    status: str
    attempts: int = 0
    created_at: str = ""
    updated_at: str = ""
    lease_expires_at: str | None = None
    sent_at: str | None = None
    ses_message_id: str | None = None
    last_error_code: str | None = None
    recipient: str | None = None
    history: list[dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str) -> DispatchRecord:
        raw = json.loads(text)
        if not isinstance(raw, dict):
            raise ConfigError("dispatch marker must be a JSON object")
        known = {f for f in cls.__dataclass_fields__}
        unknown = set(raw) - known
        if unknown:
            raise ConfigError(
                "dispatch marker has unexpected fields",
                context={"fields": sorted(unknown)},
            )
        return cls(**raw)

    @property
    def is_sent(self) -> bool:
        return self.status == STATUS_SENT

    def lease_active(self, now: datetime) -> bool:
        if self.status != STATUS_DISPATCHING or not self.lease_expires_at:
            return False
        return datetime.fromisoformat(self.lease_expires_at) > now

    def retryable_after(self, now: datetime) -> bool:
        """Should a new attempt be allowed right now?"""
        if self.status == STATUS_SENT:
            return False
        if self.status == STATUS_DISPATCHING:
            return not self.lease_active(now)
        # STATUS_FAILED: retry only when the failure was classified retryable.
        return not (
            self.last_error_code is not None
            and self.last_error_code in _PERMANENT_REASONS
        )


@dataclass(frozen=True)
class ClaimResult:
    """Outcome of asking the ledger for the right to send."""

    claimed: bool
    reason: str
    record: DispatchRecord

    @property
    def duplicate_suppressed(self) -> bool:
        return not self.claimed and self.reason in ("already_sent", "in_flight")


_PERMANENT_REASONS = frozenset(
    {
        "InvalidMessageError",
        "AuthzDeniedError",
        "PermanentError",
        "ConfigError",
        "MessageRejected",
        "AccountSendingPaused",
    }
)


class DispatchLedger:
    """Read/write access to the dispatch markers."""

    def __init__(self, storage: Storage, *, state_prefix: str = "state") -> None:
        self._storage = storage
        self._state_prefix = state_prefix.strip("/")

    # ------------------------------------------------------------------ keys
    def key_for(self, report_date: str, agent_id: str) -> str:
        return f"{self._state_prefix}/{keys.dispatch_marker_key(report_date, agent_id)}"

    def prefix_for(self, report_date: str) -> str:
        keys.validate_report_date(report_date)
        return f"{self._state_prefix}/state/dispatch/dt={report_date}/"

    # ----------------------------------------------------------------- I/O
    def read(self, report_date: str, agent_id: str) -> DispatchRecord | None:
        try:
            raw = self._storage.get_bytes(self.key_for(report_date, agent_id))
        except FileNotFoundError:
            return None
        except ClientError as exc:
            if _code(exc) in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return DispatchRecord.from_json(raw.decode("utf-8"))

    def claim(
        self,
        report_date: str,
        agent_id: str,
        *,
        recipient: str | None = None,
        now: datetime | None = None,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
    ) -> ClaimResult:
        """Try to take ownership of sending this agent's report."""
        moment = _aware(now)
        existing = self.read(report_date, agent_id)

        if existing is None:
            record = DispatchRecord(
                report_date=report_date,
                agent_id=agent_id,
                status=STATUS_DISPATCHING,
                attempts=1,
                created_at=moment.isoformat(),
                updated_at=moment.isoformat(),
                lease_expires_at=(moment + timedelta(seconds=lease_seconds)).isoformat(),
                recipient=recipient,
                history=[{"at": moment.isoformat(), "event": "claimed", "reason": "new"}],
            )
            if self._create_only(record):
                return ClaimResult(claimed=True, reason="new", record=record)
            # Lost a race with a concurrent delivery: re-read and fall through to the checks below.
            existing = self.read(report_date, agent_id)
            if existing is None:  # pragma: no cover - only reachable with a broken store
                raise ConfigError("dispatch marker vanished after a conditional-create conflict")

        if existing.status == STATUS_SENT:
            return ClaimResult(claimed=False, reason="already_sent", record=existing)
        if existing.lease_active(moment):
            return ClaimResult(claimed=False, reason="in_flight", record=existing)
        if existing.status == STATUS_FAILED and not existing.retryable_after(moment):
            return ClaimResult(claimed=False, reason="permanent_failure", record=existing)

        reason = "stale_lease" if existing.status == STATUS_DISPATCHING else "retry"
        updated = _replace(
            existing,
            status=STATUS_DISPATCHING,
            attempts=existing.attempts + 1,
            updated_at=moment.isoformat(),
            lease_expires_at=(moment + timedelta(seconds=lease_seconds)).isoformat(),
            history=[
                *existing.history,
                {"at": moment.isoformat(), "event": "claimed", "reason": reason},
            ],
        )
        self._write(updated, if_none_match=False)
        return ClaimResult(claimed=True, reason=reason, record=updated)

    def mark_sent(
        self,
        report_date: str,
        agent_id: str,
        *,
        ses_message_id: str,
        recipient: str,
        now: datetime | None = None,
        record: DispatchRecord | None = None,
    ) -> DispatchRecord:
        moment = _aware(now)
        base = record or self.read(report_date, agent_id)
        if base is None:
            raise ConfigError("cannot mark a report sent before it was claimed")
        updated = _replace(
            base,
            status=STATUS_SENT,
            sent_at=moment.isoformat(),
            ses_message_id=ses_message_id,
            recipient=recipient,
            lease_expires_at=None,
            last_error_code=None,
            updated_at=moment.isoformat(),
            history=[
                *base.history,
                {"at": moment.isoformat(), "event": "sent", "ses_message_id": ses_message_id},
            ],
        )
        self._write(updated)
        return updated

    def mark_failed(
        self,
        report_date: str,
        agent_id: str,
        *,
        error_code: str,
        error_message: str,
        now: datetime | None = None,
        record: DispatchRecord | None = None,
    ) -> DispatchRecord:
        """Record a failed attempt. ``error_code`` decides whether a retry is allowed."""
        moment = _aware(now)
        base = record or self.read(report_date, agent_id)
        if base is None:
            raise ConfigError("cannot mark a report failed before it was claimed")
        updated = _replace(
            base,
            status=STATUS_FAILED,
            last_error_code=error_code,
            lease_expires_at=None,
            updated_at=moment.isoformat(),
            history=[
                *base.history,
                {
                    "at": moment.isoformat(),
                    "event": "failed",
                    "error_code": error_code,
                    "error_message": error_message,
                },
            ],
        )
        self._write(updated)
        return updated

    def sent_agents(self, report_date: str) -> list[str]:
        """Agent ids already marked sent for a date - used by the run manifest."""
        prefix = self.prefix_for(report_date)
        found: list[str] = []
        for key in self._storage.list_keys(prefix):
            record = self.read(report_date, key.rsplit("agent_id=", 1)[-1].removesuffix(".json"))
            if record is not None and record.is_sent:
                found.append(record.agent_id)
        return sorted(set(found))

    # --------------------------------------------------------------- internals
    def _create_only(self, record: DispatchRecord) -> bool:
        """False when a marker already exists (lost the race)."""
        try:
            self._storage.put_bytes(
                self.key_for(record.report_date, record.agent_id),
                record.to_json().encode("utf-8"),
                content_type="application/json",
                if_none_match=True,
            )
        except FileExistsError:
            return False
        except ClientError as exc:
            if _code(exc) in ("412", "PreconditionFailed", "ConditionalRequestConflict"):
                return False
            raise
        except AgentReportsError:
            return False
        return True

    def _write(self, record: DispatchRecord, *, if_none_match: bool = False) -> None:
        self._storage.put_bytes(
            self.key_for(record.report_date, record.agent_id),
            record.to_json().encode("utf-8"),
            content_type="application/json",
            if_none_match=if_none_match,
        )


def _replace(record: DispatchRecord, **changes: Any) -> DispatchRecord:
    data: Mapping[str, Any] = {**asdict(record), **changes}
    return DispatchRecord(**data)


def _aware(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(tz=UTC)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return now


def _code(exc: ClientError) -> str:
    response = exc.response if isinstance(exc.response, dict) else {}
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    code = error.get("Code") if isinstance(error, dict) else None
    if code:
        return str(code)
    meta = response.get("ResponseMetadata", {}) if isinstance(response, dict) else {}
    status = meta.get("HTTPStatusCode") if isinstance(meta, dict) else None
    return str(status) if status is not None else "Unknown"
