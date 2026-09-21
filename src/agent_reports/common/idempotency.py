"""Idempotency ledger: an agent+date report is never emailed twice.

One marker object per ``(report_date, agent_id)`` under
``state/dispatch/dt=YYYY-MM-DD/agent_id=AGT-000123.json`` records what happened. The record is a
short state machine::

    (absent) --claim--> dispatching --mark_sent--> sent        (terminal: never sent again)
                              |
                              +--(retryable failure)--> failed --claim--> dispatching ...
                              +--(permanent failure)--> failed (terminal for the day)

Two writers must never both email the same agent. Three mechanisms enforce that, and all three are
**conditional writes** - a read-then-write would be decided by scheduling, not by the store:

* the first write of a fresh marker uses a conditional create (``IfNoneMatch: *`` on S3,
  ``O_EXCL``/``os.link`` locally);
* every *update* of an existing marker is a compare-and-set on the version (ETag) that was read
  (``IfMatch`` on S3, an exclusive lock + content hash locally). Two workers that both see a stale
  lease therefore cannot both claim it: one write succeeds and the other is re-evaluated against the
  winner's state, which is a live lease (``in_flight``). On the S3 path the compare-and-set is
  enforced **inside the process** by a per-key lock plus a re-read of the stored version, and
  **across processes** by ``If-Match`` - the offline backend (moto) compares the ETag and then writes
  without a lock, so it cannot be the thing that decides a threaded race (see
  :mod:`agent_reports.common.storage`);
* each claim mints a ``lease_id``, and a terminal ``sent`` record is never rewritten. A late
  ``mark_failed`` from a worker whose send already succeeded - or whose lease was taken over - cannot
  regress the marker, so the next delivery still sees ``already_sent``.

A ``dispatching`` marker is a **lease**. If a Lambda dies mid-send the lease expires and the next
delivery is allowed to retry - reported as ``stale_lease`` so the retry is visible in metrics rather
than silent. The lease (``DEFAULT_LEASE_SECONDS``) is deliberately shorter than
``visibility_timeout x maxReceiveCount`` (300 s x 3 = 900 s in ``infra/terraform/sqs.tf``) so a
message whose worker died is re-claimed on its first redelivery instead of being dead-lettered.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from botocore.exceptions import ClientError

from . import keys
from .errors import AgentReportsError, ConfigError, DependencyError
from .keys import validate_agent_id, validate_report_date
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

#: Must be longer than the dispatcher's own Lambda timeout (120 s, ``infra/terraform/lambda.tf``)
#: and shorter than the SQS redelivery window (visibility timeout x maxReceiveCount), or a worker
#: that dies mid-send leaves a lease that outlives the message's retries.
DEFAULT_LEASE_SECONDS = 240

#: Bounded retries for the compare-and-set loops. A marker that changes five times under one caller
#: is contention, not progress, and the caller should come back through SQS.
MAX_CAS_ATTEMPTS = 5


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
    lease_id: str = ""
    sent_at: str | None = None
    ses_message_id: str | None = None
    last_error_code: str | None = None
    recipient: str | None = None
    history: list[dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str) -> DispatchRecord:
        try:
            raw = json.loads(text)
        except ValueError as exc:
            # A marker that cannot be parsed must be reported as a config problem with the key in
            # context, not as a bare JSONDecodeError three frames down.
            raise ConfigError(
                "dispatch marker is not valid JSON",
                context={"bytes": len(text), "prefix": text[:80]},
            ) from exc
        if not isinstance(raw, dict):
            raise ConfigError("dispatch marker must be a JSON object")
        known = set(cls.__dataclass_fields__)
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
        return not (self.last_error_code is not None and self.last_error_code in _PERMANENT_REASONS)


@dataclass(frozen=True)
class ClaimResult:
    """Outcome of asking the ledger for the right to send."""

    claimed: bool
    reason: str
    record: DispatchRecord

    @property
    def duplicate_suppressed(self) -> bool:
        return not self.claimed and self.reason in ("already_sent", "in_flight")

    @property
    def redeliver(self) -> bool:
        """True when the message must go back to SQS instead of being acknowledged.

        ``already_sent`` is the only suppression that is safe to acknowledge: the agent has the
        email. ``in_flight`` means *another worker may still be sending* - if that worker died, the
        message is the only thing that will ever deliver the report, so it is redelivered (and the
        lease is claimed as ``stale_lease`` once it expires).
        """
        return not self.claimed and self.reason in ("in_flight", "contended")


_PERMANENT_REASONS = frozenset(
    {
        "InvalidMessageError",
        "AuthzDeniedError",
        "PermanentError",
        "ConfigError",
        "MessageRejected",
    }
)


class DispatchLedger:
    """Read/write access to the dispatch markers."""

    def __init__(self, storage: Storage, *, state_prefix: str = "state") -> None:
        self._storage = storage
        self._state_prefix = state_prefix.strip("/")

    # ------------------------------------------------------------------ keys
    def key_for(self, report_date: str, agent_id: str) -> str:
        """Marker key: ``<state_prefix>/dispatch/dt=<date>/agent_id=<id>.json``.

        Kept identical to :func:`agent_reports.common.keys.dispatch_marker_key` for the default
        ``state`` prefix (asserted in ``tests/unit/test_idempotency.py``).
        """
        validate_report_date(report_date)
        validate_agent_id(agent_id)
        return f"{self._state_prefix}/dispatch/dt={report_date}/agent_id={agent_id}.json"

    def prefix_for(self, report_date: str) -> str:
        keys.validate_report_date(report_date)
        return f"{self._state_prefix}/dispatch/dt={report_date}/"

    # ----------------------------------------------------------------- I/O
    def read(self, report_date: str, agent_id: str) -> DispatchRecord | None:
        found = self.read_with_version(report_date, agent_id)
        return found[0] if found is not None else None

    def read_with_version(
        self, report_date: str, agent_id: str
    ) -> tuple[DispatchRecord, str] | None:
        """``(record, version)`` where *version* is the token a conditional write must present."""
        try:
            raw, version = self._storage.get_bytes_with_version(self.key_for(report_date, agent_id))
        except FileNotFoundError:
            return None
        except ClientError as exc:
            if _code(exc) in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return DispatchRecord.from_json(raw.decode("utf-8")), version

    def claim(
        self,
        report_date: str,
        agent_id: str,
        *,
        recipient: str | None = None,
        now: datetime | None = None,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
    ) -> ClaimResult:
        """Try to take ownership of sending this agent's report.

        Every write here is a compare-and-set against the version that was read, so two workers that
        read the same stale lease cannot both walk away believing they own the send: the loser's
        write is rejected by the store and it re-evaluates against the winner's fresh lease (which
        is ``in_flight``, i.e. "not you, not now").
        """
        moment = _aware(now)

        for _ in range(MAX_CAS_ATTEMPTS):
            current = self.read_with_version(report_date, agent_id)

            if current is None:
                record = DispatchRecord(
                    report_date=report_date,
                    agent_id=agent_id,
                    status=STATUS_DISPATCHING,
                    attempts=1,
                    created_at=moment.isoformat(),
                    updated_at=moment.isoformat(),
                    lease_expires_at=(moment + timedelta(seconds=lease_seconds)).isoformat(),
                    lease_id=_new_lease_id(),
                    recipient=recipient,
                    history=[{"at": moment.isoformat(), "event": "claimed", "reason": "new"}],
                )
                if self._create_only(record):
                    return ClaimResult(claimed=True, reason="new", record=record)
                # Lost a race with a concurrent delivery: re-read and re-decide.
                continue

            existing, version = current
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
                lease_id=_new_lease_id(),
                history=[
                    *existing.history,
                    {"at": moment.isoformat(), "event": "claimed", "reason": reason},
                ],
            )
            if self._write_if_version(updated, version):
                return ClaimResult(claimed=True, reason=reason, record=updated)

        current = self.read_with_version(report_date, agent_id)
        if current is None:  # pragma: no cover - only reachable with a broken store
            raise ConfigError("dispatch marker vanished under a contended claim")
        return ClaimResult(claimed=False, reason="contended", record=current[0])

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
        """Record a delivered email. ``sent`` is terminal and is never rewritten."""
        moment = _aware(now)

        for _ in range(MAX_CAS_ATTEMPTS):
            current = self.read_with_version(report_date, agent_id)
            if current is None:
                raise ConfigError("cannot mark a report sent before it was claimed")
            base, version = current
            if base.status == STATUS_SENT:
                # Already terminal. Rewriting it could only lose the SES message id that the
                # duplicate-suppression path is read for.
                return base
            if not _holds_lease(base, record):
                return base
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
            if self._write_if_version(updated, version):
                return updated

        raise DependencyError(
            "the dispatch marker kept changing while recording a send; the message will be redelivered",
            context={"agent_id": agent_id, "report_date": report_date},
        )

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
        """Record a failed attempt. ``error_code`` decides whether a retry is allowed.

        A late failure must never undo a send: if the stored marker is already ``sent`` - or another
        worker holds the lease now - this returns the current record and writes nothing. That is the
        difference between "the send failed" and "the send failed *before* somebody else succeeded".
        """
        moment = _aware(now)

        for _ in range(MAX_CAS_ATTEMPTS):
            current = self.read_with_version(report_date, agent_id)
            if current is None:
                raise ConfigError("cannot mark a report failed before it was claimed")
            base, version = current
            if base.status == STATUS_SENT or not _holds_lease(base, record):
                return base
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
            if self._write_if_version(updated, version):
                return updated

        raise DependencyError(
            "the dispatch marker kept changing while recording a failure; the message will be redelivered",
            context={"agent_id": agent_id, "report_date": report_date},
        )

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

    def _write_if_version(self, record: DispatchRecord, version: str) -> bool:
        """Compare-and-set: False when the marker moved since *version* was read."""
        return self._storage.put_bytes_if_version(
            self.key_for(record.report_date, record.agent_id),
            record.to_json().encode("utf-8"),
            version=version,
            content_type="application/json",
        )


def _holds_lease(current: DispatchRecord, claimed: DispatchRecord | None) -> bool:
    """Does the caller still own the marker?

    ``claimed`` is the record returned by :meth:`DispatchLedger.claim`. A marker that now carries a
    different ``lease_id`` belongs to another worker, so this caller must not write to it - writing
    is what used to let a late ``mark_failed`` regress a ``sent`` record.
    """
    if claimed is None:
        return True
    if not current.lease_id or not claimed.lease_id:
        return True  # legacy marker without a lease id: fall back to the version check alone
    return current.lease_id == claimed.lease_id


def _new_lease_id() -> str:
    return uuid.uuid4().hex


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
