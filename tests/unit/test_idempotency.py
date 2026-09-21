"""Idempotency ledger: leases, duplicate suppression, and the conditional-create race."""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agent_reports.common.errors import ConfigError
from agent_reports.common.idempotency import (
    STATUS_DISPATCHING,
    STATUS_FAILED,
    STATUS_SENT,
    DispatchLedger,
    DispatchRecord,
)
from agent_reports.common.storage import LocalStorage, Zones

NOW = datetime(2026, 9, 20, 6, 0, tzinfo=UTC)
DATE = "2026-09-20"
AGENT = "AGT-000001"


@pytest.fixture
def ledger(tmp_path: Path) -> DispatchLedger:
    return DispatchLedger(LocalStorage(tmp_path / "processed"))


class TestClaims:
    def test_first_claim_wins(self, ledger: DispatchLedger) -> None:
        result = ledger.claim(DATE, AGENT, recipient="agt-000001@example.com", now=NOW)
        assert result.claimed is True
        assert result.reason == "new"
        assert result.record.status == STATUS_DISPATCHING
        assert result.record.attempts == 1
        assert result.record.recipient == "agt-000001@example.com"
        assert result.record.lease_expires_at is not None

    def test_second_claim_is_suppressed_while_the_lease_is_live(
        self, ledger: DispatchLedger
    ) -> None:
        ledger.claim(DATE, AGENT, now=NOW)
        second = ledger.claim(DATE, AGENT, now=NOW + timedelta(seconds=5))
        assert second.claimed is False
        assert second.reason == "in_flight"
        assert second.duplicate_suppressed is True

    def test_sent_reports_are_never_claimed_again(self, ledger: DispatchLedger) -> None:
        claim = ledger.claim(DATE, AGENT, now=NOW)
        ledger.mark_sent(
            DATE,
            AGENT,
            ses_message_id="ses-1",
            recipient="agt-000001@example.com",
            now=NOW + timedelta(seconds=1),
            record=claim.record,
        )
        again = ledger.claim(DATE, AGENT, now=NOW + timedelta(days=1))
        assert again.claimed is False
        assert again.reason == "already_sent"
        assert again.record.status == STATUS_SENT
        assert again.record.ses_message_id == "ses-1"
        assert again.record.sent_at is not None

    def test_expired_lease_allows_a_retry(self, ledger: DispatchLedger) -> None:
        ledger.claim(DATE, AGENT, now=NOW, lease_seconds=60)
        later = NOW + timedelta(seconds=61)
        retry = ledger.claim(DATE, AGENT, now=later)
        assert retry.claimed is True
        assert retry.reason == "stale_lease"
        assert retry.record.attempts == 2
        assert retry.record.lease_expires_at == (later + timedelta(seconds=900)).isoformat()

    def test_retryable_failure_allows_a_retry(self, ledger: DispatchLedger) -> None:
        claim = ledger.claim(DATE, AGENT, now=NOW)
        ledger.mark_failed(
            DATE,
            AGENT,
            error_code="MissingReportError",
            error_message="report not in S3",
            now=NOW + timedelta(seconds=1),
            record=claim.record,
        )
        retry = ledger.claim(DATE, AGENT, now=NOW + timedelta(seconds=2))
        assert retry.claimed is True
        assert retry.reason == "retry"
        assert retry.record.attempts == 2

    def test_permanent_failure_stops_the_day(self, ledger: DispatchLedger) -> None:
        claim = ledger.claim(DATE, AGENT, now=NOW)
        ledger.mark_failed(
            DATE,
            AGENT,
            error_code="InvalidMessageError",
            error_message="bad recipient",
            now=NOW + timedelta(seconds=1),
            record=claim.record,
        )
        blocked = ledger.claim(DATE, AGENT, now=NOW + timedelta(days=1))
        assert blocked.claimed is False
        assert blocked.reason == "permanent_failure"
        assert blocked.record.status == STATUS_FAILED

    def test_mark_sent_requires_a_prior_claim(self, ledger: DispatchLedger) -> None:
        with pytest.raises(ConfigError, match="before it was claimed"):
            ledger.mark_sent(DATE, AGENT, ses_message_id="x", recipient="y@example.com", now=NOW)

    def test_history_records_every_transition(self, ledger: DispatchLedger) -> None:
        claim = ledger.claim(DATE, AGENT, now=NOW)
        record = ledger.mark_sent(
            DATE,
            AGENT,
            ses_message_id="ses-9",
            recipient="agt-000001@example.com",
            now=NOW + timedelta(seconds=2),
            record=claim.record,
        )
        assert [entry["event"] for entry in record.history] == ["claimed", "sent"]
        assert record.history[-1]["ses_message_id"] == "ses-9"


class TestStorage:
    def test_marker_lives_at_the_documented_key(
        self, ledger: DispatchLedger, tmp_path: Path
    ) -> None:
        ledger.claim(DATE, AGENT, now=NOW)
        assert (
            tmp_path / "processed" / "state" / "dispatch" / f"dt={DATE}" / f"agent_id={AGENT}.json"
        ).exists()

    def test_marker_key_matches_the_shared_convention(self, ledger: DispatchLedger) -> None:
        from agent_reports.common import keys

        assert ledger.key_for(DATE, AGENT) == keys.dispatch_marker_key(DATE, AGENT)
        assert ledger.prefix_for(DATE) == f"state/dispatch/dt={DATE}/"

    def test_marker_key_is_validated(self, ledger: DispatchLedger) -> None:
        with pytest.raises(ValueError, match="agent_id must look like"):
            ledger.key_for(DATE, "../../etc/passwd")

    def test_read_returns_none_when_absent(self, ledger: DispatchLedger) -> None:
        assert ledger.read(DATE, "AGT-000009") is None

    def test_sent_agents_lists_only_delivered_reports(self, ledger: DispatchLedger) -> None:
        first = ledger.claim(DATE, AGENT, now=NOW)
        ledger.mark_sent(
            DATE,
            AGENT,
            ses_message_id="s1",
            recipient="a@example.com",
            now=NOW,
            record=first.record,
        )
        ledger.claim(DATE, "AGT-000002", now=NOW)
        assert ledger.sent_agents(DATE) == [AGENT]

    def test_prefix_for_rejects_a_bad_date(self, ledger: DispatchLedger) -> None:
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            ledger.prefix_for("20-09-2026")


class TestRecordSerialisation:
    def test_round_trip(self) -> None:
        record = DispatchRecord(
            report_date=DATE,
            agent_id=AGENT,
            status=STATUS_SENT,
            attempts=2,
            created_at=NOW.isoformat(),
            updated_at=NOW.isoformat(),
            sent_at=NOW.isoformat(),
            ses_message_id="ses-3",
            history=[{"at": NOW.isoformat(), "event": "claimed"}],
        )
        assert DispatchRecord.from_json(record.to_json()) == record

    def test_unknown_fields_are_rejected(self) -> None:
        with pytest.raises(ConfigError, match="unexpected fields"):
            DispatchRecord.from_json('{"report_date":"x","agent_id":"y","status":"sent","bogus":1}')

    def test_non_object_is_rejected(self) -> None:
        with pytest.raises(ConfigError, match="JSON object"):
            DispatchRecord.from_json("[1, 2, 3]")

    def test_lease_active_only_while_dispatching(self) -> None:
        record = DispatchRecord(
            report_date=DATE,
            agent_id=AGENT,
            status=STATUS_SENT,
            lease_expires_at=(NOW + timedelta(hours=1)).isoformat(),
        )
        assert record.lease_active(NOW) is False

    def test_naive_now_is_rejected(self, ledger: DispatchLedger) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            ledger.claim(DATE, AGENT, now=datetime(2026, 9, 20, 6, 0))


class TestConditionalCreate:
    def test_conditional_create_is_atomic_for_racing_writers(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        store.put_bytes("k", b"first", if_none_match=True)
        with pytest.raises(FileExistsError):
            store.put_bytes("k", b"second", if_none_match=True)
        assert store.get_bytes("k") == b"first"

    def test_two_threads_cannot_both_claim(self, tmp_path: Path) -> None:
        ledger = DispatchLedger(LocalStorage(tmp_path / "processed"))
        results: list[bool] = []
        barrier = threading.Barrier(2)

        def attempt() -> None:
            barrier.wait()
            results.append(ledger.claim(DATE, AGENT, now=NOW).claimed)

        threads = [threading.Thread(target=attempt) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert sorted(results) == [False, True]

    def test_s3_conditional_create_raises_precondition_failed(self, aws: object) -> None:
        import boto3

        from agent_reports.common.storage import S3Storage

        store = S3Storage(
            boto3.client("s3", region_name="us-east-1"), bucket="agent-reports-processed"
        )
        ledger = DispatchLedger(store)
        first = ledger.claim(DATE, AGENT, now=NOW)
        assert first.claimed is True
        # A racing writer must lose the conditional create, not overwrite the lease.
        second = ledger.claim(DATE, AGENT, now=NOW)
        assert second.claimed is False
        assert second.reason == "in_flight"

    def test_s3_ledger_survives_a_fresh_reader(self, aws: object) -> None:
        import boto3

        from agent_reports.common.storage import S3Storage

        store = S3Storage(
            boto3.client("s3", region_name="us-east-1"), bucket="agent-reports-processed"
        )
        ledger = DispatchLedger(store)
        claim = ledger.claim(DATE, AGENT, now=NOW)
        ledger.mark_sent(
            DATE,
            AGENT,
            ses_message_id="ses-42",
            recipient="agt-000001@example.com",
            now=NOW,
            record=claim.record,
        )
        reread = DispatchLedger(store).read(DATE, AGENT)
        assert reread is not None
        assert reread.status == STATUS_SENT
        assert reread.ses_message_id == "ses-42"


def test_zones_fixture_is_usable(local_zones: Zones) -> None:
    """The filesystem zones fixture used by the Spark test must accept a marker write."""
    ledger = DispatchLedger(local_zones.processed)
    assert ledger.claim(DATE, AGENT, now=NOW).claimed is True
