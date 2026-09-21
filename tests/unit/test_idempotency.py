"""Idempotency ledger: leases, duplicate suppression, and the conditional-create race."""

from __future__ import annotations

import hashlib
import io
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from agent_reports.common.errors import ConfigError
from agent_reports.common.idempotency import (
    DEFAULT_LEASE_SECONDS,
    STATUS_DISPATCHING,
    STATUS_FAILED,
    STATUS_SENT,
    ClaimResult,
    DispatchLedger,
    DispatchRecord,
)
from agent_reports.common.storage import LocalStorage, S3Storage, Zones

NOW = datetime(2026, 9, 20, 6, 0, tzinfo=UTC)
DATE = "2026-09-20"
AGENT = "AGT-000001"
BUCKET = "agent-reports-processed"


@pytest.fixture
def ledger(tmp_path: Path) -> DispatchLedger:
    return DispatchLedger(LocalStorage(tmp_path / "processed"))


def _race(workers: int, attempt: Callable[[int], Any]) -> list[Any]:
    """Run *attempt(index)* in *workers* threads that all start at the same instant.

    Anything a worker raises is collected as the result (and shows up in the assertion message),
    because an exception inside a thread is otherwise invisible to pytest.
    """
    barrier = threading.Barrier(workers, timeout=30)
    results: list[Any] = []
    lock = threading.Lock()

    def run(index: int) -> None:
        barrier.wait()
        try:
            outcome: Any = attempt(index)
        except Exception as exc:  # collected, never swallowed: the assertion below reports it
            outcome = exc
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=run, args=(index,)) for index in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


class _ConditionalWritesIgnored:
    """An S3 client double that stores objects but never evaluates a conditional-write header.

    This is the shape the offline backend really has: on CI (run 35640872774) two of four workers won
    the same stale lease, because moto evaluates ``If-Match`` by comparing the ETag and *then* writing,
    with no lock in between. Pinning that here - instead of hoping the scheduler reproduces it - is
    what makes the in-process guarantee testable: the backend decides nothing, so the only thing that
    can make a conditional write exclusive is the guard inside :class:`S3Storage`.

    :meth:`sync_next_reads` pins the *interleaving* as well: every racing worker reads before any of
    them writes, which is the window the race needs (and the one Linux scheduling produced on CI).
    Without it the test would depend on how the threads happen to be scheduled - the reason the CI
    failure never reproduced on this machine.
    """

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self._guard = threading.Lock()
        self._reads_until_sync = 0
        self._read_barrier: threading.Barrier | None = None

    def sync_next_reads(self, count: int) -> None:
        """Rendezvous the next *count* readers: none of them proceeds until all have read."""
        with self._guard:
            self._reads_until_sync = count
            self._read_barrier = threading.Barrier(count, timeout=30)

    def _etag(self, key: str) -> str:
        digest = hashlib.sha256(self.objects[key]).hexdigest()
        return f'"{digest}"'

    # Parameter names follow the S3 API (PascalCase) because S3Storage calls them as keywords.
    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str | None = None,
        IfMatch: str | None = None,
        IfNoneMatch: str | None = None,
    ) -> dict[str, Any]:
        del ContentType, IfMatch, IfNoneMatch  # accepted and deliberately not evaluated
        with self._guard:
            self.objects[Key] = bytes(Body)
        return {"ETag": self._etag(Key)}

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        with self._guard:
            if Key not in self.objects:
                raise _not_found(Key)
            return {"ETag": self._etag(Key), "ContentLength": len(self.objects[Key])}

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        barrier = None
        with self._guard:
            if self._reads_until_sync > 0:
                self._reads_until_sync -= 1
                barrier = self._read_barrier
            payload = self.objects.get(Key)
            etag = self._etag(Key) if payload is not None else ""
        if barrier is not None:  # one-shot: a loser's re-read must not wait for anybody
            barrier.wait()
        if payload is None:
            raise _not_found(Key)
        return {"Body": io.BytesIO(payload), "ETag": etag}


def _not_found(key: str) -> ClientError:
    # Typed as Any: this is a synthetic error response for the double, not a full botocore response.
    response: Any = {
        "Error": {"Code": "NoSuchKey", "Message": f"no such object: {key}"},
        "ResponseMetadata": {"HTTPStatusCode": 404},
    }
    return ClientError(response, "GetObject")


def _header(value: object) -> str:
    """Header values arrive as bytes on a prepared request."""
    if value is None:
        return ""
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


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
        assert (
            retry.record.lease_expires_at
            == (later + timedelta(seconds=DEFAULT_LEASE_SECONDS)).isoformat()
        )

    def test_the_default_lease_outlives_the_lambda_but_not_the_sqs_retries(self) -> None:
        """The lease has to sit between two numbers, or a crashed worker loses the message.

        ``infra/terraform/lambda.tf`` gives the dispatcher a 120 s timeout, and
        ``infra/terraform/sqs.tf`` gives the queue a 300 s visibility timeout with
        ``maxReceiveCount`` 3. A lease shorter than the Lambda timeout could be stolen from a worker
        that is still running (two emails); a lease longer than the retry window means the message is
        dead-lettered before the lease ever expires (no email).
        """
        assert DEFAULT_LEASE_SECONDS > 120
        assert DEFAULT_LEASE_SECONDS < 300 * 3

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

    def test_compare_and_set_admits_exactly_one_writer(self, tmp_path: Path) -> None:
        """The primitive the ledger's stale-lease claim is built on.

        Every contender reads the same version, then tries to write against it: the store must let
        exactly one through, whichever order the threads happen to run in.
        """
        store = LocalStorage(tmp_path)
        store.put_bytes("marker", b"v1")
        _, version = store.get_bytes_with_version("marker")
        barrier = threading.Barrier(4)
        results: list[bool] = []
        lock = threading.Lock()

        def attempt(index: int) -> None:
            barrier.wait()
            wrote = store.put_bytes_if_version("marker", f"v2-{index}".encode(), version=version)
            with lock:
                results.append(wrote)

        threads = [threading.Thread(target=attempt, args=(index,)) for index in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert sorted(results) == [False, False, False, True]
        assert store.get_bytes("marker").startswith(b"v2-")

    def test_compare_and_set_rejects_a_stale_version(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        store.put_bytes("marker", b"v1")
        _, stale = store.get_bytes_with_version("marker")
        assert store.put_bytes_if_version("marker", b"v2", version=stale) is True
        assert store.put_bytes_if_version("marker", b"v3", version=stale) is False
        assert store.get_bytes("marker") == b"v2"

    def test_compare_and_set_refuses_a_missing_object(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        assert store.put_bytes_if_version("absent", b"x", version="whatever") is False

    def test_compare_and_set_leaves_no_transient_files(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        store.put_bytes("marker", b"v1")
        _, version = store.get_bytes_with_version("marker")
        assert store.put_bytes_if_version("marker", b"v2", version=version) is True
        assert [path.name for path in tmp_path.iterdir()] == ["marker"]
        assert store.list_keys() == ["marker"]

    def test_create_if_absent_never_publishes_a_partial_file(self, tmp_path: Path) -> None:
        """A racing reader must see either no file or the whole file - never an empty one."""
        store = LocalStorage(tmp_path)
        payload = b'{"status":"dispatching","attempts":1}'
        store.put_bytes("marker.json", payload, if_none_match=True)
        assert store.get_bytes("marker.json") == payload
        assert [path.name for path in tmp_path.iterdir()] == ["marker.json"]  # no temp left behind

    def test_marker_that_is_not_json_is_reported_as_a_config_error(self) -> None:
        from agent_reports.common.idempotency import DispatchRecord

        with pytest.raises(ConfigError, match="not valid JSON"):
            DispatchRecord.from_json("")

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


class TestConditionalWriteGuards:
    """What the S3 path guarantees *in this process*, and what only real S3 can guarantee.

    The reviewer's HIGH finding was that two workers could both claim the same stale lease, and the
    CI run that followed showed it is not enough to hand the decision to the backend: moto's
    ``If-Match`` check is a compare followed by a write, so two racing threads both got through
    (``[False, False, True, True]``). These tests pin the two halves separately:

    * the guard that is ours - a per-key lock plus a version re-read - admits exactly one writer even
      against a backend that ignores conditional headers entirely;
    * the guard that is S3's - the conditional headers themselves - is proven only to be *issued*,
      because no offline backend enforces it atomically.
    """

    def test_compare_and_set_admits_exactly_one_writer_without_backend_support(self) -> None:
        store = S3Storage(_ConditionalWritesIgnored(), bucket=BUCKET)  # type: ignore[arg-type]
        store.put_bytes("state/probe.json", b"v1")
        _, version = store.get_bytes_with_version("state/probe.json")

        results = _race(
            4,
            lambda index: store.put_bytes_if_version(
                "state/probe.json", f"v2-{index}".encode(), version=version
            ),
        )

        assert sorted(results) == [False, False, False, True], results
        assert store.get_bytes("state/probe.json").startswith(b"v2-")

    def test_conditional_create_admits_exactly_one_writer_without_backend_support(self) -> None:
        """Two workers creating the same fresh marker is the other way an agent gets two emails."""

        def attempt(index: int) -> bool:
            try:
                store.put_bytes("state/fresh.json", f"v{index}".encode(), if_none_match=True)
            except FileExistsError:
                return False
            return True

        store = S3Storage(_ConditionalWritesIgnored(), bucket=BUCKET)  # type: ignore[arg-type]
        results = _race(4, attempt)

        assert sorted(results) == [False, False, False, True], results
        assert store.get_bytes("state/fresh.json") in {b"v0", b"v1", b"v2", b"v3"}

    def test_a_stale_version_is_rejected_by_the_in_process_guard(self) -> None:
        """No backend support needed: the version is re-read inside the key lock."""
        store = S3Storage(_ConditionalWritesIgnored(), bucket=BUCKET)  # type: ignore[arg-type]
        store.put_bytes("state/probe.json", b"v1")
        _, stale = store.get_bytes_with_version("state/probe.json")

        assert store.put_bytes_if_version("state/probe.json", b"v2", version=stale) is True
        assert store.put_bytes_if_version("state/probe.json", b"v3", version=stale) is False
        assert store.get_bytes("state/probe.json") == b"v2"
        assert store.put_bytes_if_version("state/absent.json", b"x", version="whatever") is False

    def test_s3_put_object_requests_carry_the_conditional_headers(self, aws: object) -> None:
        """The cross-process guard is *requested*; S3's enforcement of it is unverifiable offline.

        Two Lambda *processes* are separated only by S3 evaluating ``If-Match`` server-side, and
        nothing offline can prove that evaluation happens: moto evaluates the same header as a
        compare followed by a write (on CI two of four workers still won the same lease), so it cannot
        stand in for S3's atomic decision. What this test does prove is the half that is ours to get
        right - the request really carries ``If-Match`` with the version that was read, and the
        create-if-absent request really carries ``If-None-Match: *``.
        """
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        store = S3Storage(client, bucket=BUCKET)
        sent: list[tuple[str, str, str]] = []

        def capture(request: Any, **kwargs: Any) -> None:
            headers = {name.lower(): value for name, value in request.headers.items()}
            sent.append(
                (
                    str(request.url),
                    _header(headers.get("if-match")),
                    _header(headers.get("if-none-match")),
                )
            )

        client.meta.events.register("before-send.s3.PutObject", capture)

        store.put_bytes("state/probe.json", b"v1")
        _, version = store.get_bytes_with_version("state/probe.json")
        assert store.put_bytes_if_version("state/probe.json", b"v2", version=version) is True
        store.put_bytes("state/fresh.json", b"v1", if_none_match=True)

        probe = [entry for entry in sent if entry[0].endswith("state/probe.json")]
        assert len(probe) == 2, sent
        assert probe[0][1] == "", "an unconditional write must not claim to be conditional"
        assert probe[1][1], "If-Match must carry the version that was read"
        # S3 ignores the surrounding quotes on an ETag, so compare with them stripped.
        assert probe[1][1].replace('"', "") == version.replace('"', "")

        fresh = [entry for entry in sent if entry[0].endswith("state/fresh.json")]
        assert [entry[2] for entry in fresh] == ["*"], sent


class TestStaleLeaseRace:
    """The reviewer's barrier-synchronised race, pinned.

    Before the compare-and-set, two workers that both read the same stale-lease marker both got
    ``claimed=True`` and both emailed the agent.
    """

    def test_only_one_worker_can_claim_a_stale_lease(self, tmp_path: Path) -> None:
        ledger = DispatchLedger(LocalStorage(tmp_path / "processed"))
        ledger.claim(DATE, AGENT, now=NOW, lease_seconds=1)
        moment = NOW + timedelta(seconds=2)  # the lease is now stale
        barrier = threading.Barrier(6)
        results: list[tuple[bool, str]] = []
        lock = threading.Lock()

        def attempt() -> None:
            barrier.wait()
            claim = ledger.claim(DATE, AGENT, now=moment)
            with lock:
                results.append((claim.claimed, claim.reason))

        threads = [threading.Thread(target=attempt) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        winners = [reason for claimed, reason in results if claimed]
        assert winners == ["stale_lease"], results
        losers = {reason for claimed, reason in results if not claimed}
        assert losers <= {"in_flight", "contended"}, results
        stored = ledger.read(DATE, AGENT)
        assert stored is not None
        assert stored.attempts == 2  # exactly one takeover, not six

    def test_the_stale_lease_race_is_decided_without_backend_support(self) -> None:
        """The CI failure, made deterministic: the backend evaluates nothing, so the guard decides.

        moto let two of four workers through on Linux because its ``If-Match`` check is a compare
        followed by a write. The client double here lets *every* worker through, and
        ``sync_first_reads`` holds all six at the point where they have each read the same stale
        marker - the reviewer's interleaving, and the one CI produced - so a single winner can only
        come from the in-process lock plus the version re-read inside :class:`S3Storage`. The losers
        then have to come back through the ledger and see a live lease, which is what stops the second
        email.
        """
        backend = _ConditionalWritesIgnored()
        store = S3Storage(backend, bucket=BUCKET)  # type: ignore[arg-type]
        ledger = DispatchLedger(store)
        ledger.claim(DATE, AGENT, now=NOW, lease_seconds=1)
        moment = NOW + timedelta(seconds=2)
        # Every worker reads the same stale marker before any of them writes: the reviewer's window.
        backend.sync_next_reads(6)

        results = _race(6, lambda _: DispatchLedger(store).claim(DATE, AGENT, now=moment))
        claims = [item for item in results if isinstance(item, ClaimResult)]
        assert len(claims) == 6, results
        assert [claim.reason for claim in claims if claim.claimed] == ["stale_lease"], results
        assert {claim.reason for claim in claims if not claim.claimed} <= {
            "in_flight",
            "contended",
        }, results
        stored = ledger.read(DATE, AGENT)
        assert stored is not None
        assert stored.attempts == 2  # exactly one takeover, not six

    def test_the_fresh_marker_create_race_is_decided_without_backend_support(self) -> None:
        """The other way an agent gets two emails: two workers both creating the marker."""
        backend = _ConditionalWritesIgnored()
        store = S3Storage(backend, bucket=BUCKET)  # type: ignore[arg-type]
        # All four workers see "no marker" before any of them writes one.
        backend.sync_next_reads(4)

        results = _race(4, lambda _: DispatchLedger(store).claim(DATE, AGENT, now=NOW))

        claims = [item for item in results if isinstance(item, ClaimResult)]
        assert len(claims) == 4, results
        assert sorted(claim.claimed for claim in claims) == [False, False, False, True], results
        assert [claim.reason for claim in claims if claim.claimed] == ["new"], results
        assert {claim.reason for claim in claims if not claim.claimed} <= {
            "in_flight",
            "contended",
        }, results
        stored = DispatchLedger(store).read(DATE, AGENT)
        assert stored is not None
        assert stored.attempts == 1

    def test_a_live_lease_is_deferred_rather_than_dropped(self, ledger: DispatchLedger) -> None:
        """A crashed worker's message must not be acknowledged away.

        ``in_flight`` means the message goes back to SQS (``redeliver``); only ``already_sent`` is
        safe to acknowledge, because then the agent really does have the email.
        """
        first = ledger.claim(DATE, AGENT, now=NOW)
        assert first.claimed is True
        second = ledger.claim(DATE, AGENT, now=NOW + timedelta(seconds=1))
        assert second.claimed is False
        assert second.reason == "in_flight"
        assert second.redeliver is True
        assert second.duplicate_suppressed is True

        ledger.mark_sent(
            DATE,
            AGENT,
            ses_message_id="ses-1",
            recipient="agt-000001@example.com",
            now=NOW + timedelta(seconds=2),
            record=first.record,
        )
        third = ledger.claim(DATE, AGENT, now=NOW + timedelta(seconds=3))
        assert third.reason == "already_sent"
        assert third.redeliver is False


class TestTerminalSentIsNeverRegressed:
    """The reviewer's second race: a late ``mark_failed`` used to undo a ``sent`` marker."""

    def test_a_late_failure_does_not_resurrect_a_sent_report(self, ledger: DispatchLedger) -> None:
        claim = ledger.claim(DATE, AGENT, now=NOW)
        ledger.mark_sent(
            DATE,
            AGENT,
            ses_message_id="ses-1",
            recipient="agt-000001@example.com",
            now=NOW + timedelta(seconds=1),
            record=claim.record,
        )
        late = ledger.mark_failed(
            DATE,
            AGENT,
            error_code="DependencyError",
            error_message="SES throttled a duplicate delivery",
            now=NOW + timedelta(seconds=2),
            record=claim.record,
        )
        assert late.status == STATUS_SENT
        assert late.ses_message_id == "ses-1"
        assert late.sent_at is not None

        again = ledger.claim(DATE, AGENT, now=NOW + timedelta(days=1))
        assert again.claimed is False
        assert again.reason == "already_sent"

    def test_a_late_sent_does_not_overwrite_the_ses_message_id(
        self, ledger: DispatchLedger
    ) -> None:
        claim = ledger.claim(DATE, AGENT, now=NOW)
        first = ledger.mark_sent(
            DATE,
            AGENT,
            ses_message_id="ses-first",
            recipient="agt-000001@example.com",
            now=NOW + timedelta(seconds=1),
            record=claim.record,
        )
        second = ledger.mark_sent(
            DATE,
            AGENT,
            ses_message_id="ses-second",
            recipient="agt-000001@example.com",
            now=NOW + timedelta(seconds=2),
            record=claim.record,
        )
        assert first.ses_message_id == "ses-first"
        assert second.ses_message_id == "ses-first"

    def test_a_worker_that_lost_the_lease_cannot_write_to_it(self, ledger: DispatchLedger) -> None:
        """After a takeover the marker belongs to the new worker, not to the old one."""
        stale = ledger.claim(DATE, AGENT, now=NOW, lease_seconds=1)
        takeover = ledger.claim(DATE, AGENT, now=NOW + timedelta(seconds=2))
        assert takeover.claimed is True
        assert takeover.reason == "stale_lease"
        assert takeover.record.lease_id != stale.record.lease_id

        written = ledger.mark_failed(
            DATE,
            AGENT,
            error_code="DependencyError",
            error_message="the worker I replaced came back",
            now=NOW + timedelta(seconds=3),
            record=stale.record,
        )
        assert written.status == STATUS_DISPATCHING
        assert written.lease_id == takeover.record.lease_id

    def test_the_lease_id_is_recorded_and_rotated(self, ledger: DispatchLedger) -> None:
        first = ledger.claim(DATE, AGENT, now=NOW, lease_seconds=1)
        assert first.record.lease_id
        second = ledger.claim(DATE, AGENT, now=NOW + timedelta(seconds=2))
        assert second.record.lease_id
        assert second.record.lease_id != first.record.lease_id

    def test_s3_compare_and_set_rejects_a_stale_version(self, aws: object) -> None:
        """The moto path rejects a stale version too - but not because moto is the referee.

        moto *does* compare the ETag (so a sequential stale write is refused), it just does not do the
        compare and the write atomically. The refusal this test sees comes from the in-process guard
        re-reading the version under the key lock, which is why it holds under a threaded race as well
        (``TestConditionalWriteGuards`` proves the same thing against a backend that checks nothing).
        """
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        store = S3Storage(client, bucket=BUCKET)
        store.put_bytes("state/probe.json", b"v1")
        _, version = store.get_bytes_with_version("state/probe.json")
        assert store.put_bytes_if_version("state/probe.json", b"v2", version=version) is True
        assert store.put_bytes_if_version("state/probe.json", b"v3", version=version) is False
        assert store.get_bytes("state/probe.json") == b"v2"

    def test_s3_stale_lease_race_is_decided_by_the_store(self, aws: object) -> None:
        """Four workers race one stale lease; exactly one may win - decided by the store, not by luck.

        The assertion is the one CI broke (``[False, False, True, True]``: two winners, two emails to
        the same agent). It is decided in-process by the per-key lock plus the version re-read inside
        :class:`S3Storage`, so it does not depend on moto's ``If-Match`` evaluation - which is a
        compare followed by a write, i.e. exactly the race the store has to close itself.
        """
        import boto3

        store = S3Storage(
            boto3.client("s3", region_name="us-east-1"), bucket="agent-reports-processed"
        )
        ledger = DispatchLedger(store)
        ledger.claim(DATE, AGENT, now=NOW, lease_seconds=1)
        moment = NOW + timedelta(seconds=2)

        results = _race(4, lambda _: DispatchLedger(store).claim(DATE, AGENT, now=moment))

        claims = [item for item in results if isinstance(item, ClaimResult)]
        assert len(claims) == 4, results
        assert sorted(claim.claimed for claim in claims) == [False, False, False, True], results
        assert [claim.reason for claim in claims if claim.claimed] == ["stale_lease"], results
        assert {claim.reason for claim in claims if not claim.claimed} <= {
            "in_flight",
            "contended",
        }, results
        stored = ledger.read(DATE, AGENT)
        assert stored is not None
        assert stored.attempts == 2  # exactly one takeover, not four
