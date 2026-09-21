"""Dispatcher: payload validation, quarantine, retries, duplicate suppression, batchItemFailures."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from botocore.exceptions import ClientError

from agent_reports.common.idempotency import DispatchLedger
from agent_reports.common.keys import report_key
from agent_reports.common.report import AgentTotals, render_report_csv
from agent_reports.common.settings import Settings
from agent_reports.common.storage import Zones
from agent_reports.lambda_handlers.dispatcher import (
    handler,
    parse_message,
    quarantine_message,
    run_dispatcher,
)

REPORT_DATE = "2026-09-20"
AGENT = "AGT-000001"
NOW = datetime(2026, 9, 20, 6, 0, tzinfo=UTC)
PAYLOAD = {
    "agent_id": AGENT,
    "report_date": REPORT_DATE,
    "recipient": "agt-000001@example.com",
    "agent_name": "Nisha Verma",
    "region": "Central",
    "branch": "Indore",
}


def sqs_record(payload: dict[str, Any] | str, message_id: str = "msg-1") -> dict[str, Any]:
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return {
        "messageId": message_id,
        "receiptHandle": f"rh-{message_id}",
        "body": body,
        "attributes": {"ApproximateReceiveCount": "1"},
        "messageSourceAttributes": {},
    }


def write_report(
    zones: Zones, agent_id: str = AGENT, report_date: str = REPORT_DATE, policies: int = 1
) -> str:
    details = [
        {
            "agent_id": agent_id,
            "agent_name": "Nisha Verma",
            "region": "Central",
            "branch": "Indore",
            "policy_id": f"POL-{index:010d}",
            "product": "Individual Health",
            "policy_start": "2026-01-01",
            "policy_end": "2027-01-01",
            "sum_insured": "1000000.00",
            "premium": "12000.50",
            "commission": "1200.05",
            "claim_count": 1,
            "claim_amount": "3000.00",
            "settled_amount": "2500.00",
        }
        for index in range(1, policies + 1)
    ]
    totals = AgentTotals(
        agent_id=agent_id,
        policy_count=policies,
        premium=Decimal("12000.50") * policies,
        commission=Decimal("1200.05") * policies,
        claim_count=policies,
        claim_amount=Decimal("3000.00") * policies,
        settled_amount=Decimal("2500.00") * policies,
        loss_ratio=Decimal("0.2500"),
    )
    key = report_key(report_date, agent_id)
    zones.reports.put_bytes(key, render_report_csv(details, totals).encode("utf-8"))
    return key


class TestParseMessage:
    def test_valid_payload(self) -> None:
        assert parse_message(json.dumps(PAYLOAD), "m1")["agent_id"] == AGENT

    @pytest.mark.parametrize(
        "body, match",
        [
            ("not json", "not valid JSON"),
            ("[]", "must be a JSON object"),
            ('{"agent_id":"AGT-000001"}', "missing required fields"),
            ('{"agent_id":"AGT-1","report_date":"2026-09-20","recipient":"a@b.com"}', "malformed"),
            (
                '{"agent_id":"AGT-000001","report_date":"2026-9-2","recipient":"a@b.com"}',
                "malformed",
            ),
            (
                '{"agent_id":"AGT-000001","report_date":"2026-09-20","recipient":"nope"}',
                "not an email",
            ),
            (
                '{"agent_id":"AGT-000001","report_date":"2026-09-20","recipient":"@b.com"}',
                "not an email",
            ),
            (
                '{"agent_id":"AGT-000001","report_date":"2026-09-20","recipient":"a@"}',
                "not an email",
            ),
        ],
    )
    def test_invalid_payloads(self, body: str, match: str) -> None:
        from agent_reports.common.errors import InvalidMessageError

        with pytest.raises(InvalidMessageError, match=match):
            parse_message(body, "m1")


class TestQuarantine:
    def test_quarantine_key_and_content(self, zones: Zones) -> None:
        from agent_reports.common.errors import InvalidMessageError

        error = InvalidMessageError("bad", context={"message_id": "m1"})
        key = quarantine_message(
            zones.processed,
            report_date=REPORT_DATE,
            message_id="m1",
            body="{oops",
            error=error,
            received_at=NOW,
        )
        assert key.startswith(f"state/quarantine/dt={REPORT_DATE}/message-")
        record = json.loads(zones.processed.get_bytes(key))
        assert record["message_id"] == "m1"
        assert record["error_code"] == "InvalidMessageError"
        assert record["body"] == "{oops"
        assert record["quarantined_at"] == NOW.isoformat()

    def test_quarantine_key_is_deterministic(self, zones: Zones) -> None:
        from agent_reports.common.errors import InvalidMessageError

        error = InvalidMessageError("bad")
        first = quarantine_message(
            zones.processed, report_date=REPORT_DATE, message_id="m1", body="x", error=error
        )
        second = quarantine_message(
            zones.processed, report_date=REPORT_DATE, message_id="m1", body="x", error=error
        )
        assert first == second


class TestQuarantineDatePartition:
    """A quarantined message must always land somewhere findable, whatever is wrong with it."""

    def test_date_taken_from_the_message_attributes(self, aws: Settings, zones: Zones) -> None:
        record = sqs_record("garbage", "m1")
        record["messageAttributes"] = {"report_date": {"stringValue": "2026-09-01"}}
        run_dispatcher(aws, records=[record], zones=zones, emit_metrics=False)
        assert zones.processed.list_keys("state/quarantine/dt=2026-09-01/")

    def test_date_taken_from_the_body_when_attributes_are_absent(
        self, aws: Settings, zones: Zones
    ) -> None:
        run_dispatcher(
            aws,
            records=[sqs_record('{"report_date":"2026-08-15","recipient":"x"}', "m2")],
            zones=zones,
            emit_metrics=False,
        )
        assert zones.processed.list_keys("state/quarantine/dt=2026-08-15/")

    def test_falls_back_to_the_configured_run_date(self, aws: Settings, zones: Zones) -> None:
        run_dispatcher(
            aws, records=[sqs_record("not json at all", "m3")], zones=zones, emit_metrics=False
        )
        assert zones.processed.list_keys(f"state/quarantine/dt={REPORT_DATE}/")

    def test_non_object_json_body_is_quarantined(self, aws: Settings, zones: Zones) -> None:
        result = run_dispatcher(
            aws, records=[sqs_record("[1, 2, 3]", "m4")], zones=zones, emit_metrics=False
        )
        assert result.quarantined == 1
        assert result.batch_item_failures == []


class TestDispatchBatch:
    def test_happy_path_sends_one_email_and_marks_the_ledger(
        self, aws: Settings, zones: Zones
    ) -> None:
        write_report(zones)
        result = run_dispatcher(aws, records=[sqs_record(PAYLOAD)], zones=zones, emit_metrics=False)
        assert result.sent == 1
        assert result.failed == 0
        assert result.batch_item_failures == []
        assert result.outcomes[0].ses_message_id
        assert result.outcomes[0].report_key == report_key(REPORT_DATE, AGENT)
        assert result.outcomes[0].report_age_seconds is not None

        from agent_reports.testing import sent_messages

        messages = sent_messages(aws.region)
        assert len(messages) == 1
        assert messages[0].destinations == {"ToAddresses": ["agt-000001@example.com"]}
        assert "Nisha Verma" in str(messages[0].body)
        assert "12000.50" in str(messages[0].body)

        from agent_reports.common.idempotency import DispatchLedger as Ledger

        record = Ledger(zones.processed).read(REPORT_DATE, AGENT)
        assert record is not None and record.status == "sent"

    def test_a_second_delivery_is_suppressed(self, aws: Settings, zones: Zones) -> None:
        write_report(zones)
        first = run_dispatcher(
            aws, records=[sqs_record(PAYLOAD, "m1")], zones=zones, emit_metrics=False
        )
        second = run_dispatcher(
            aws, records=[sqs_record(PAYLOAD, "m2")], zones=zones, emit_metrics=False
        )
        assert first.sent == 1
        assert second.sent == 0
        assert second.duplicates == 1
        assert second.outcomes[0].reason == "already_sent"

        from agent_reports.testing import sent_messages

        assert len(sent_messages(aws.region)) == 1

    def test_missing_report_is_retryable_and_reported_as_a_batch_failure(
        self, aws: Settings, zones: Zones
    ) -> None:
        result = run_dispatcher(aws, records=[sqs_record(PAYLOAD)], zones=zones, emit_metrics=False)
        assert result.failed == 1
        assert result.batch_item_failures == ["msg-1"]
        assert result.outcomes[0].error_code == "MissingReportError"
        assert result.outcomes[0].retryable is True

    def test_invalid_payload_is_quarantined_and_acknowledged(
        self, aws: Settings, zones: Zones
    ) -> None:
        write_report(zones)
        result = run_dispatcher(
            aws,
            records=[sqs_record("{not json"), sqs_record(PAYLOAD, "msg-2")],
            zones=zones,
            emit_metrics=False,
        )
        assert result.quarantined == 1
        assert result.sent == 1
        assert result.batch_item_failures == []
        assert result.outcomes[0].status == "quarantined"
        assert zones.processed.list_keys(f"state/quarantine/dt={REPORT_DATE}/")

    def test_permanent_ses_rejection_is_not_retried_but_is_dead_lettered(
        self, aws: Settings, zones: Zones
    ) -> None:
        """A permanent rejection needs a human, so it must reach the DLQ rather than vanish.

        The message is still returned in ``batchItemFailures``: SQS redelivers it up to
        ``maxReceiveCount`` and then moves it to the DLQ, where RUNBOOK section 5a's redrive
        playbook can find it. Acknowledging it would delete it with no DLQ entry.
        """
        write_report(zones)

        class RejectingSes:
            def send_email(self, **kwargs: Any) -> dict[str, Any]:
                raise ClientError(
                    {
                        "Error": {
                            "Code": "MessageRejected",
                            "Message": "Email address not verified",
                        },
                        "ResponseMetadata": {
                            "HTTPStatusCode": 400,
                            "RequestId": "req-1",
                            "HostId": "host-1",
                            "HTTPHeaders": {},
                            "RetryAttempts": 0,
                        },
                    },
                    "SendEmail",
                )

        result = run_dispatcher(
            aws,
            records=[sqs_record(PAYLOAD)],
            zones=zones,
            ses=RejectingSes(),
            emit_metrics=False,
        )
        assert result.failed == 1
        assert result.batch_item_failures == ["msg-1"]
        assert result.outcomes[0].error_code == "PermanentError"
        assert result.outcomes[0].retryable is False

    def test_throttling_is_retried_inside_the_invocation(self, aws: Settings, zones: Zones) -> None:
        write_report(zones)
        calls = {"count": 0}

        class FlakySes:
            def send_email(self, **kwargs: Any) -> dict[str, Any]:
                calls["count"] += 1
                if calls["count"] < 3:
                    raise ClientError(
                        {
                            "Error": {"Code": "Throttling", "Message": "slow down"},
                            "ResponseMetadata": {
                                "HTTPStatusCode": 429,
                                "RequestId": "req-1",
                                "HostId": "host-1",
                                "HTTPHeaders": {},
                                "RetryAttempts": 0,
                            },
                        },
                        "SendEmail",
                    )
                return {"MessageId": f"ses-{calls['count']}"}

        result = run_dispatcher(
            aws, records=[sqs_record(PAYLOAD)], zones=zones, ses=FlakySes(), emit_metrics=False
        )
        assert calls["count"] == 3
        assert result.sent == 1
        assert result.outcomes[0].ses_message_id == "ses-3"

    def test_persistent_throttling_becomes_a_batch_failure(
        self, aws: Settings, zones: Zones
    ) -> None:
        write_report(zones)

        class AlwaysThrottled:
            def send_email(self, **kwargs: Any) -> dict[str, Any]:
                raise ClientError(
                    {
                        "Error": {"Code": "Throttling", "Message": "slow down"},
                        "ResponseMetadata": {
                            "HTTPStatusCode": 429,
                            "RequestId": "req-1",
                            "HostId": "host-1",
                            "HTTPHeaders": {},
                            "RetryAttempts": 0,
                        },
                    },
                    "SendEmail",
                )

        result = run_dispatcher(
            aws,
            records=[sqs_record(PAYLOAD)],
            zones=zones,
            ses=AlwaysThrottled(),
            emit_metrics=False,
        )
        assert result.failed == 1
        assert result.batch_item_failures == ["msg-1"]
        assert result.outcomes[0].error_code == "DependencyError"

    def test_mixed_batch_is_partitioned_correctly(self, aws: Settings, zones: Zones) -> None:
        write_report(zones)
        records = [
            sqs_record(PAYLOAD, "m-ok"),
            sqs_record(
                {**PAYLOAD, "agent_id": "AGT-000002", "recipient": "agt-000002@example.com"},
                "m-missing",
            ),
            sqs_record("garbage", "m-bad"),
        ]
        result = run_dispatcher(aws, records=records, zones=zones, emit_metrics=False)
        assert result.processed == 3
        assert (result.sent, result.failed, result.quarantined) == (1, 1, 1)
        assert result.batch_item_failures == ["m-missing"]

    def test_handler_returns_partial_batch_response(
        self, handler_env: Settings, zones: Zones
    ) -> None:
        write_report(zones)
        response = handler({"Records": [sqs_record(PAYLOAD, "m1"), sqs_record("junk", "m2")]})
        assert response == {"batchItemFailures": []}
        response = handler({"Records": [sqs_record({**PAYLOAD, "agent_id": "AGT-000002"}, "m3")]})
        assert response == {"batchItemFailures": [{"itemIdentifier": "m3"}]}

    def test_oversized_report_still_sends_without_inline_totals(
        self, aws: Settings, zones: Zones
    ) -> None:
        from dataclasses import replace

        write_report(zones, policies=12)  # ~2 KB, comfortably over the 1 KB guardrail
        tiny_limit = replace(aws, max_report_bytes=1024)
        result = run_dispatcher(
            tiny_limit, records=[sqs_record(PAYLOAD)], zones=zones, emit_metrics=False
        )
        assert result.sent == 1
        assert result.outcomes[0].report_age_seconds is not None
        from agent_reports.testing import sent_messages

        assert "see report" in str(sent_messages(aws.region)[-1].body)

    def test_metrics_are_emitted_for_a_batch(self, aws: Settings, zones: Zones) -> None:
        from agent_reports.pipeline import capture_telemetry

        write_report(zones)
        with capture_telemetry() as telemetry:
            run_dispatcher(aws, records=[sqs_record(PAYLOAD)], zones=zones)
        assert telemetry.metric_documents
        names = {name for document in telemetry.metric_documents for name in document}
        assert "EmailsSent" in names
        assert "DispatchLatencyMs" in names
        assert "dispatcher_batch_completed" in telemetry.events

    def test_ledger_is_reused_across_batches(self, aws: Settings, zones: Zones) -> None:
        write_report(zones)
        ledger = DispatchLedger(zones.processed)
        run_dispatcher(
            aws, records=[sqs_record(PAYLOAD, "m1")], zones=zones, ledger=ledger, emit_metrics=False
        )
        assert ledger.sent_agents(REPORT_DATE) == [AGENT]


class TestNothingIsSilentlyDropped:
    """The reviewer's M4/M5/M6: every path that could delete a message without an email."""

    def test_a_message_whose_lease_is_still_live_is_redelivered(
        self, aws: Settings, zones: Zones
    ) -> None:
        """M4: a crash after claim leaves a lease; the message must come back, not be deleted."""
        write_report(zones)
        ledger = DispatchLedger(zones.processed)
        crashed = ledger.claim(REPORT_DATE, AGENT, now=NOW)  # a worker that then died
        assert crashed.claimed is True

        result = run_dispatcher(
            aws,
            records=[sqs_record(PAYLOAD)],
            zones=zones,
            ledger=ledger,
            emit_metrics=False,
            now=NOW + timedelta(seconds=1),  # while the crashed worker's lease is still live
        )
        assert result.sent == 0
        assert result.deferred == 1
        assert result.duplicates == 0
        assert result.batch_item_failures == ["msg-1"]
        assert result.outcomes[0].reason == "in_flight"
        assert result.outcomes[0].retryable is True

        # ... and once the lease expires the same message delivers the report.
        from agent_reports.testing import sent_messages

        assert sent_messages(aws.region) == []
        retry = run_dispatcher(
            aws,
            records=[sqs_record(PAYLOAD, "msg-2")],
            zones=zones,
            ledger=ledger,
            emit_metrics=False,
            now=NOW + timedelta(seconds=901),
        )
        assert retry.sent == 1
        assert retry.batch_item_failures == []
        assert len(sent_messages(aws.region)) == 1

    def test_ses_account_pause_is_retryable_and_reaches_the_dlq(
        self, aws: Settings, zones: Zones
    ) -> None:
        """M5: ``AccountSendingPausedException`` is an account state, not a poison message."""
        write_report(zones)

        class PausedSes:
            def send_email(self, **kwargs: Any) -> dict[str, Any]:
                raise ClientError(
                    {
                        "Error": {
                            "Code": "AccountSendingPausedException",
                            "Message": "Email sending is paused for this account",
                        },
                        "ResponseMetadata": {
                            "HTTPStatusCode": 400,
                            "RequestId": "req-1",
                            "HostId": "host-1",
                            "HTTPHeaders": {},
                            "RetryAttempts": 0,
                        },
                    },
                    "SendEmail",
                )

        result = run_dispatcher(
            aws, records=[sqs_record(PAYLOAD)], zones=zones, ses=PausedSes(), emit_metrics=False
        )
        assert result.failed == 1
        assert result.outcomes[0].retryable is True
        assert result.outcomes[0].error_code == "DependencyError"
        assert result.batch_item_failures == ["msg-1"]

        # The marker records a retryable failure, so the redelivery is allowed to send.
        ledger = DispatchLedger(zones.processed)
        marker = ledger.read(REPORT_DATE, AGENT)
        assert marker is not None
        assert marker.status == "failed"
        assert ledger.claim(REPORT_DATE, AGENT, now=NOW + timedelta(seconds=1)).claimed is True

    def test_a_corrupt_marker_is_redelivered_not_swallowed(
        self, aws: Settings, zones: Zones
    ) -> None:
        """M6: an unreadable marker is a ConfigError - it must reach the DLQ, not vanish."""
        write_report(zones)
        ledger = DispatchLedger(zones.processed)
        key = ledger.key_for(REPORT_DATE, AGENT)
        zones.processed.put_bytes(key, b"{ this is not json", content_type="application/json")

        result = run_dispatcher(aws, records=[sqs_record(PAYLOAD)], zones=zones, emit_metrics=False)
        assert result.failed == 1
        assert result.outcomes[0].error_code == "ConfigError"
        assert result.batch_item_failures == ["msg-1"]
        # The marker is left exactly as it was: a human has to look at it.
        assert zones.processed.get_bytes(key) == b"{ this is not json"

        from agent_reports.testing import sent_messages

        assert sent_messages(aws.region) == []
