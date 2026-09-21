"""Orchestrator: fan-out planning, partial batch failures, and the run manifest."""

from __future__ import annotations

import json
from typing import Any

import pytest
from botocore.exceptions import ClientError

from agent_reports.common.keys import manifest_key
from agent_reports.common.settings import Settings
from agent_reports.common.storage import Zones
from agent_reports.lambda_handlers.chunker import run_chunker
from agent_reports.lambda_handlers.orchestrator import (
    build_message,
    handler,
    plan_fanout,
    run_orchestrator,
)

REPORT_DATE = "2026-09-20"
ROSTER = {
    "AGT-000001": {
        "email": "agt-000001@example.com",
        "agent_name": "A One",
        "region": "North",
        "branch": "Delhi",
    },
    "AGT-000002": {
        "email": "agt-000002@example.com",
        "agent_name": "B Two",
        "region": "South",
        "branch": "Kochi",
    },
    "AGT-000003": {
        "email": "agt-000003@example.com",
        "agent_name": "C Three",
        "region": "East",
        "branch": "Patna",
    },
}


class TestPlanning:
    def test_only_agents_with_reports_are_targeted(self) -> None:
        targets, missing = plan_fanout(ROSTER, ["AGT-000001", "AGT-000003"])
        assert targets == ["AGT-000001", "AGT-000003"]
        assert missing == ["AGT-000002"]

    def test_require_report_false_includes_everyone(self) -> None:
        targets, missing = plan_fanout(ROSTER, [], require_report=False)
        assert targets == ["AGT-000001", "AGT-000002", "AGT-000003"]
        assert missing == targets

    def test_explicit_request_is_intersected_with_the_roster(self) -> None:
        targets, _ = plan_fanout(
            ROSTER, ["AGT-000001", "AGT-000002"], requested_agent_ids=["AGT-000002", "AGT-000404"]
        )
        assert targets == ["AGT-000002"]

    def test_empty_inputs_produce_no_targets(self) -> None:
        assert plan_fanout({}, []) == ([], [])

    def test_message_payload(self) -> None:
        payload = build_message("AGT-000001", REPORT_DATE, ROSTER["AGT-000001"])
        assert payload == {
            "agent_id": "AGT-000001",
            "report_date": REPORT_DATE,
            "recipient": "agt-000001@example.com",
            "agent_name": "A One",
            "region": "North",
            "branch": "Delhi",
        }


class _PartialFailSqs:
    """Fails the first entry of the first batch, accepts the retry - the real SQS behaviour."""

    def __init__(self, queue_url: str, *, fail_first_only: bool = True) -> None:
        self.queue_url = queue_url
        self.fail_first_only = fail_first_only
        self.batch_calls = 0
        self.single_calls = 0
        self.accepted: list[str] = []

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:
        assert QueueUrl == self.queue_url
        self.batch_calls += 1
        failed = [Entries[0]] if self.batch_calls == 1 else []
        successful = [
            {"Id": entry["Id"], "MessageId": f"m-{entry['Id']}"}
            for entry in Entries
            if entry not in failed
        ]
        self.accepted.extend(entry["Id"] for entry in successful)
        return {
            "Successful": successful,
            "Failed": [
                {
                    "Id": entry["Id"],
                    "SenderFault": False,
                    "Code": "RequestThrottled",
                    "Message": "slow down",
                }
                for entry in failed
            ],
        }

    def send_message(
        self, *, QueueUrl: str, MessageBody: str, MessageAttributes: Any
    ) -> dict[str, Any]:
        assert QueueUrl == self.queue_url
        self.single_calls += 1
        payload = json.loads(MessageBody)
        self.accepted.append(payload["agent_id"])
        return {"MessageId": f"single-{payload['agent_id']}"}


class _AlwaysFailSqs(_PartialFailSqs):
    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:
        self.batch_calls += 1
        return {
            "Successful": [],
            "Failed": [
                {
                    "Id": entry["Id"],
                    "SenderFault": False,
                    "Code": "RequestThrottled",
                    "Message": "no",
                }
                for entry in Entries
            ],
        }

    def send_message(
        self, *, QueueUrl: str, MessageBody: str, MessageAttributes: Any
    ) -> dict[str, Any]:
        raise ClientError(
            {
                "Error": {"Code": "RequestThrottled", "Message": "nope"},
                "ResponseMetadata": {
                    "HTTPStatusCode": 429,
                    "RequestId": "req-1",
                    "HostId": "host-1",
                    "HTTPHeaders": {},
                    "RetryAttempts": 0,
                },
            },
            "SendMessage",
        )


@pytest.fixture
def reports_ready(aws: Settings, zones: Zones, small_dataset: dict[str, int]) -> Zones:
    """Aggregate the small dataset so every agent has a report object."""
    run_chunker(aws, report_date=REPORT_DATE, zones=zones, emit_metrics=False)
    return zones


class TestRunOrchestrator:
    def test_fanout_enqueues_one_message_per_agent(
        self, aws: Settings, reports_ready: Zones
    ) -> None:
        sqs = _PartialFailSqs(aws.agent_queue_url)
        result = run_orchestrator(
            aws, report_date=REPORT_DATE, zones=reports_ready, sqs=sqs, emit_metrics=False
        )
        assert result.agents_discovered == small_agent_count(reports_ready)
        assert result.agents_with_reports == result.agents_discovered
        assert result.agents_targeted == result.agents_discovered
        assert result.messages_enqueued == result.agents_discovered
        assert result.partial_batch_failures == 1
        assert sqs.single_calls == 1
        assert result.failed_agent_ids == []

    def test_partial_batch_failure_is_retried_individually(
        self, aws: Settings, reports_ready: Zones
    ) -> None:
        sqs = _PartialFailSqs(aws.agent_queue_url)
        run_orchestrator(
            aws, report_date=REPORT_DATE, zones=reports_ready, sqs=sqs, emit_metrics=False
        )
        assert len(sqs.accepted) == len(set(sqs.accepted))
        assert sqs.batch_calls >= 1
        assert sqs.single_calls == 1

    def test_hopeless_failures_are_recorded_not_dropped(
        self, aws: Settings, reports_ready: Zones
    ) -> None:
        sqs = _AlwaysFailSqs(aws.agent_queue_url)
        result = run_orchestrator(
            aws, report_date=REPORT_DATE, zones=reports_ready, sqs=sqs, emit_metrics=False
        )
        assert result.messages_enqueued == 0
        assert result.failed_agent_ids
        assert len(result.failed_agent_ids) == result.agents_targeted

    def test_manifest_is_written_with_the_counts(self, aws: Settings, reports_ready: Zones) -> None:
        sqs = _PartialFailSqs(aws.agent_queue_url)
        result = run_orchestrator(
            aws, report_date=REPORT_DATE, zones=reports_ready, sqs=sqs, emit_metrics=False
        )
        manifest = json.loads(reports_ready.processed.get_bytes(manifest_key(REPORT_DATE)))
        assert manifest["report_date"] == REPORT_DATE
        assert manifest["messages_enqueued"] == result.messages_enqueued
        assert manifest["agents_discovered"] == result.agents_discovered
        assert manifest["settings"]["sqs_batch_size"] == aws.sqs_batch_size
        assert manifest["duration_seconds"] >= 0

    def test_no_reports_means_no_targets(
        self, aws: Settings, zones: Zones, small_dataset: dict[str, int]
    ) -> None:
        sqs = _PartialFailSqs(aws.agent_queue_url)
        result = run_orchestrator(
            aws, report_date=REPORT_DATE, zones=zones, sqs=sqs, emit_metrics=False
        )
        assert result.agents_discovered == small_dataset["agents"]
        assert result.agents_with_reports == 0
        assert result.agents_targeted == 0
        assert sqs.batch_calls == 0

    def test_unknown_agents_are_ignored(self, aws: Settings, reports_ready: Zones) -> None:
        sqs = _PartialFailSqs(aws.agent_queue_url)
        result = run_orchestrator(
            aws,
            report_date=REPORT_DATE,
            zones=reports_ready,
            sqs=sqs,
            requested_agent_ids=["AGT-999999"],
            emit_metrics=False,
        )
        assert result.agents_targeted == 0

    def test_handler_reads_the_event_and_returns_the_summary(
        self, handler_env: Settings, reports_ready: Zones
    ) -> None:
        summary = handler({"report_date": REPORT_DATE})
        assert summary["report_date"] == REPORT_DATE
        assert summary["messages_enqueued"] == summary["agents_targeted"]
        assert summary["manifest_uri"].endswith(manifest_key(REPORT_DATE))

    def test_handler_rejects_a_malformed_agent_id(
        self, handler_env: Settings, reports_ready: Zones
    ) -> None:
        with pytest.raises(ValueError, match="agent_id must look like"):
            handler({"report_date": REPORT_DATE, "agent_ids": ["../../etc/passwd"]})


def small_agent_count(zones: Zones) -> int:
    from agent_reports.common.roster import report_agent_ids

    return len(report_agent_ids(zones.reports, REPORT_DATE))
