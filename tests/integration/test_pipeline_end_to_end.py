"""Full-pipeline integration tests under moto: the free-tier path end to end, offline.

These are the tests that would catch a regression the unit tests cannot: the chunker, orchestrator
and dispatcher agreeing on keys, the pre-signed link inside the delivered email actually resolving
to the right CSV, the queue draining, a poison message reaching the DLQ, and a replay sending
nothing twice.
"""

from __future__ import annotations

import csv
import io
import json
from decimal import Decimal

import pytest

from agent_reports.common.aws import cloudwatch_client, sqs_client
from agent_reports.common.keys import manifest_key, parse_report_key, report_key
from agent_reports.common.report import parse_report_totals
from agent_reports.common.roster import iter_source_rows, report_agent_ids
from agent_reports.common.settings import Settings
from agent_reports.common.storage import Zones
from agent_reports.lambda_handlers.dispatcher import run_dispatcher
from agent_reports.lambda_handlers.orchestrator import run_orchestrator
from agent_reports.pipeline import (
    PipelineOptions,
    extract_download_url,
    queue_depth,
    receive_records,
    run_local_pipeline,
)
from agent_reports.testing import sent_messages, verify_presigned_delivery

REPORT_DATE = "2026-09-20"
POISON_AGENT = "AGT-999999"


def drain_queue(aws: Settings, zones: Zones, *, max_batches: int = 20) -> dict[str, int]:
    """Consume the fan-out queue the way the event source mapping does (poison ends in the DLQ)."""
    sqs = sqs_client(aws)
    totals = {"sent": 0, "duplicates": 0, "failed": 0, "batches": 0, "batch_item_failures": 0}
    for _ in range(max_batches):
        records = receive_records(sqs, aws.agent_queue_url, max_messages=10)
        if not records:
            break
        totals["batches"] += 1
        batch = run_dispatcher(aws, records=records, zones=zones, emit_metrics=False)
        totals["sent"] += batch.sent
        totals["duplicates"] += batch.duplicates
        totals["failed"] += batch.failed
        totals["batch_item_failures"] += len(batch.batch_item_failures)
        failed_ids = set(batch.batch_item_failures)
        for record in records:
            if str(record["messageId"]) in failed_ids:
                continue
            sqs.delete_message(
                QueueUrl=aws.agent_queue_url, ReceiptHandle=str(record["receiptHandle"])
            )
    return totals


@pytest.fixture
def pipeline_run(aws: Settings, zones: Zones) -> tuple[Settings, Zones, dict[str, object]]:
    result = run_local_pipeline(
        aws,
        PipelineOptions(report_date=REPORT_DATE, rows=1_200, seed=13, shards=2, dlq_wait_receives=1),
    )
    return aws, zones, result.as_dict()


class TestEndToEnd:
    def test_pipeline_invariants(self, pipeline_run: tuple[Settings, Zones, dict[str, object]]) -> None:
        _, _, result = pipeline_run
        assert result["rows_in"] >= 1_200
        assert result["agents_reported"] == result["reports_written"]
        assert result["emails_sent"] == result["agents_reported"]
        assert result["duplicates"] == 0
        assert result["failed"] == 0
        assert result["quarantined"] == 0
        assert result["queue_drained"] is True
        assert result["dlq_messages"] == 0
        assert result["metrics_emitted"] > 0
        assert result["objects_written"] >= result["reports_written"]
        assert result["duration_seconds"] > 0
        assert set(result["stages"]) == {"generate", "aggregate", "fanout", "dispatch"}

    def test_emf_metrics_cover_the_three_stages(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        _, _, result = pipeline_run
        names = set(result["metric_names"])
        assert {"ReportsWritten", "RowsIn", "MessagesEnqueued", "EmailsSent"} <= names
        assert "DispatchLatencyMs" in names
        assert "ReportAgeSeconds" in names

    def test_cloudwatch_received_the_datapoints(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        aws, _, _ = pipeline_run
        metrics = cloudwatch_client(aws).list_metrics(Namespace="AgentReports")["Metrics"]
        names = {metric["MetricName"] for metric in metrics}
        assert {"EmailsSent", "ReportsWritten", "AgentsDiscovered", "MessagesEnqueued"} <= names
        assert any(metric["Dimensions"] for metric in metrics)

    def test_ses_captured_exactly_one_message_per_agent(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        aws, _, result = pipeline_run
        messages = sent_messages(aws.region)
        assert len(messages) == result["emails_sent"]
        recipients = {str(message.destinations["ToAddresses"][0]) for message in messages}
        assert len(recipients) == len(messages)
        for message in messages:
            assert message.source == aws.ses_sender
            assert str(message.subject).startswith(f"Agent report {REPORT_DATE}")

    def test_every_report_object_has_the_expected_shape(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        aws, zones, _ = pipeline_run
        keys = [
            key
            for key in zones.reports.list_keys(f"reports/dt={REPORT_DATE}/")
            if parse_report_key(key) is not None
        ]
        assert len(keys) == len(report_agent_ids(zones.reports, REPORT_DATE))
        sample = zones.reports.get_bytes(keys[0]).decode("utf-8")
        rows = list(csv.DictReader(io.StringIO(sample)))
        assert rows[0]["row_type"] == "DETAIL"
        assert rows[-1]["row_type"] == "TOTAL"
        totals = parse_report_totals(sample)
        assert totals.policy_count == len(rows) - 1
        assert totals.premium > 0

    def test_run_manifest_records_the_fanout(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        aws, zones, result = pipeline_run
        manifest = json.loads(zones.processed.get_bytes(manifest_key(REPORT_DATE)))
        assert manifest["report_date"] == REPORT_DATE
        assert manifest["agents_discovered"] == result["agents_discovered"]
        assert manifest["messages_enqueued"] == result["messages_enqueued"]
        assert manifest["partial_batch_failures"] == 0
        assert manifest["failed_agent_ids"] == []

    def test_dispatch_markers_exist_for_every_delivery(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        aws, zones, result = pipeline_run
        markers = zones.processed.list_keys(f"state/dispatch/dt={REPORT_DATE}/")
        assert len(markers) == result["emails_sent"]
        record = json.loads(zones.processed.get_bytes(markers[0]))
        assert record["status"] == "sent"
        assert record["ses_message_id"]
        assert record["attempts"] == 1

    def test_embedded_presigned_link_returns_the_report_object(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        aws, zones, result = pipeline_run
        agent_id = str(result["sample_agent_id"])
        delivery = verify_presigned_delivery(
            aws,
            region=aws.region,
            recipient=f"{agent_id.lower()}@example.com",
            agent_id=agent_id,
            report_date=REPORT_DATE,
            zones=zones,
        )
        assert delivery["http_status"] == 200
        assert delivery["matches_report_object"] is True
        assert delivery["bytes"] > 0
        assert delivery["url_scheme"] == "https"
        assert "X-Amz-Signature" in delivery["url_query_keys"]

    def test_queue_is_empty_after_the_run(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        aws, _, _ = pipeline_run
        assert queue_depth(sqs_client(aws), aws.agent_queue_url) == {
            "visible": 0,
            "in_flight": 0,
            "delayed": 0,
        }
        assert queue_depth(sqs_client(aws), aws.dlq_url)["visible"] == 0


class TestReportCorrectness:
    def test_report_totals_match_an_independent_recomputation(
        self, pipeline_run: tuple[Settings, Zones, dict[str, object]]
    ) -> None:
        """Recompute one agent's numbers straight from the raw partitions and compare."""
        aws, zones, _ = pipeline_run
        agent_id = report_agent_ids(zones.reports, REPORT_DATE)[0]

        policies: dict[str, dict[str, str]] = {}
        for row in iter_source_rows(zones.raw, REPORT_DATE, "policies"):
            if row["agent_id"] == agent_id:
                policies[row["policy_id"]] = row
        assert policies
        expected_premium = sum((Decimal(row["premium"]) for row in policies.values()), Decimal("0"))
        expected_commission = sum(
            (
                (Decimal(row["premium"]) * Decimal(row["commission_rate"])).quantize(
                    Decimal("0.01")
                )
                for row in policies.values()
            ),
            Decimal("0"),
        )
        expected_claims = 0
        expected_claim_amount = Decimal("0")
        expected_settled = Decimal("0")
        for row in iter_source_rows(zones.raw, REPORT_DATE, "claims"):
            if row["agent_id"] != agent_id:
                continue
            expected_claims += 1
            expected_claim_amount += Decimal(row["claimed_amount"])
            expected_settled += Decimal(row["settled_amount"])

        totals = parse_report_totals(
            zones.reports.get_bytes(report_key(REPORT_DATE, agent_id)).decode("utf-8")
        )
        assert totals.policy_count == len(policies)
        assert totals.premium == expected_premium
        assert totals.commission == expected_commission
        assert totals.claim_count == expected_claims
        assert totals.claim_amount == expected_claim_amount
        assert totals.settled_amount == expected_settled


class TestReplay:
    def test_a_second_dispatch_of_the_same_day_sends_nothing(
        self, aws: Settings, zones: Zones
    ) -> None:
        first = run_local_pipeline(
            aws, PipelineOptions(report_date=REPORT_DATE, rows=600, seed=21, shards=1)
        )
        assert first.emails_sent > 0
        before = len(sent_messages(aws.region))

        fanout = run_orchestrator(
            aws, report_date=REPORT_DATE, zones=zones, sqs=sqs_client(aws), emit_metrics=False
        )
        assert fanout.messages_enqueued == first.emails_sent
        replay = drain_queue(aws, zones)
        assert replay["sent"] == 0
        assert replay["duplicates"] == fanout.messages_enqueued
        assert replay["batch_item_failures"] == 0
        assert len(sent_messages(aws.region)) == before

    def test_quarantine_records_a_corrupt_message(
        self, aws: Settings, zones: Zones
    ) -> None:
        sqs = sqs_client(aws)
        sqs.send_message(QueueUrl=aws.agent_queue_url, MessageBody="{not-json")
        totals = drain_queue(aws, zones)
        assert totals["batches"] >= 1
        quarantined = zones.processed.list_keys(f"state/quarantine/dt={REPORT_DATE}/")
        assert len(quarantined) == 1
        record = json.loads(zones.processed.get_bytes(quarantined[0]))
        assert record["error_code"] == "InvalidMessageError"
        assert record["body"] == "{not-json"


class TestPoisonMessage:
    def test_poison_message_lands_in_the_dlq_after_max_receives(
        self, aws: Settings, zones: Zones
    ) -> None:
        """A message whose report never appears is retried, then dead-lettered - not lost, not looped."""
        run_local_pipeline(aws, PipelineOptions(report_date=REPORT_DATE, rows=300, seed=5, shards=1))
        sqs = sqs_client(aws)
        sqs.send_message(
            QueueUrl=aws.agent_queue_url,
            MessageBody=json.dumps(
                {
                    "agent_id": POISON_AGENT,
                    "report_date": REPORT_DATE,
                    "recipient": "agt-999999@example.com",
                    "agent_name": "Missing Agent",
                }
            ),
        )
        assert queue_depth(sqs, aws.agent_queue_url)["visible"] == 1

        totals = drain_queue(aws, zones)
        assert totals["failed"] >= 1
        assert totals["batch_item_failures"] >= 1
        assert totals["sent"] == 0
        assert queue_depth(sqs, aws.agent_queue_url)["visible"] == 0
        assert queue_depth(sqs, aws.dlq_url)["visible"] == 1

        dead = sqs.receive_message(
            QueueUrl=aws.dlq_url, MaxNumberOfMessages=1, AttributeNames=["All"]
        )["Messages"][0]
        payload = json.loads(dead["Body"])
        assert payload["agent_id"] == POISON_AGENT
        assert int(dead["Attributes"]["ApproximateReceiveCount"]) >= 1

    def test_dlq_message_can_be_redriven_after_the_report_appears(
        self, aws: Settings, zones: Zones
    ) -> None:
        """The DLQ playbook: fix the cause, redrive, and the message is delivered."""
        sqs = sqs_client(aws)
        body = json.dumps(
            {
                "agent_id": POISON_AGENT,
                "report_date": REPORT_DATE,
                "recipient": "agt-999999@example.com",
            }
        )
        sqs.send_message(QueueUrl=aws.agent_queue_url, MessageBody=body)
        drain_queue(aws, zones)
        assert queue_depth(sqs, aws.dlq_url)["visible"] == 1

        # The report now exists (as if the aggregation bug had been fixed).
        from agent_reports.lambda_handlers.chunker import run_chunker

        zones.raw.put_bytes(
            f"raw/dt={REPORT_DATE}/source=agents/part-00000.csv",
            (
                "agent_id,agent_name,email,region,branch,manager_id,joined_on\n"
                f"{POISON_AGENT},Late Arrival,agt-999999@example.com,North,Delhi,"
                f"{POISON_AGENT},2025-01-01\n"
            ).encode(),
        )
        zones.raw.put_bytes(
            f"raw/dt={REPORT_DATE}/source=policies/part-00000.csv",
            (
                "policy_id,agent_id,customer_id,product,policy_start,policy_end,sum_insured,"
                "premium,commission_rate,status\n"
                f"POL-0000099999,{POISON_AGENT},CUS-000000001,Term Life,2026-01-01,2027-01-01,"
                "1000000.00,5000.00,0.2000,Active\n"
            ).encode(),
        )
        run_chunker(aws, report_date=REPORT_DATE, agent_ids=[POISON_AGENT], zones=zones, emit_metrics=False)
        assert zones.reports.exists(report_key(REPORT_DATE, POISON_AGENT))

        dead = sqs.receive_message(QueueUrl=aws.dlq_url, MaxNumberOfMessages=1)["Messages"][0]
        sqs.send_message(QueueUrl=aws.agent_queue_url, MessageBody=dead["Body"])
        sqs.delete_message(QueueUrl=aws.dlq_url, ReceiptHandle=dead["ReceiptHandle"])

        totals = drain_queue(aws, zones)
        assert totals["sent"] == 1
        assert totals["failed"] == 0
        assert queue_depth(sqs, aws.dlq_url)["visible"] == 0

    def test_poison_message_body_is_never_mutated(self, aws: Settings, zones: Zones) -> None:
        sqs = sqs_client(aws)
        original = json.dumps({"agent_id": POISON_AGENT, "report_date": REPORT_DATE, "recipient": "a@b.com"})
        sqs.send_message(QueueUrl=aws.agent_queue_url, MessageBody=original)
        drain_queue(aws, zones)
        dead = sqs.receive_message(QueueUrl=aws.dlq_url, MaxNumberOfMessages=1)["Messages"][0]
        assert dead["Body"] == original


class TestDownloadUrlExtraction:
    def test_extracts_the_first_url(self) -> None:
        text = "Download here: https://s3.example.com/x.csv?X-Amz-Signature=abc and enjoy"
        assert extract_download_url(text) == "https://s3.example.com/x.csv?X-Amz-Signature=abc"

    def test_unescapes_html_entities(self) -> None:
        assert extract_download_url('href="https://x/y?a=1&amp;b=2"') == "https://x/y?a=1&b=2"

    def test_missing_url_raises(self) -> None:
        with pytest.raises(ValueError, match="no download URL"):
            extract_download_url("no links here")
