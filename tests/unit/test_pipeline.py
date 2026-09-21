"""The pipeline runner itself: batch budgeting and SQS record shaping."""

from __future__ import annotations

import json

import pytest

from agent_reports.common.settings import Settings
from agent_reports.common.storage import Zones
from agent_reports.pipeline import PipelineOptions, queue_depth, receive_records, run_local_pipeline


class TestDispatchBudget:
    def test_budget_scales_with_the_fanout(self) -> None:
        options = PipelineOptions(report_date="2026-09-20")
        assert options.dispatch_batch_budget(0, 10) == 50
        assert options.dispatch_batch_budget(1_000, 10) == 350
        assert options.dispatch_batch_budget(3_805, 10) == 1_190

    def test_budget_always_covers_the_fanout_plus_headroom(self) -> None:
        options = PipelineOptions(report_date="2026-09-20")
        for agents in (10, 100, 3_805, 50_000):
            needed = -(-agents // 10)  # ceil
            assert options.dispatch_batch_budget(agents, 10) >= needed + 50

    def test_explicit_cap_is_honoured(self) -> None:
        options = PipelineOptions(report_date="2026-09-20", max_dispatch_batches=7)
        assert options.dispatch_batch_budget(10_000, 10) == 7

    def test_zero_batch_size_does_not_divide_by_zero(self) -> None:
        assert PipelineOptions(report_date="2026-09-20").dispatch_batch_budget(5, 0) == 65


class TestReceiveRecords:
    def test_records_are_shaped_like_an_event_source_mapping(self, aws: Settings) -> None:
        import boto3

        sqs = boto3.client("sqs", region_name=aws.region)
        sqs.send_message(
            QueueUrl=aws.agent_queue_url,
            MessageBody=json.dumps({"agent_id": "AGT-000001", "report_date": "2026-09-20"}),
            MessageAttributes={
                "report_date": {"DataType": "String", "StringValue": "2026-09-20"},
            },
        )
        records = receive_records(sqs, aws.agent_queue_url, max_messages=10)
        assert len(records) == 1
        record = records[0]
        assert set(record) >= {
            "messageId",
            "receiptHandle",
            "body",
            "attributes",
            "messageAttributes",
        }
        assert json.loads(record["body"])["agent_id"] == "AGT-000001"
        # The event source mapping hands the handler camelCase attributes, not the SQS API's shape.
        assert record["messageAttributes"]["report_date"]["stringValue"] == "2026-09-20"
        assert record["messageAttributes"]["report_date"]["dataType"] == "String"
        assert record["eventSource"] == "aws:sqs"

    def test_message_attribute_conversion_is_lossless_for_other_shapes(self) -> None:
        from agent_reports.pipeline import _event_message_attributes

        converted = _event_message_attributes(
            {
                "plain": {"DataType": "String", "StringValue": "v"},
                "listy": {"DataType": "String.Array", "StringListValues": ["a", "b"]},
                "binary": {"DataType": "Binary", "BinaryValue": b"\x01"},
                "junk": "not-a-mapping",
            }
        )
        assert converted["plain"] == {"dataType": "String", "stringValue": "v"}
        assert converted["listy"]["stringListValues"] == ["a", "b"]
        assert converted["binary"]["binaryValue"] == b"\x01"
        assert "junk" not in converted

    def test_empty_queue_returns_no_records(self, aws: Settings) -> None:
        import boto3

        assert (
            receive_records(boto3.client("sqs", region_name=aws.region), aws.agent_queue_url) == []
        )

    def test_queue_depth_counts_visible_messages(self, aws: Settings) -> None:
        import boto3

        sqs = boto3.client("sqs", region_name=aws.region)
        assert queue_depth(sqs, aws.agent_queue_url) == {"visible": 0, "in_flight": 0, "delayed": 0}
        sqs.send_message(QueueUrl=aws.agent_queue_url, MessageBody="{}")
        assert queue_depth(sqs, aws.agent_queue_url)["visible"] == 1


class TestPipelineOptions:
    def test_dataset_config_derives_from_rows(self) -> None:
        config = PipelineOptions(report_date="2026-09-20", rows=1_000, seed=3).dataset_config()
        assert config.estimated_rows >= 1_000
        assert config.seed == 3

    def test_explicit_agent_count_wins(self) -> None:
        config = PipelineOptions(report_date="2026-09-20", agents=7).dataset_config()
        assert config.agents == 7


class TestEmrRoutingThreshold:
    """``AGENT_REPORTS_EMR_ROW_THRESHOLD`` used to be documented and never read."""

    def test_the_emr_routing_threshold_is_read(
        self, aws: Settings, zones: Zones, small_dataset: dict[str, int]
    ) -> None:
        from dataclasses import replace

        from agent_reports.lambda_handlers.chunker import run_chunker
        from agent_reports.pipeline import capture_telemetry

        assert small_dataset["rows_total"] > 0
        with capture_telemetry() as telemetry:
            quiet = run_chunker(
                replace(aws, emr_row_threshold=10_000),
                report_date="2026-09-20",
                zones=zones,
                emit_metrics=False,
            )
        assert quiet.rows_read > 0
        assert quiet.emr_routing_advised is False
        assert quiet.as_dict()["emr_routing_advised"] is False
        assert "emr_routing_advised" not in telemetry.events

        with capture_telemetry() as telemetry:
            loud = run_chunker(
                replace(aws, emr_row_threshold=1),
                report_date="2026-09-20",
                zones=zones,
                emit_metrics=False,
            )
        assert loud.emr_routing_advised is True
        assert "emr_routing_advised" in telemetry.events


@pytest.mark.parametrize("report_date", ["20-09-2026", ""])
def test_pipeline_rejects_a_malformed_date(aws: Settings, report_date: str) -> None:
    with pytest.raises(ValueError, match="report_date"):
        run_local_pipeline(aws, PipelineOptions(report_date=report_date, rows=50))
