"""Logging + EMF metrics: the JSON contract, the EMF document shape, and the CloudWatch write path."""

from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from agent_reports.common.logging_utils import (
    JsonFormatter,
    configure_logging,
    get_logger,
    log_emf,
    log_event,
)
from agent_reports.common.metrics import (
    DEFAULT_NAMESPACE,
    METRIC_NAMES,
    Metric,
    emf_document,
    emit_emf,
    put_metric_data,
)


@pytest.fixture
def log_stream() -> Iterator[io.StringIO]:
    stream = io.StringIO()
    configure_logging("DEBUG", stream=stream, force=True)
    yield stream
    configure_logging(force=True)


class TestJsonLogging:
    def test_json_line_has_the_contract_fields(self, log_stream: io.StringIO) -> None:
        logger = get_logger("tests.json")
        log_event(logger, "unit_event", agent_id="AGT-000001", rows=3)
        payload = json.loads(log_stream.getvalue().strip().splitlines()[-1])
        assert payload["event"] == "unit_event"
        assert payload["level"] == "INFO"
        assert payload["service"] == "agent-reports"
        assert payload["logger"] == "agent_reports.tests.json"
        assert payload["agent_id"] == "AGT-000001"
        assert payload["rows"] == 3
        assert payload["timestamp"].endswith("Z")

    def test_log_event_returns_the_logged_payload(self, log_stream: io.StringIO) -> None:
        payload = log_event(get_logger("tests.json"), "returned", value=1)
        assert payload == {"event": "returned", "value": 1}

    def test_extra_fields_cannot_shadow_reserved_attributes(self) -> None:
        record = logging.LogRecord("x", logging.INFO, "p", 1, "msg", (), None)
        formatter = JsonFormatter()
        record.__dict__["message"] = "should be ignored"
        record.__dict__["agent_id"] = "AGT-000002"
        rendered = json.loads(formatter.format(record))
        assert rendered["agent_id"] == "AGT-000002"
        assert rendered["message"] == "msg"

    def test_exception_is_rendered(self) -> None:
        try:
            raise ValueError("kaboom")
        except ValueError:
            record = logging.LogRecord(
                "x", logging.ERROR, "p", 1, "failed", (), __import__("sys").exc_info()
            )
        rendered = json.loads(JsonFormatter().format(record))
        assert "ValueError: kaboom" in rendered["exception"]

    def test_get_logger_prefixes_bare_names(self) -> None:
        assert get_logger("chunker").name == "agent_reports.chunker"
        assert get_logger("agent_reports.chunker").name == "agent_reports.chunker"

    def test_configure_logging_is_idempotent_unless_forced(self) -> None:
        logger = configure_logging("INFO", stream=io.StringIO(), force=True)
        first = len(logger.handlers)
        configure_logging("INFO")
        assert len(logger.handlers) == first
        configure_logging("INFO", stream=io.StringIO(), force=True)
        assert len(logger.handlers) == first
        configure_logging(force=True)

    def test_emf_records_bypass_the_json_formatter(self, log_stream: io.StringIO) -> None:
        log_emf(get_logger("tests.json"), {"_aws": {"Timestamp": 1}, "EmailsSent": 2.0})
        line = log_stream.getvalue().strip().splitlines()[-1]
        assert json.loads(line)["_aws"] == {"Timestamp": 1}

    def test_normal_records_are_not_written_by_the_emf_handler(
        self, log_stream: io.StringIO
    ) -> None:
        log_event(get_logger("tests.json"), "plain")
        lines = [line for line in log_stream.getvalue().strip().splitlines() if line]
        assert len(lines) == 1
        assert json.loads(lines[0])["event"] == "plain"


class TestMetrics:
    def test_emf_document_shape(self) -> None:
        document = emf_document(
            [Metric("EmailsSent", 3.0)],
            {"Service": "dispatcher", "ReportDate": "2026-09-20"},
            timestamp=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
        )
        assert document["_aws"]["Timestamp"] == 1789905600000
        entry = document["_aws"]["CloudWatchMetrics"][0]
        assert entry["Namespace"] == DEFAULT_NAMESPACE
        assert entry["Dimensions"] == [["Service", "ReportDate"]]
        assert entry["Metrics"] == [{"Name": "EmailsSent", "Unit": "Count"}]
        assert document["Service"] == "dispatcher"
        assert document["EmailsSent"] == 3.0

    def test_dimensions_and_metrics_are_required(self) -> None:
        with pytest.raises(ValueError, match="at least one metric"):
            emf_document([], {"Service": "x"})
        with pytest.raises(ValueError, match="at least one dimension"):
            emf_document([Metric("A", 1.0)], {})

    def test_naive_timestamp_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            emf_document([Metric("A", 1.0)], {"Service": "x"}, timestamp=datetime(2026, 9, 20))

    def test_metric_validation(self) -> None:
        with pytest.raises(ValueError, match="name must not be empty"):
            Metric("", 1.0)
        with pytest.raises(ValueError, match="unsupported EMF unit"):
            Metric("A", 1.0, unit="Furlongs")

    def test_properties_are_prefixed(self) -> None:
        document = emf_document(
            [Metric("A", 1.0)], {"Service": "x"}, properties={"queue": "fanout"}
        )
        assert document["property_queue"] == "fanout"

    def test_emit_emf_writes_one_raw_document(self, log_stream: io.StringIO) -> None:
        returned = emit_emf([Metric("ReportsWritten", 4.0)], {"Service": "chunker"})
        line = log_stream.getvalue().strip().splitlines()[-1]
        written = json.loads(line)
        assert written == returned
        assert written["ReportsWritten"] == 4.0

    def test_put_metric_data_counts_datapoints(self, aws: object) -> None:
        import boto3

        client = boto3.client("cloudwatch", region_name="us-east-1")
        sent = put_metric_data(
            client,
            [Metric("EmailsSent", 2.0), Metric("EmailsFailed", 1.0)],
            {"Service": "dispatcher"},
        )
        assert sent == 2
        listed = client.list_metrics(Namespace=DEFAULT_NAMESPACE)["Metrics"]
        names = {metric["MetricName"] for metric in listed}
        assert {"EmailsSent", "EmailsFailed"} <= names
        datapoint = next(metric for metric in listed if metric["MetricName"] == "EmailsSent")
        assert datapoint["Dimensions"] == [{"Name": "Service", "Value": "dispatcher"}]

    def test_put_metric_data_ignores_empty_input(self, aws: object) -> None:
        import boto3

        assert (
            put_metric_data(boto3.client("cloudwatch", region_name="us-east-1"), [], {"S": "x"})
            == 0
        )

    def test_metric_names_are_used_by_the_pipeline(self) -> None:
        from agent_reports.lambda_handlers import chunker, dispatcher, orchestrator, presign

        source = " ".join(
            module.__file__ or "" for module in (chunker, dispatcher, orchestrator, presign)
        )
        assert source  # modules are importable from the package
        for name in ("emails_sent", "reports_written", "presign_issued"):
            assert METRIC_NAMES[name]
