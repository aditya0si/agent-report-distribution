"""Key conventions: every builder/parser pair round-trips, and every parser rejects junk."""

from __future__ import annotations

import pytest

from agent_reports.common import keys


class TestRawKeys:
    def test_raw_key_round_trip(self) -> None:
        key = keys.raw_key("2026-09-20", "policies", 3)
        assert key == "raw/dt=2026-09-20/source=policies/part-00003.csv"
        parsed = keys.parse_raw_key(key)
        assert parsed is not None
        assert (parsed.report_date, parsed.source, parsed.part, parsed.extension) == (
            "2026-09-20",
            "policies",
            3,
            "csv",
        )

    def test_parquet_extension(self) -> None:
        assert keys.raw_key("2026-09-20", "claims", 0, "parquet").endswith("part-00000.parquet")

    @pytest.mark.parametrize(
        "key",
        [
            "raw/dt=2026-09-20/source=unknown/part-00000.csv",
            "raw/dt=2026-9-2/source=claims/part-00000.csv",
            "raw/dt=2026-09-20/source=claims/part-00000.json",
            "raw/dt=2026-09-20/source=claims/part-0.csv",
            "reports/dt=2026-09-20/agent_id=AGT-000001/report.csv",
            "raw/dt=2026-09-20/_dataset_manifest.json",
            "",
        ],
    )
    def test_parse_raw_key_rejects(self, key: str) -> None:
        assert keys.parse_raw_key(key) is None

    def test_invalid_source_raises(self) -> None:
        with pytest.raises(ValueError, match="source must be one of"):
            keys.raw_key("2026-09-20", "commissions", 0)

    def test_invalid_date_raises(self) -> None:
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            keys.raw_key("20-09-2026", "agents", 0)

    def test_impossible_calendar_date_raises(self) -> None:
        with pytest.raises(ValueError, match="not a real calendar date"):
            keys.validate_report_date("2026-02-30")

    def test_negative_part_raises(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            keys.raw_key("2026-09-20", "agents", -1)


class TestReportKeys:
    def test_report_key_layout(self) -> None:
        key = keys.report_key("2026-09-20", "AGT-000123")
        assert key == "reports/dt=2026-09-20/agent_id=AGT-000123/report.csv"
        assert keys.parse_report_key(key) == keys.AgentReportKey("2026-09-20", "AGT-000123")

    def test_partition_prefix_has_trailing_slash(self) -> None:
        assert keys.report_partition_prefix("2026-09-20", "AGT-000001").endswith("/")

    @pytest.mark.parametrize(
        "key",
        [
            "reports/dt=2026-09-20/agent_id=AGT-000123/part-00000.csv",
            "reports/dt=2026-09-20/agent_id=AGT-123/report.csv",
            "reports/dt=2026-09-20/agent_id=../../etc/passwd/report.csv",
            "reports/2026-09-20/agent_id=AGT-000123/report.csv",
        ],
    )
    def test_parse_report_key_rejects(self, key: str) -> None:
        assert keys.parse_report_key(key) is None

    def test_agent_id_validation_blocks_traversal(self) -> None:
        with pytest.raises(ValueError, match="agent_id must look like"):
            keys.validate_agent_id("../../etc/passwd")

    def test_agent_ids_from_report_keys(self) -> None:
        listed = [
            "reports/dt=2026-09-20/agent_id=AGT-000002/report.csv",
            "reports/dt=2026-09-20/agent_id=AGT-000001/report.csv",
            "reports/dt=2026-09-20/agent_id=AGT-000002/report.csv",
            "reports/dt=2026-09-20/_job_summary.json",
            "reports/dt=2026-09-20/agent_id=AGT-000001/part-00000.csv",
        ]
        assert keys.agent_ids_from_report_keys(listed) == ["AGT-000001", "AGT-000002"]


class TestStateKeys:
    def test_manifest_key(self) -> None:
        assert keys.manifest_key("2026-09-20") == "state/runs/dt=2026-09-20/manifest.json"

    def test_dispatch_marker_round_trip(self) -> None:
        key = keys.dispatch_marker_key("2026-09-20", "AGT-000042")
        assert key == "state/dispatch/dt=2026-09-20/agent_id=AGT-000042.json"
        parsed = keys.parse_dispatch_marker_key(key)
        assert parsed == keys.AgentReportKey("2026-09-20", "AGT-000042")

    def test_dispatch_marker_rejects_other_prefixes(self) -> None:
        assert keys.parse_dispatch_marker_key("state/runs/dt=2026-09-20/manifest.json") is None
        assert (
            keys.parse_dispatch_marker_key("state/dispatch/dt=2026-09-20/AGT-000042.json") is None
        )

    def test_sources_constant_matches_schema(self) -> None:
        from agent_reports.ingest.schema import SOURCE_COLUMNS

        assert set(keys.SOURCES) == set(SOURCE_COLUMNS)
