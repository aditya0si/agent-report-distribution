"""Spark-job shaping helpers that do not need a JVM.

The heavy end-to-end Spark run lives in ``tests/integration/test_spark_job.py`` (marked
``requires_jvm``); these tests pin the column contract and the partition-column re-insertion, which
is the part of the job that is easy to get subtly wrong.
"""

from __future__ import annotations

import csv
import io

import pytest

from agent_reports.common.report import REPORT_COLUMNS
from agent_reports.emr.jobs.agent_report_job import insert_agent_column

#: What Spark's partitioned writer emits: the partition column is dropped from the file content.
#: The two DETAIL rows are deliberately in descending policy_id order - the finalizer must sort them.
SPARK_HEADER = (
    "row_type,agent_name,region,branch,policy_id,product,policy_start,policy_end,sum_insured,"
    "premium,commission,policy_count,claim_count,claim_amount,settled_amount,loss_ratio"
)
SPARK_DETAIL_2 = (
    "DETAIL,Meera Iyer,South,Chennai,POL-0000000002,Term Life,2026-01-01,2027-01-01,"
    "2000000.00,8000.25,1600.05,1,1,1200.50,1000.00,0.1500"
)
SPARK_DETAIL_1 = (
    "DETAIL,Meera Iyer,South,Chennai,POL-0000000001,Individual Health,2025-11-01,2026-11-01,"
    "1000000.00,12000.50,1200.05,1,2,3000.00,2500.00,0.2500"
)
SPARK_TOTAL = "TOTAL,,,,,,,,,20000.75,2800.10,2,3,4200.50,3500.00,0.2100"
SPARK_PART_FILE = "\n".join([SPARK_HEADER, SPARK_DETAIL_2, SPARK_DETAIL_1, SPARK_TOTAL]) + "\n"


def part_file(*lines: str) -> str:
    return "\n".join(lines) + "\n"


class TestInsertAgentColumn:
    def test_header_matches_the_canonical_columns(self) -> None:
        rendered = insert_agent_column(SPARK_PART_FILE, "AGT-000001")
        assert next(csv.reader(io.StringIO(rendered))) == list(REPORT_COLUMNS)

    def test_agent_id_is_filled_on_every_row(self) -> None:
        rows = list(csv.DictReader(io.StringIO(insert_agent_column(SPARK_PART_FILE, "AGT-000042"))))
        assert len(rows) == 3
        assert {row["agent_id"] for row in rows} == {"AGT-000042"}
        assert rows[0]["row_type"] == "DETAIL"
        assert rows[-1]["row_type"] == "TOTAL"

    def test_line_endings_are_lf_only(self) -> None:
        assert "\r\n" not in insert_agent_column(SPARK_PART_FILE, "AGT-000001")

    def test_output_is_parseable_by_the_dispatcher(self) -> None:
        from agent_reports.common.report import parse_report_totals

        totals = parse_report_totals(insert_agent_column(SPARK_PART_FILE, "AGT-000001"))
        assert totals.agent_id == "AGT-000001"
        assert str(totals.premium) == "20000.75"
        assert totals.policy_count == 2

    def test_missing_columns_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="missing report columns"):
            insert_agent_column(part_file("row_type,policy_id", "DETAIL,POL-1"), "AGT-000001")

    def test_already_partitioned_input_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="already contains agent_id"):
            insert_agent_column(
                part_file("row_type,agent_id,agent_name", "DETAIL,AGT-000001,Meera"), "AGT-000001"
            )

    def test_empty_report_is_rejected(self) -> None:
        header = ",".join(column for column in REPORT_COLUMNS if column != "agent_id")
        with pytest.raises(ValueError, match="has no rows"):
            insert_agent_column(part_file(header), "AGT-000001")

    def test_round_trip_is_stable(self) -> None:
        once = insert_agent_column(SPARK_PART_FILE, "AGT-000001")
        # Re-inserting into already-finalized output is a programming error, not silent corruption.
        with pytest.raises(ValueError, match="already contains agent_id"):
            insert_agent_column(once, "AGT-000001")

    def test_out_of_order_detail_rows_are_sorted(self) -> None:
        """Spark guarantees the file per agent, not the row order inside it."""
        rendered = insert_agent_column(SPARK_PART_FILE, "AGT-000001")
        rows = list(csv.DictReader(io.StringIO(rendered)))
        assert [row["policy_id"] for row in rows] == ["POL-0000000001", "POL-0000000002", ""]
        assert [row["row_type"] for row in rows] == ["DETAIL", "DETAIL", "TOTAL"]

    def test_rows_with_an_unknown_row_type_are_rejected(self) -> None:
        broken = part_file(SPARK_HEADER, SPARK_DETAIL_1.replace("DETAIL", "SUMMARY"), SPARK_TOTAL)
        with pytest.raises(ValueError, match="exactly one TOTAL row"):
            insert_agent_column(broken, "AGT-000001")

    def test_missing_total_row_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="exactly one TOTAL row"):
            insert_agent_column(part_file(SPARK_HEADER, SPARK_DETAIL_1), "AGT-000001")

    def test_duplicate_total_rows_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="exactly one TOTAL row"):
            insert_agent_column(
                part_file(SPARK_HEADER, SPARK_DETAIL_1, SPARK_TOTAL, SPARK_TOTAL), "AGT-000001"
            )
