"""Report shaping: money formatting, totals, and the CSV the dispatcher reads back."""

from __future__ import annotations

import csv
import io
from decimal import Decimal

import pytest

from agent_reports.common.report import (
    DETAIL,
    REPORT_COLUMNS,
    TOTAL,
    AgentTotals,
    format_money,
    loss_ratio,
    parse_report_totals,
    render_report_csv,
    summarize_details,
    to_decimal,
)

DETAILS = [
    {
        "agent_id": "AGT-000001",
        "agent_name": "Meera Iyer",
        "region": "South",
        "branch": "Chennai",
        "policy_id": "POL-0000000002",
        "product": "Term Life",
        "policy_start": "2026-01-01",
        "policy_end": "2027-01-01",
        "sum_insured": "2000000.00",
        "premium": "8000.25",
        "commission": "1600.05",
        "claim_count": 1,
        "claim_amount": "1200.50",
        "settled_amount": "1000.00",
    },
    {
        "agent_id": "AGT-000001",
        "agent_name": "Meera Iyer",
        "region": "South",
        "branch": "Chennai",
        "policy_id": "POL-0000000001",
        "product": "Individual Health",
        "policy_start": "2025-11-01",
        "policy_end": "2026-11-01",
        "sum_insured": "1000000.00",
        "premium": "12000.50",
        "commission": "1200.05",
        "claim_count": 2,
        "claim_amount": "3000.00",
        "settled_amount": "2500.00",
    },
]


class TestMoney:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("12000.5", "12000.50"),
            (12000.499, "12000.50"),
            (0, "0.00"),
            ("", "0.00"),
            (Decimal("1.005"), "1.01"),
        ],
    )
    def test_format_money(self, value: object, expected: str) -> None:
        assert format_money(value) == expected  # type: ignore[arg-type]

    def test_to_decimal_rounds_half_up(self) -> None:
        assert to_decimal("2.345") == Decimal("2.35")

    def test_loss_ratio_basic(self) -> None:
        assert loss_ratio("1500.00", "10000.00") == Decimal("0.1500")

    def test_loss_ratio_without_premium_is_zero(self) -> None:
        assert loss_ratio("1500.00", "0") == Decimal("0.0000")

    def test_loss_ratio_is_capped_by_precision(self) -> None:
        assert loss_ratio("1.00", "3.00") == Decimal("0.3333")


class TestSummarize:
    def test_totals_add_up(self) -> None:
        totals = summarize_details(DETAILS)
        assert totals.agent_id == "AGT-000001"
        assert totals.policy_count == 2
        assert totals.premium == Decimal("20000.75")
        assert totals.commission == Decimal("2800.10")
        assert totals.claim_count == 3
        assert totals.claim_amount == Decimal("4200.50")
        assert totals.settled_amount == Decimal("3500.00")
        assert totals.loss_ratio == Decimal("0.2100")

    def test_empty_report_rejected(self) -> None:
        with pytest.raises(ValueError, match="empty report"):
            summarize_details([])

    def test_mixed_agents_rejected(self) -> None:
        rows = [dict(DETAILS[0]), {**DETAILS[1], "agent_id": "AGT-000002"}]
        with pytest.raises(ValueError, match="multiple agents"):
            summarize_details(rows)

    def test_email_context_formats_numbers(self) -> None:
        context = summarize_details(DETAILS).as_email_context()
        assert context["premium"] == "20000.75"
        assert context["policy_count"] == "2"
        assert context["loss_ratio"] == "21.00%"


class TestRender:
    def test_header_is_the_canonical_column_set(self) -> None:
        text = render_report_csv(DETAILS, summarize_details(DETAILS))
        header = next(csv.reader(io.StringIO(text)))
        assert header == list(REPORT_COLUMNS)

    def test_rows_are_sorted_by_caller_and_total_is_last(self) -> None:
        details = sorted(DETAILS, key=lambda row: str(row["policy_id"]))
        text = render_report_csv(details, summarize_details(details))
        rows = list(csv.DictReader(io.StringIO(text)))
        assert [row["row_type"] for row in rows] == [DETAIL, DETAIL, TOTAL]
        assert [row["policy_id"] for row in rows] == ["POL-0000000001", "POL-0000000002", ""]

    def test_detail_row_rendering(self) -> None:
        details = sorted(DETAILS, key=lambda row: str(row["policy_id"]))
        rows = list(
            csv.DictReader(io.StringIO(render_report_csv(details, summarize_details(details))))
        )
        first = rows[0]
        assert first["agent_id"] == "AGT-000001"
        assert first["policy_count"] == "1"
        assert first["claim_count"] == "2"
        assert first["premium"] == "12000.50"
        assert first["loss_ratio"] == "0.2500"

    def test_total_row_rendering(self) -> None:
        text = render_report_csv(DETAILS, summarize_details(DETAILS))
        total = next(row for row in csv.DictReader(io.StringIO(text)) if row["row_type"] == TOTAL)
        assert total["premium"] == "20000.75"
        assert total["policy_count"] == "2"
        assert total["claim_count"] == "3"
        assert total["loss_ratio"] == "0.2100"
        assert total["policy_id"] == ""
        assert total["agent_name"] == ""

    def test_empty_details_rejected(self) -> None:
        totals = AgentTotals(
            agent_id="AGT-000001",
            policy_count=0,
            premium=Decimal("0.00"),
            commission=Decimal("0.00"),
            claim_count=0,
            claim_amount=Decimal("0.00"),
            settled_amount=Decimal("0.00"),
            loss_ratio=Decimal("0.0000"),
        )
        with pytest.raises(ValueError, match="at least one policy row"):
            render_report_csv([], totals)

    def test_render_parse_round_trip(self) -> None:
        text = render_report_csv(DETAILS, summarize_details(DETAILS))
        assert parse_report_totals(text) == summarize_details(DETAILS)

    def test_parse_rejects_a_foreign_header(self) -> None:
        with pytest.raises(ValueError, match="unexpected report header"):
            parse_report_totals("a,b\n1,2\n")

    def test_parse_requires_a_total_row(self) -> None:
        text = render_report_csv(DETAILS, summarize_details(DETAILS))
        without_total = "\n".join(line for line in text.splitlines() if not line.startswith(TOTAL))
        with pytest.raises(ValueError, match="no TOTAL row"):
            parse_report_totals(without_total)

    def test_line_terminator_is_lf_only(self) -> None:
        text = render_report_csv(DETAILS, summarize_details(DETAILS))
        assert "\r\n" not in text
