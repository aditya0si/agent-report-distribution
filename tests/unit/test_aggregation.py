"""Streaming aggregation: the free-tier replacement for Spark, tested without any AWS at all."""

from __future__ import annotations

from decimal import Decimal

import pytest

from agent_reports.common.aggregation import ChunkerCapacityError, RawAggregator
from agent_reports.common.report import parse_report_totals

AGENT = {
    "agent_id": "AGT-000001",
    "agent_name": "Kabir Rao",
    "region": "West",
    "branch": "Pune",
    "email": "agt-000001@example.com",
}
POLICY_A = {
    "policy_id": "POL-0000000001",
    "agent_id": "AGT-000001",
    "product": "Individual Health",
    "policy_start": "2025-11-01",
    "policy_end": "2026-11-01",
    "sum_insured": "1000000.00",
    "premium": "12000.50",
    "commission_rate": "0.1000",
}
POLICY_B = {
    "policy_id": "POL-0000000002",
    "agent_id": "AGT-000001",
    "product": "Term Life",
    "policy_start": "2026-01-01",
    "policy_end": "2027-01-01",
    "sum_insured": "2000000.00",
    "premium": "8000.25",
    "commission_rate": "0.2000",
}
CLAIMS = [
    {
        "policy_id": "POL-0000000001",
        "agent_id": "AGT-000001",
        "claimed_amount": "3000.00",
        "settled_amount": "2500.00",
    },
    {
        "policy_id": "POL-0000000001",
        "agent_id": "AGT-000001",
        "claimed_amount": "1500.00",
        "settled_amount": "0.00",
    },
    {
        "policy_id": "POL-0000000002",
        "agent_id": "AGT-000001",
        "claimed_amount": "1200.50",
        "settled_amount": "1000.00",
    },
]


def build(*, agent_ids: set[str] | None = None, max_policies: int = 10) -> RawAggregator:
    aggregator = RawAggregator(agent_ids=agent_ids, max_policies=max_policies)
    aggregator.add_agent_row(AGENT)
    aggregator.add_policy_row(POLICY_A)
    aggregator.add_policy_row(POLICY_B)
    for claim in CLAIMS:
        aggregator.add_claim_row(claim)
    return aggregator


class TestAggregation:
    def test_commission_is_derived_from_the_contracted_rate(self) -> None:
        aggregator = build()
        report = aggregator.report_for("AGT-000001")
        rows = {row["policy_id"]: row for row in _rows(report) if row["row_type"] == "DETAIL"}
        assert Decimal(rows["POL-0000000001"]["commission"]) == Decimal("1200.05")
        assert Decimal(rows["POL-0000000002"]["commission"]) == Decimal("1600.05")

    def test_claims_roll_up_per_policy(self) -> None:
        rows = {row["policy_id"]: row for row in _rows(build().report_for("AGT-000001"))}
        assert rows["POL-0000000001"]["claim_count"] == "2"
        assert Decimal(rows["POL-0000000001"]["claim_amount"]) == Decimal("4500.00")
        assert Decimal(rows["POL-0000000001"]["settled_amount"]) == Decimal("2500.00")
        assert rows["POL-0000000002"]["claim_count"] == "1"

    def test_totals_row_matches_the_python_report_shaper(self) -> None:
        totals = parse_report_totals(build().report_for("AGT-000001"))
        assert totals.agent_id == "AGT-000001"
        assert totals.policy_count == 2
        assert totals.premium == Decimal("20000.75")
        assert totals.commission == Decimal("2800.10")
        assert totals.claim_count == 3
        assert totals.claim_amount == Decimal("5700.50")
        assert totals.settled_amount == Decimal("3500.00")
        assert totals.loss_ratio == Decimal("0.2850")

    def test_detail_rows_are_sorted_by_policy_id(self) -> None:
        rows = _rows(build().report_for("AGT-000001"))
        assert [row["policy_id"] for row in rows] == ["POL-0000000001", "POL-0000000002", ""]

    def test_agent_metadata_is_denormalised_into_detail_rows(self) -> None:
        rows = _rows(build().report_for("AGT-000001"))
        assert rows[0]["agent_name"] == "Kabir Rao"
        assert rows[0]["region"] == "West"
        assert rows[0]["branch"] == "Pune"

    def test_stats_count_what_was_read(self) -> None:
        stats = build().stats
        assert stats.rows_read == 6
        assert stats.rows_in_scope == 6
        assert stats.policies == 2
        assert stats.claims == 3
        assert stats.agents == 1

    def test_iter_reports_is_sorted(self) -> None:
        aggregator = build()
        aggregator.add_agent_row({**AGENT, "agent_id": "AGT-000002"})
        aggregator.add_policy_row(
            {**POLICY_A, "policy_id": "POL-0000000009", "agent_id": "AGT-000002"}
        )
        assert [agent_id for agent_id, _ in aggregator.iter_reports()] == [
            "AGT-000001",
            "AGT-000002",
        ]


class TestFilteringAndIdempotency:
    def test_agent_filter_excludes_other_agents(self) -> None:
        aggregator = build(agent_ids={"AGT-000002"})
        assert aggregator.agents_with_policies == []
        assert aggregator.stats.rows_in_scope == 0

    def test_duplicate_policy_rows_are_ignored(self) -> None:
        aggregator = build()
        assert aggregator.add_policy_row(POLICY_A) is False
        assert aggregator.stats.policies == 2

    def test_orphan_claims_are_counted_not_fatal(self) -> None:
        aggregator = build()
        assert (
            aggregator.add_claim_row(
                {
                    "policy_id": "POL-9999999999",
                    "agent_id": "AGT-000001",
                    "claimed_amount": "10.00",
                    "settled_amount": "0.00",
                }
            )
            is False
        )
        assert aggregator.stats.orphan_claims == 1

    def test_rows_without_ids_are_skipped(self) -> None:
        aggregator = RawAggregator()
        assert aggregator.add_agent_row({"agent_id": ""}) is False
        assert aggregator.add_policy_row({"agent_id": "AGT-000001", "policy_id": ""}) is False
        assert aggregator.add_claim_row({"agent_id": "AGT-000001"}) is False
        assert aggregator.stats.rows_in_scope == 0

    def test_report_for_unknown_agent_raises(self) -> None:
        with pytest.raises(ChunkerCapacityError, match="no policies in this shard"):
            build().report_for("AGT-000009")


class TestGuardrails:
    def test_commission_rounds_half_up_on_exact_ties(self) -> None:
        """23380.45 x 0.1000 = 2338.045 exactly: money rounds up, not to even (banker's rounding)."""
        aggregator = RawAggregator()
        aggregator.add_policy_row(
            {
                **POLICY_A,
                "policy_id": "POL-0000000003",
                "premium": "23380.45",
                "commission_rate": "0.1000",
            }
        )
        rows = {row["policy_id"]: row for row in _rows(aggregator.report_for("AGT-000001"))}
        assert rows["POL-0000000003"]["commission"] == "2338.05"

    def test_capacity_guardrail_trips_before_the_lambda_dies(self) -> None:
        aggregator = RawAggregator(max_policies=1)
        aggregator.add_policy_row(POLICY_A)
        with pytest.raises(ChunkerCapacityError) as excinfo:
            aggregator.add_policy_row(POLICY_B)
        assert excinfo.value.context["max_policies"] == 1
        assert excinfo.value.retryable is False

    def test_max_policies_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_policies"):
            RawAggregator(max_policies=0)


def _rows(csv_text: str) -> list[dict[str, str]]:
    import csv
    import io

    return list(csv.DictReader(io.StringIO(csv_text)))


class TestCommissionPrecision:
    """A rate is not money: it must not be rounded to paise before the multiplication.

    The Spark path reads ``commission_rate`` as ``DecimalType(9, 4)``; the chunker used to run it
    through ``to_decimal`` (2dp), so ``0.0750`` became ``0.08`` and the two paths disagreed on the
    same input - including on the TOTAL row and the emailed figure.
    """

    def test_sub_paise_rate_is_not_rounded_before_multiplying(self) -> None:
        """The reviewer's repro: 99999.99 x 0.0750 = 7499.99925 -> 7500.00, not 8000.00."""
        aggregator = RawAggregator()
        aggregator.add_policy_row(
            {
                **POLICY_A,
                "policy_id": "POL-0000000004",
                "premium": "99999.99",
                "commission_rate": "0.0750",
            }
        )
        rows = {row["policy_id"]: row for row in _rows(aggregator.report_for("AGT-000001"))}
        assert rows["POL-0000000004"]["commission"] == "7500.00"

    @pytest.mark.parametrize(
        "rate, expected",
        [
            ("0.0625", "625.00"),  # 10000.00 x 0.0625 - a 2dp rate of 0.06 would give 600.00
            ("0.1075", "1075.00"),
            ("0.1575", "1575.00"),
            ("0.1825", "1825.00"),
            ("0.07505", "751.00"),  # 5dp input: 4dp HALF_UP (0.0751), matching Spark's cast
            ("0.074949", "749.00"),  # ... rounds down to 0.0749 at 4dp
        ],
    )
    def test_rates_are_parsed_at_four_decimal_places(self, rate: str, expected: str) -> None:
        aggregator = RawAggregator()
        aggregator.add_policy_row(
            {
                **POLICY_A,
                "policy_id": "POL-0000000005",
                "premium": "10000.00",
                "commission_rate": rate,
            }
        )
        rows = {row["policy_id"]: row for row in _rows(aggregator.report_for("AGT-000001"))}
        assert rows["POL-0000000005"]["commission"] == expected

    def test_the_totals_row_carries_the_same_commission(self) -> None:
        aggregator = RawAggregator()
        aggregator.add_policy_row(
            {
                **POLICY_A,
                "policy_id": "POL-0000000006",
                "premium": "99999.99",
                "commission_rate": "0.0750",
            }
        )
        totals = parse_report_totals(aggregator.report_for("AGT-000001"))
        assert totals.commission == Decimal("7500.00")

    def test_to_rate_decimal_is_not_the_money_parser(self) -> None:
        from agent_reports.common.report import to_decimal, to_rate_decimal

        assert to_decimal("0.0750") == Decimal("0.08")
        assert to_rate_decimal("0.0750") == Decimal("0.0750")
        assert to_rate_decimal("") == Decimal("0.0000")
        assert to_rate_decimal(None) == Decimal("0.0000")


class TestClaimAttribution:
    """A claim belongs to the policy it points at - that is the join Spark performs."""

    def test_a_claim_with_a_foreign_agent_id_still_lands_on_its_policy(self) -> None:
        aggregator = build()
        assert (
            aggregator.add_claim_row(
                {
                    "policy_id": "POL-0000000002",
                    "agent_id": "AGT-000002",  # stale/denormalised: not this policy's owner
                    "claimed_amount": "100.00",
                    "settled_amount": "0.00",
                }
            )
            is True
        )
        rows = {row["policy_id"]: row for row in _rows(aggregator.report_for("AGT-000001"))}
        assert rows["POL-0000000002"]["claim_count"] == "2"
        assert aggregator.stats.orphan_claims == 0

    def test_a_claim_for_an_unknown_policy_is_an_orphan(self) -> None:
        aggregator = build()
        assert (
            aggregator.add_claim_row(
                {
                    "policy_id": "POL-9999999999",
                    "agent_id": "AGT-000001",
                    "claimed_amount": "10.00",
                    "settled_amount": "0.00",
                }
            )
            is False
        )
        assert aggregator.stats.orphan_claims == 1

    def test_a_claim_without_a_policy_id_is_skipped(self) -> None:
        aggregator = build()
        assert aggregator.add_claim_row({"agent_id": "AGT-000001"}) is False
        assert aggregator.stats.orphan_claims == 0
