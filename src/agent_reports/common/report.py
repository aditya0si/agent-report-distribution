"""Canonical report shape shared by the PySpark job and the Lambda chunker.

Both the EMR path and the free-tier path must produce byte-identical CSVs for the same input - that
is what lets the cheap path be a drop-in replacement for modest volumes, and it is asserted by
``tests/integration/test_spark_job.py``.

Column set (``REPORT_COLUMNS``)::

    row_type       DETAIL | TOTAL
    agent_id       AGT-000123
    agent_name     synthetic display name
    region, branch agent's org unit
    policy_id      POL-0000000000 (empty on the TOTAL row)
    product        Individual Health | Group Health | Personal Accident | Term Life
    policy_start, policy_end
    sum_insured    cover in INR
    premium        gross written premium in INR
    commission     premium x the policy's contracted commission rate
    policy_count   1 on DETAIL rows, number of policies on the TOTAL row
    claim_count    claims filed against the policy (0 when none)
    claim_amount   claimed amount in INR
    settled_amount approved/settled amount in INR
    loss_ratio     claim_amount / premium (0 when premium is 0)
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

__all__ = [
    "DETAIL",
    "REPORT_COLUMNS",
    "TOTAL",
    "AgentTotals",
    "format_money",
    "loss_ratio",
    "parse_report_totals",
    "render_report_csv",
    "summarize_details",
]

DETAIL = "DETAIL"
TOTAL = "TOTAL"

REPORT_COLUMNS: tuple[str, ...] = (
    "row_type",
    "agent_id",
    "agent_name",
    "region",
    "branch",
    "policy_id",
    "product",
    "policy_start",
    "policy_end",
    "sum_insured",
    "premium",
    "commission",
    "policy_count",
    "claim_count",
    "claim_amount",
    "settled_amount",
    "loss_ratio",
)

MONEY_PLACES = Decimal("0.01")
RATIO_PLACES = Decimal("0.0001")


@dataclass(frozen=True)
class AgentTotals:
    """Agent-level roll-up carried on the TOTAL row and used to personalise the email."""

    agent_id: str
    policy_count: int
    premium: Decimal
    commission: Decimal
    claim_count: int
    claim_amount: Decimal
    settled_amount: Decimal
    loss_ratio: Decimal

    def as_email_context(self) -> dict[str, str]:
        """Human-facing strings for the email template."""
        return {
            "agent_id": self.agent_id,
            "policy_count": f"{self.policy_count:,}",
            "premium": format_money(self.premium),
            "commission": format_money(self.commission),
            "claim_count": f"{self.claim_count:,}",
            "claim_amount": format_money(self.claim_amount),
            "settled_amount": format_money(self.settled_amount),
            "loss_ratio": f"{(self.loss_ratio * 100):.2f}%",
        }


def to_decimal(value: Any) -> Decimal:
    """Parse a CSV cell into a 2dp Decimal."""
    if value is None or value == "":
        return Decimal("0.00")
    return Decimal(str(value)).quantize(MONEY_PLACES, rounding=ROUND_HALF_UP)


def format_money(value: Decimal | float | int | str | None) -> str:
    """Render money with exactly two decimals (stable across platforms)."""
    return str(to_decimal(value))


def loss_ratio(claim_amount: Decimal | float | str, premium: Decimal | float | str) -> Decimal:
    """Claim-to-premium ratio, 4dp; 0 when the policy/agent has no premium."""
    premium_dec = to_decimal(premium)
    if premium_dec == 0:
        return Decimal("0.0000")
    ratio = to_decimal(claim_amount) / premium_dec
    return ratio.quantize(RATIO_PLACES, rounding=ROUND_HALF_UP)


def summarize_details(details: Sequence[Mapping[str, Any]]) -> AgentTotals:
    """Roll up DETAIL rows into the TOTAL row."""
    if not details:
        raise ValueError("cannot summarise an empty report")
    agent_ids = {str(row["agent_id"]) for row in details}
    if len(agent_ids) != 1:
        raise ValueError(f"report rows span multiple agents: {sorted(agent_ids)}")
    premium = sum((to_decimal(row["premium"]) for row in details), Decimal("0.00"))
    commission = sum((to_decimal(row["commission"]) for row in details), Decimal("0.00"))
    claim_amount = sum((to_decimal(row["claim_amount"]) for row in details), Decimal("0.00"))
    settled = sum((to_decimal(row["settled_amount"]) for row in details), Decimal("0.00"))
    claim_count = sum(int(row["claim_count"] or 0) for row in details)
    return AgentTotals(
        agent_id=agent_ids.pop(),
        policy_count=len(details),
        premium=premium,
        commission=commission,
        claim_count=claim_count,
        claim_amount=claim_amount,
        settled_amount=settled,
        loss_ratio=loss_ratio(claim_amount, premium),
    )


def render_report_csv(
    details: Iterable[Mapping[str, Any]],
    totals: AgentTotals,
) -> str:
    """Render the full per-agent CSV: DETAIL rows then exactly one TOTAL row."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(REPORT_COLUMNS), lineterminator="\n")
    writer.writeheader()
    detail_count = 0
    for row in details:
        writer.writerow(_detail_row(row))
        detail_count += 1
    if detail_count == 0:
        raise ValueError("a report must contain at least one policy row")
    writer.writerow(_total_row(totals))
    return buffer.getvalue()


def _detail_row(row: Mapping[str, Any]) -> dict[str, str]:
    return {
        "row_type": DETAIL,
        "agent_id": str(row["agent_id"]),
        "agent_name": str(row.get("agent_name", "")),
        "region": str(row.get("region", "")),
        "branch": str(row.get("branch", "")),
        "policy_id": str(row["policy_id"]),
        "product": str(row.get("product", "")),
        "policy_start": str(row.get("policy_start", "")),
        "policy_end": str(row.get("policy_end", "")),
        "sum_insured": format_money(row.get("sum_insured", 0)),
        "premium": format_money(row.get("premium", 0)),
        "commission": format_money(row.get("commission", 0)),
        "policy_count": "1",
        "claim_count": str(int(row.get("claim_count") or 0)),
        "claim_amount": format_money(row.get("claim_amount", 0)),
        "settled_amount": format_money(row.get("settled_amount", 0)),
        "loss_ratio": str(loss_ratio(row.get("claim_amount", 0), row.get("premium", 0))),
    }


def _total_row(totals: AgentTotals) -> dict[str, str]:
    return {
        "row_type": TOTAL,
        "agent_id": totals.agent_id,
        "agent_name": "",
        "region": "",
        "branch": "",
        "policy_id": "",
        "product": "",
        "policy_start": "",
        "policy_end": "",
        "sum_insured": "",
        "premium": format_money(totals.premium),
        "commission": format_money(totals.commission),
        "policy_count": str(totals.policy_count),
        "claim_count": str(totals.claim_count),
        "claim_amount": format_money(totals.claim_amount),
        "settled_amount": format_money(totals.settled_amount),
        "loss_ratio": str(totals.loss_ratio),
    }


def parse_report_totals(csv_text: str) -> AgentTotals:
    """Read the TOTAL row back out of a rendered report (used by the dispatcher for email copy)."""
    reader = csv.DictReader(io.StringIO(csv_text))
    if reader.fieldnames is None or list(reader.fieldnames) != list(REPORT_COLUMNS):
        raise ValueError(f"unexpected report header: {reader.fieldnames!r}")
    for row in reader:
        if row.get("row_type") != TOTAL:
            continue
        return AgentTotals(
            agent_id=str(row["agent_id"]),
            policy_count=int(row["policy_count"]),
            premium=to_decimal(row["premium"]),
            commission=to_decimal(row["commission"]),
            claim_count=int(row["claim_count"]),
            claim_amount=to_decimal(row["claim_amount"]),
            settled_amount=to_decimal(row["settled_amount"]),
            loss_ratio=Decimal(str(row["loss_ratio"])),
        )
    raise ValueError("report has no TOTAL row")
