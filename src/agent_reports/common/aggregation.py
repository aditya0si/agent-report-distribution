"""Pure-Python streaming aggregation: the free-tier replacement for the Spark step.

The chunker Lambda feeds raw rows in and gets per-agent report CSVs out, using the same
:mod:`agent_reports.common.report` shaping code that the tests compare against the PySpark job's
output. Memory is bounded by the number of *policies in the shard*, not by the number of raw rows,
because rows are streamed and only the running per-policy totals are kept.

Guardrail: :class:`ChunkerCapacityError` is raised when a shard would need more than
``max_policies`` in memory. That is the documented trigger to switch the day to the EMR path rather
than letting a Lambda die at the memory limit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from typing import Iterator, Mapping

from .errors import PermanentError
from .report import AgentTotals, render_report_csv, summarize_details, to_decimal

__all__ = ["ChunkerCapacityError", "PolicyRow", "RawAggregator"]


class ChunkerCapacityError(PermanentError):
    """The shard does not fit in the Lambda chunker; use the EMR/Spark path for this volume."""

    code = "ChunkerCapacityError"


@dataclass
class PolicyRow:
    """One policy with its running claim totals - the input to a DETAIL row."""

    agent_id: str
    policy_id: str
    product: str
    policy_start: str
    policy_end: str
    sum_insured: Decimal
    premium: Decimal
    commission: Decimal
    claim_count: int = 0
    claim_amount: Decimal = Decimal("0.00")
    settled_amount: Decimal = Decimal("0.00")

    def as_detail(self, agent: Mapping[str, str]) -> dict[str, str]:
        return {
            "agent_id": self.agent_id,
            "agent_name": agent.get("agent_name", ""),
            "region": agent.get("region", ""),
            "branch": agent.get("branch", ""),
            "policy_id": self.policy_id,
            "product": self.product,
            "policy_start": self.policy_start,
            "policy_end": self.policy_end,
            "sum_insured": self.sum_insured,
            "premium": self.premium,
            "commission": self.commission,
            "claim_count": self.claim_count,
            "claim_amount": self.claim_amount,
            "settled_amount": self.settled_amount,
        }


@dataclass
class AggregatorStats:
    rows_read: int = 0
    rows_in_scope: int = 0
    policies: int = 0
    claims: int = 0
    agents: int = 0
    orphan_claims: int = 0


class RawAggregator:
    """Accumulate raw rows for a set of agents, then render their reports."""

    def __init__(
        self,
        *,
        agent_ids: set[str] | None = None,
        max_policies: int = 2_000_000,
    ) -> None:
        if max_policies < 1:
            raise ValueError("max_policies must be >= 1")
        self.agent_ids = set(agent_ids) if agent_ids is not None else None
        self.max_policies = max_policies
        self._policies: dict[str, PolicyRow] = {}
        self._agents: dict[str, dict[str, str]] = {}
        self._agent_policies: dict[str, list[str]] = {}
        self.stats = AggregatorStats()

    # ------------------------------------------------------------------ ingest
    def _in_scope(self, agent_id: str) -> bool:
        return self.agent_ids is None or agent_id in self.agent_ids

    def add_agent_row(self, row: Mapping[str, str]) -> bool:
        self.stats.rows_read += 1
        agent_id = row.get("agent_id", "")
        if not agent_id or not self._in_scope(agent_id):
            return False
        self._agents[agent_id] = {
            "agent_name": row.get("agent_name", ""),
            "region": row.get("region", ""),
            "branch": row.get("branch", ""),
            "email": row.get("email", ""),
        }
        self.stats.rows_in_scope += 1
        self.stats.agents = len(self._agents)
        return True

    def add_policy_row(self, row: Mapping[str, str]) -> bool:
        self.stats.rows_read += 1
        agent_id = row.get("agent_id", "")
        policy_id = row.get("policy_id", "")
        if not agent_id or not policy_id or not self._in_scope(agent_id):
            return False
        if policy_id in self._policies:
            # Duplicate delivery of the same partition: keep the first occurrence (idempotent).
            return False
        if len(self._policies) >= self.max_policies:
            raise ChunkerCapacityError(
                "shard exceeds the chunker capacity guardrail; route this day to EMR",
                context={"max_policies": self.max_policies, "agent_ids": len(self.agent_ids or ())},
            )
        premium = to_decimal(row.get("premium", "0"))
        # HALF_UP (not the Decimal context default of HALF_EVEN) - commission is money, and the
        # Spark job's round() is HALF_UP too, so the two paths must agree on exact half-paise ties.
        commission = (premium * to_decimal(row.get("commission_rate", "0"))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        self._policies[policy_id] = PolicyRow(
            agent_id=agent_id,
            policy_id=policy_id,
            product=row.get("product", ""),
            policy_start=row.get("policy_start", ""),
            policy_end=row.get("policy_end", ""),
            sum_insured=to_decimal(row.get("sum_insured", "0")),
            premium=premium,
            commission=commission,
        )
        self._agent_policies.setdefault(agent_id, []).append(policy_id)
        self.stats.rows_in_scope += 1
        self.stats.policies = len(self._policies)
        return True

    def add_claim_row(self, row: Mapping[str, str]) -> bool:
        self.stats.rows_read += 1
        agent_id = row.get("agent_id", "")
        policy_id = row.get("policy_id", "")
        if not agent_id or not policy_id or not self._in_scope(agent_id):
            return False
        policy = self._policies.get(policy_id)
        if policy is None:
            # Claim for a policy outside this shard (or a policy row we never saw).
            self.stats.orphan_claims += 1
            return False
        policy.claim_count += 1
        policy.claim_amount += to_decimal(row.get("claimed_amount", "0"))
        policy.settled_amount += to_decimal(row.get("settled_amount", "0"))
        self.stats.rows_in_scope += 1
        self.stats.claims += 1
        return True

    # ------------------------------------------------------------------ output
    def report_for(self, agent_id: str) -> str:
        """Render one agent's CSV (DETAIL rows sorted by policy id, then the TOTAL row)."""
        policy_ids = sorted(self._agent_policies.get(agent_id, []))
        if not policy_ids:
            raise ChunkerCapacityError(
                "agent has no policies in this shard", context={"agent_id": agent_id}
            )
        details = [self._policies[pid].as_detail(self._agents.get(agent_id, {})) for pid in policy_ids]
        totals: AgentTotals = summarize_details(details)
        return render_report_csv(details, totals)

    def iter_reports(self) -> Iterator[tuple[str, str]]:
        """``(agent_id, csv_text)`` for every agent with at least one policy, sorted by id."""
        for agent_id in sorted(self._agent_policies):
            yield agent_id, self.report_for(agent_id)

    @property
    def agents_with_policies(self) -> list[str]:
        return sorted(self._agent_policies)

    def totals_for(self, agent_id: str) -> AgentTotals:
        return summarize_details(
            [
                self._policies[pid].as_detail(self._agents.get(agent_id, {}))
                for pid in sorted(self._agent_policies.get(agent_id, []))
            ]
        )
