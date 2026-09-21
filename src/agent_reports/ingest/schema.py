"""Documented schema for the synthetic raw dataset (three sources, one date partition).

The data is generated, not extracted: no customer, agent or claim here refers to a real person or a
real insurer. Agent emails use the reserved ``example.com`` domain, customer ids are opaque
synthetic keys, and diagnosis fields are coarse ICD-10 chapter codes rather than free text, so
nothing PII-shaped is ever written.

``agents`` (day roster, one row per agent)
------------------------------------------
================  =========  =========================================================
column            type       notes
================  =========  =========================================================
agent_id          string     ``AGT-\\d{6}`` - primary key, also the S3 partition value
agent_name        string     synthetic display name
email             string     ``agt-000123@example.com`` (non-routable by design)
region            string     North | South | East | West | Central
branch            string     city-level office
manager_id        string     agent_id of the reporting manager (``AGT-`` id)
joined_on         date       ISO date the agent was onboarded
================  =========  =========================================================

``policies`` (written premium per policy)
-----------------------------------------
=================  =========  ========================================================
column             type       notes
=================  =========  ========================================================
policy_id          string     ``POL-\\d{10}`` - primary key
agent_id           string     FK -> agents.agent_id
customer_id        string     opaque synthetic key ``CUS-\\d{9}``
product            string     Individual Health | Group Health | Personal Accident | Term Life
policy_start       date       ISO date
policy_end         date       ISO date (start + 12 months)
sum_insured        decimal    cover in INR
premium            decimal    gross written premium in INR
commission_rate    decimal    contracted rate for the product (0.06 - 0.20)
status             string     Active | Lapsed | Renewed
=================  =========  ========================================================

``claims`` (zero or more per policy)
------------------------------------
=================  =========  ========================================================
column             type       notes
=================  =========  ========================================================
claim_id           string     ``CLM-\\d{10}`` - primary key
policy_id          string     FK -> policies.policy_id
agent_id           string     FK -> agents.agent_id (denormalised for shuffle-free aggregation)
claim_date         date       ISO date
claim_type         string     Cashless | Reimbursement
diagnosis_chapter  string     ICD-10 chapter code (e.g. ``J`` respiratory)
claimed_amount     decimal    amount claimed in INR
settled_amount     decimal    amount paid in INR (0 while pending / when rejected)
status             string     Settled | Approved | Rejected | Pending
tat_days           integer    turnaround time in days
hospital_tier      string     Tier-1 | Tier-2 | Tier-3
=================  =========  ========================================================

Key conventions for the raw zone live in :mod:`agent_reports.common.keys`
(``raw/dt=YYYY-MM-DD/source=<source>/part-NNNNN.<ext>``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

__all__ = [
    "AGENT_COLUMNS",
    "CLAIM_COLUMNS",
    "POLICY_COLUMNS",
    "PRODUCTS",
    "REGIONS",
    "SOURCE_COLUMNS",
    "ProductSpec",
]

AGENT_COLUMNS: Final[tuple[str, ...]] = (
    "agent_id",
    "agent_name",
    "email",
    "region",
    "branch",
    "manager_id",
    "joined_on",
)

POLICY_COLUMNS: Final[tuple[str, ...]] = (
    "policy_id",
    "agent_id",
    "customer_id",
    "product",
    "policy_start",
    "policy_end",
    "sum_insured",
    "premium",
    "commission_rate",
    "status",
)

CLAIM_COLUMNS: Final[tuple[str, ...]] = (
    "claim_id",
    "policy_id",
    "agent_id",
    "claim_date",
    "claim_type",
    "diagnosis_chapter",
    "claimed_amount",
    "settled_amount",
    "status",
    "tat_days",
    "hospital_tier",
)

SOURCE_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "agents": AGENT_COLUMNS,
    "policies": POLICY_COLUMNS,
    "claims": CLAIM_COLUMNS,
}

#: Region -> branch offices (synthetic org structure; Indian city names, no real offices claimed).
REGIONS: Final[dict[str, tuple[str, ...]]] = {
    "North": ("Delhi", "Jaipur", "Lucknow", "Chandigarh"),
    "South": ("Bengaluru", "Chennai", "Hyderabad", "Kochi"),
    "East": ("Kolkata", "Patna", "Bhubaneswar", "Guwahati"),
    "West": ("Mumbai", "Pune", "Ahmedabad", "Surat"),
    "Central": ("Bhopal", "Indore", "Nagpur", "Raipur"),
}

CLAIM_TYPES: Final[tuple[str, ...]] = ("Cashless", "Reimbursement")
CLAIM_STATUSES: Final[tuple[float, ...]] = (0.55, 0.20, 0.15, 0.10)  # Settled, Approved, Rejected, Pending
CLAIM_STATUS_NAMES: Final[tuple[str, ...]] = ("Settled", "Approved", "Rejected", "Pending")
POLICY_STATUSES: Final[tuple[str, ...]] = ("Active", "Renewed", "Lapsed")
HOSPITAL_TIERS: Final[tuple[str, ...]] = ("Tier-1", "Tier-2", "Tier-3")

#: ICD-10 chapters, coarse enough to stay non-identifying.
DIAGNOSIS_CHAPTERS: Final[tuple[str, ...]] = (
    "A", "C", "E", "F", "I", "J", "K", "M", "N", "O", "S", "Z",
)

FIRST_NAMES: Final[tuple[str, ...]] = (
    "Aarav", "Isha", "Rohan", "Meera", "Kabir", "Ananya", "Vikram", "Priya",
    "Dev", "Nisha", "Arjun", "Kavya", "Rahul", "Sneha", "Manish", "Divya",
)
LAST_NAMES: Final[tuple[str, ...]] = (
    "Sharma", "Iyer", "Patel", "Nair", "Reddy", "Bose", "Chauhan", "Desai",
    "Gupta", "Kulkarni", "Mehta", "Rao", "Singh", "Verma", "Joshi", "Pillai",
)


@dataclass(frozen=True)
class ProductSpec:
    """Underwriting parameters per product line - the shape of the premium/claim distribution."""

    name: str
    commission_rate: float
    sum_insured_min: int
    sum_insured_max: int
    premium_rate_min: float
    premium_rate_max: float
    claim_probability: float
    max_claims_per_policy: int
    severity_mean: float
    severity_sigma: float


PRODUCTS: Final[tuple[ProductSpec, ...]] = (
    ProductSpec(
        name="Individual Health",
        commission_rate=0.10,
        sum_insured_min=300_000,
        sum_insured_max=2_000_000,
        premium_rate_min=0.012,
        premium_rate_max=0.035,
        claim_probability=0.30,
        max_claims_per_policy=3,
        severity_mean=0.18,
        severity_sigma=0.60,
    ),
    ProductSpec(
        name="Group Health",
        commission_rate=0.06,
        sum_insured_min=1_000_000,
        sum_insured_max=5_000_000,
        premium_rate_min=0.008,
        premium_rate_max=0.020,
        claim_probability=0.45,
        max_claims_per_policy=4,
        severity_mean=0.22,
        severity_sigma=0.70,
    ),
    ProductSpec(
        name="Personal Accident",
        commission_rate=0.15,
        sum_insured_min=500_000,
        sum_insured_max=5_000_000,
        premium_rate_min=0.002,
        premium_rate_max=0.006,
        claim_probability=0.05,
        max_claims_per_policy=1,
        severity_mean=0.30,
        severity_sigma=0.80,
    ),
    ProductSpec(
        name="Term Life",
        commission_rate=0.20,
        sum_insured_min=1_000_000,
        sum_insured_max=10_000_000,
        premium_rate_min=0.004,
        premium_rate_max=0.012,
        claim_probability=0.03,
        max_claims_per_policy=1,
        severity_mean=0.45,
        severity_sigma=0.90,
    ),
)

PRODUCTS_BY_NAME: Final[dict[str, ProductSpec]] = {p.name: p for p in PRODUCTS}
