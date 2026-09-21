"""The PySpark job, executed for real on a small local dataset.

Two things this proves that nothing else can:

1. the EMR artifact actually runs (a real ``SparkSession`` on ``local[2]``, real shuffle, real CSV
   write, real Hadoop FS rename);
2. it produces **byte-identical** output to the free-tier chunker, so switching between the cheap
   path and the EMR path cannot change what an agent receives.

Spark needs a JVM and (on Windows) ``HADOOP_HOME`` pointing at ``winutils.exe``/``hadoop.dll``; see
``docs/RUNBOOK.md``. When no JVM is present the ``spark`` fixture prints a loud banner and skips -
CI installs one with ``actions/setup-java`` so these tests always run there.
"""

from __future__ import annotations

import csv
import io
import json
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

from agent_reports.common.keys import parse_report_key
from agent_reports.common.report import REPORT_COLUMNS
from agent_reports.common.roster import read_roster
from agent_reports.common.storage import LocalStorage, Storage, Zones
from agent_reports.emr.jobs.agent_report_job import run_job
from agent_reports.ingest.generator import DatasetConfig, generate_dataset
from agent_reports.lambda_handlers.chunker import run_chunker

REPORT_DATE = "2026-09-20"


@pytest.fixture
def dataset(tmp_path: Path, settings: object) -> tuple[Path, Zones]:
    """A local filesystem dataset plus zones rooted at the same place (no S3 needed for Spark)."""
    root = tmp_path / "zones"
    zones = Zones(
        raw=LocalStorage(root / "raw"),
        processed=LocalStorage(root / "processed"),
        reports=LocalStorage(root / "reports"),
    )
    generate_dataset(
        DatasetConfig(report_date=REPORT_DATE, agents=40, policies_per_agent=4, seed=17),
        zones.raw,
    )
    return root, zones


def test_spark_job_writes_one_report_per_agent(spark: object, dataset: tuple[Path, Zones]) -> None:
    root, zones = dataset
    summary = run_job(
        spark,
        raw_uri=(root / "raw").as_uri(),
        reports_uri=(root / "out").as_uri(),
        report_date=REPORT_DATE,
    )
    assert summary["rows_in"] > 0
    assert summary["agents_reported"] == len(read_roster(zones.raw, REPORT_DATE))
    assert summary["spark_master"].startswith("local")
    assert summary["duration_seconds"] > 0

    out_root = root / "out" / "reports" / f"dt={REPORT_DATE}"
    agent_dirs = sorted(path for path in out_root.iterdir() if path.is_dir())
    assert len(agent_dirs) == summary["agents_reported"]
    for agent_dir in agent_dirs:
        files = sorted(path.name for path in agent_dir.iterdir() if path.suffix == ".csv")
        assert files == ["report.csv"], agent_dir

    summary_path = out_root / "_job_summary.json"
    assert (
        json.loads(summary_path.read_text(encoding="utf-8"))["agents_reported"]
        == summary["agents_reported"]
    )


def test_spark_output_rows_are_ordered_and_shaped(
    spark: object, dataset: tuple[Path, Zones]
) -> None:
    root, _ = dataset
    run_job(
        spark,
        raw_uri=(root / "raw").as_uri(),
        reports_uri=(root / "out").as_uri(),
        report_date=REPORT_DATE,
    )
    out_root = root / "out" / "reports" / f"dt={REPORT_DATE}"
    report = sorted(out_root.glob("agent_id=*/report.csv"))[0]
    text = report.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(text))
    assert list(reader.fieldnames or []) == list(REPORT_COLUMNS)
    rows = list(reader)
    assert rows[0]["row_type"] == "DETAIL"
    assert rows[-1]["row_type"] == "TOTAL"
    assert [row["policy_id"] for row in rows[:-1]] == sorted(row["policy_id"] for row in rows[:-1])
    assert rows[-1]["policy_count"] == str(len(rows) - 1)
    assert rows[-1]["policy_id"] == ""


def test_spark_output_is_byte_identical_to_the_chunker(
    spark: object, dataset: tuple[Path, Zones], settings: object
) -> None:
    """The two aggregation implementations must be interchangeable.

    The two outputs are written to separate roots on purpose: writing them to the same path would
    let the second writer overwrite the first and the comparison would pass for the wrong reason.
    """
    root, zones = dataset
    spark_out = root / "spark_out"
    chunker_out = root / "chunker_out"
    run_job(
        spark,
        raw_uri=(root / "raw").as_uri(),
        reports_uri=spark_out.as_uri(),
        report_date=REPORT_DATE,
    )
    chunker_zones = Zones(
        raw=zones.raw,
        processed=zones.processed,
        reports=LocalStorage(chunker_out),
    )
    chunker_result = run_chunker(
        settings,  # type: ignore[arg-type]
        report_date=REPORT_DATE,
        zones=chunker_zones,
        emit_metrics=False,
    )
    assert chunker_result.reports_written > 0

    spark_root = spark_out / "reports" / f"dt={REPORT_DATE}"
    spark_reports = {
        path.parent.name.split("=", 1)[1]: path for path in spark_root.glob("agent_id=*/report.csv")
    }
    chunker_reports = {
        parsed.agent_id: key
        for key in chunker_zones.reports.list_keys(f"reports/dt={REPORT_DATE}/")
        if (parsed := parse_report_key(key)) is not None
    }
    assert set(spark_reports) == set(chunker_reports)
    assert len(spark_reports) == chunker_result.reports_written

    for agent_id, spark_path in spark_reports.items():
        spark_bytes = spark_path.read_bytes()
        chunker_bytes = chunker_zones.reports.get_bytes(chunker_reports[agent_id])
        assert spark_bytes == chunker_bytes, f"mismatch for {agent_id}"
        assert spark_bytes.decode("utf-8").startswith("row_type,agent_id,agent_name")


def test_spark_job_totals_match_the_raw_partitions(
    spark: object, dataset: tuple[Path, Zones]
) -> None:
    root, _ = dataset
    run_job(
        spark,
        raw_uri=(root / "raw").as_uri(),
        reports_uri=(root / "out").as_uri(),
        report_date=REPORT_DATE,
    )
    out_root = root / "out" / "reports" / f"dt={REPORT_DATE}"
    report = sorted(out_root.glob("agent_id=*/report.csv"))[0]
    agent_id = report.parent.name.split("=", 1)[1]
    rows = list(csv.DictReader(io.StringIO(report.read_text(encoding="utf-8"))))
    total = rows[-1]

    raw_policies = {}
    for part in sorted((root / "raw").glob(f"raw/dt={REPORT_DATE}/source=policies/part-*.csv")):
        for row in csv.DictReader(io.StringIO(part.read_text(encoding="utf-8"))):
            if row["agent_id"] == agent_id:
                raw_policies[row["policy_id"]] = row
    assert raw_policies

    expected_premium = Decimal("0")
    expected_commission = Decimal("0")
    expected_insured = Decimal("0")
    for row in raw_policies.values():
        expected_premium += Decimal(row["premium"])
        expected_commission += (Decimal(row["premium"]) * Decimal(row["commission_rate"])).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        expected_insured += Decimal(row["sum_insured"])

    assert Decimal(total["premium"]) == expected_premium
    assert Decimal(total["commission"]) == expected_commission
    assert Decimal(total["loss_ratio"]) == (
        Decimal(total["claim_amount"]) / expected_premium
    ).quantize(Decimal("0.0001"))

    # Per-policy DETAIL rows must carry their own sum insured, not a copy of the premium.
    detail = {row["policy_id"]: row for row in rows[:-1]}
    assert set(detail) == set(raw_policies)
    assert sum(Decimal(row["sum_insured"]) for row in detail.values()) == expected_insured
    assert any(
        detail[policy_id]["sum_insured"] != detail[policy_id]["premium"] for policy_id in detail
    )


def test_spark_job_is_re_runnable(spark: object, dataset: tuple[Path, Zones]) -> None:
    """A rerun of the same day overwrites cleanly instead of nesting part files."""
    root, _ = dataset
    first = run_job(
        spark,
        raw_uri=(root / "raw").as_uri(),
        reports_uri=(root / "out").as_uri(),
        report_date=REPORT_DATE,
    )
    second = run_job(
        spark,
        raw_uri=(root / "raw").as_uri(),
        reports_uri=(root / "out").as_uri(),
        report_date=REPORT_DATE,
    )
    assert first["agents_reported"] == second["agents_reported"]
    out_root = root / "out" / "reports" / f"dt={REPORT_DATE}"
    for agent_dir in out_root.iterdir():
        if agent_dir.is_dir():
            assert [path.name for path in agent_dir.iterdir() if path.suffix == ".csv"] == [
                "report.csv"
            ]


# --------------------------------------------------------------------------------- adversarial
# A hand-written day that exercises the four ways the two paths used to disagree. Column order
# matches the Spark schemas exactly, because Spark's CSV reader maps a supplied schema by position.
ADVERSARIAL_AGENT_COLUMNS = (
    "agent_id",
    "agent_name",
    "email",
    "region",
    "branch",
    "manager_id",
    "joined_on",
)
ADVERSARIAL_POLICY_COLUMNS = (
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
ADVERSARIAL_CLAIM_COLUMNS = (
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

ROSTERLESS_AGENT = "AGT-000009"
REPLAYED_POLICY = "POL-0000000001"


def write_part(
    store: Storage,
    source: str,
    index: int,
    columns: tuple[str, ...],
    rows: list[dict[str, str]],
) -> None:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(columns), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in columns})
    store.put_bytes(
        f"raw/dt={REPORT_DATE}/source={source}/part-{index:05d}.csv",
        buffer.getvalue().encode("utf-8"),
    )


@pytest.fixture
def adversarial_dataset(tmp_path: Path) -> tuple[Path, Zones]:
    """A day built to break byte-identity, written straight to the filesystem.

    * ``POL-0000000001``: premium 99999.99 at a **4dp** rate (0.0750). Rounding the rate to paise
      first gives 8000.00 instead of 7500.00.
    * ``POL-0000000004``: a 5dp rate (0.07505), to prove both paths round the rate to 4dp the same way.
    * ``POL-0000000005``: belongs to an agent with **no roster row** - it must not produce a report.
    * ``POL-0000000001`` again in a second partition: an at-least-once replay of a raw file, which
      must not double-count the policy.
    * ``CLM-0000000001``: a claim whose own ``agent_id`` is not the policy's owner - it belongs to
      the policy, not to the agent the row claims.
    """
    root = tmp_path / "zones"
    zones = Zones(
        raw=LocalStorage(root / "raw"),
        processed=LocalStorage(root / "processed"),
        reports=LocalStorage(root / "reports"),
    )
    write_part(
        zones.raw,
        "agents",
        0,
        ADVERSARIAL_AGENT_COLUMNS,
        [
            {
                "agent_id": "AGT-000001",
                "agent_name": "Rohan Bose",
                "email": "agt-000001@example.com",
                "region": "West",
                "branch": "Pune",
                "manager_id": ROSTERLESS_AGENT,
                "joined_on": "2024-01-01",
            },
            {
                "agent_id": "AGT-000002",
                "agent_name": "Isha Nair",
                "email": "agt-000002@example.com",
                "region": "South",
                "branch": "Kochi",
                "manager_id": ROSTERLESS_AGENT,
                "joined_on": "2024-02-01",
            },
        ],
    )
    policy_one = {
        "policy_id": REPLAYED_POLICY,
        "agent_id": "AGT-000001",
        "customer_id": "CUS-000000001",
        "product": "Individual Health",
        "policy_start": "2025-11-01",
        "policy_end": "2026-11-01",
        "sum_insured": "1000000.00",
        "premium": "99999.99",
        "commission_rate": "0.0750",
        "status": "Active",
    }
    write_part(
        zones.raw,
        "policies",
        0,
        ADVERSARIAL_POLICY_COLUMNS,
        [
            policy_one,
            {
                "policy_id": "POL-0000000002",
                "agent_id": "AGT-000001",
                "customer_id": "CUS-000000002",
                "product": "Group Health",
                "policy_start": "2026-01-01",
                "policy_end": "2027-01-01",
                "sum_insured": "2000000.00",
                "premium": "12000.50",
                "commission_rate": "0.0625",
                "status": "Active",
            },
            {
                "policy_id": "POL-0000000003",
                "agent_id": "AGT-000002",
                "customer_id": "CUS-000000003",
                "product": "Personal Accident",
                "policy_start": "2026-02-01",
                "policy_end": "2027-02-01",
                "sum_insured": "500000.00",
                "premium": "8000.25",
                "commission_rate": "0.1575",
                "status": "Renewed",
            },
            {
                "policy_id": "POL-0000000004",
                "agent_id": "AGT-000002",
                "customer_id": "CUS-000000004",
                "product": "Term Life",
                "policy_start": "2026-03-01",
                "policy_end": "2027-03-01",
                "sum_insured": "3000000.00",
                "premium": "33333.33",
                "commission_rate": "0.07505",
                "status": "Active",
            },
            {
                "policy_id": "POL-0000000005",
                "agent_id": ROSTERLESS_AGENT,
                "customer_id": "CUS-000000005",
                "product": "Term Life",
                "policy_start": "2026-03-01",
                "policy_end": "2027-03-01",
                "sum_insured": "1000000.00",
                "premium": "100.00",
                "commission_rate": "0.1075",
                "status": "Active",
            },
        ],
    )
    write_part(zones.raw, "policies", 1, ADVERSARIAL_POLICY_COLUMNS, [dict(policy_one)])
    write_part(
        zones.raw,
        "claims",
        0,
        ADVERSARIAL_CLAIM_COLUMNS,
        [
            {
                "claim_id": "CLM-0000000001",
                "policy_id": "POL-0000000002",
                "agent_id": "AGT-000002",  # foreign: this policy belongs to AGT-000001
                "claim_date": "2026-05-01",
                "claim_type": "Cashless",
                "diagnosis_chapter": "J",
                "claimed_amount": "3000.00",
                "settled_amount": "2500.00",
                "status": "Settled",
                "tat_days": "5",
                "hospital_tier": "Tier-1",
            },
            {
                "claim_id": "CLM-0000000002",
                "policy_id": REPLAYED_POLICY,
                "agent_id": "AGT-000001",
                "claim_date": "2026-06-01",
                "claim_type": "Reimbursement",
                "diagnosis_chapter": "K",
                "claimed_amount": "1500.00",
                "settled_amount": "0.00",
                "status": "Pending",
                "tat_days": "9",
                "hospital_tier": "Tier-2",
            },
        ],
    )
    return root, zones


def _report_rows(store: LocalStorage, agent_id: str) -> list[dict[str, str]]:
    key = f"reports/dt={REPORT_DATE}/agent_id={agent_id}/report.csv"
    return list(csv.DictReader(io.StringIO(store.get_bytes(key).decode("utf-8"))))


def test_spark_and_chunker_agree_on_adversarial_input(
    spark: object, adversarial_dataset: tuple[Path, Zones], settings: object
) -> None:
    """Byte-identity on a day built to break it: sub-paise rates, replay, foreign claim, no roster."""
    root, zones = adversarial_dataset
    spark_out = root / "spark_out"
    chunker_out = root / "chunker_out"
    run_job(
        spark,
        raw_uri=(root / "raw").as_uri(),
        reports_uri=spark_out.as_uri(),
        report_date=REPORT_DATE,
    )
    chunker_zones = Zones(
        raw=zones.raw,
        processed=zones.processed,
        reports=LocalStorage(chunker_out),
    )
    run_chunker(
        settings,  # type: ignore[arg-type]
        report_date=REPORT_DATE,
        zones=chunker_zones,
        emit_metrics=False,
    )

    spark_root = spark_out / "reports" / f"dt={REPORT_DATE}"
    spark_reports = {
        path.parent.name.split("=", 1)[1]: path for path in spark_root.glob("agent_id=*/report.csv")
    }
    chunker_reports = {
        parsed.agent_id: key
        for key in chunker_zones.reports.list_keys(f"reports/dt={REPORT_DATE}/")
        if (parsed := parse_report_key(key)) is not None
    }

    # The policy whose agent has no roster row must not produce a report on either path.
    assert set(spark_reports) == {"AGT-000001", "AGT-000002"}
    assert set(chunker_reports) == set(spark_reports)

    for agent_id, spark_path in spark_reports.items():
        assert spark_path.read_bytes() == chunker_zones.reports.get_bytes(
            chunker_reports[agent_id]
        ), f"mismatch for {agent_id}"

    # The money: 99999.99 x 0.0750 = 7499.99925 -> 7500.00 (NOT 99999.99 x 0.08 = 8000.00).
    spark_rows = _report_rows(LocalStorage(spark_out), "AGT-000001")
    details = {row["policy_id"]: row for row in spark_rows if row["row_type"] == "DETAIL"}
    assert set(details) == {"POL-0000000001", "POL-0000000002"}  # replayed policy counted once
    assert details["POL-0000000001"]["commission"] == "7500.00"
    assert details["POL-0000000002"]["commission"] == "750.03"  # 12000.50 x 0.0625
    # The foreign-agent claim is attributed to its policy, exactly as the chunker does it.
    assert details["POL-0000000002"]["claim_count"] == "1"
    assert details["POL-0000000002"]["claim_amount"] == "3000.00"
    assert spark_rows[-1]["row_type"] == "TOTAL"
    assert spark_rows[-1]["commission"] == "8250.03"
    assert spark_rows[-1]["policy_count"] == "2"

    # ... and the 5dp rate rounds to 4dp the same way on both paths.
    agent_two = {
        row["policy_id"]: row
        for row in _report_rows(LocalStorage(spark_out), "AGT-000002")
        if row["row_type"] == "DETAIL"
    }
    assert agent_two["POL-0000000004"]["commission"] == "2503.33"  # 33333.33 x 0.0751
