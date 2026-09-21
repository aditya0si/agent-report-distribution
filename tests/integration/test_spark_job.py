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
from pathlib import Path

import pytest

from agent_reports.common.keys import parse_report_key
from agent_reports.common.report import REPORT_COLUMNS
from agent_reports.common.roster import read_roster
from agent_reports.common.storage import LocalStorage, Zones
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
    assert json.loads(summary_path.read_text(encoding="utf-8"))["agents_reported"] == summary[
        "agents_reported"
    ]


def test_spark_output_rows_are_ordered_and_shaped(spark: object, dataset: tuple[Path, Zones]) -> None:
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
    assert [row["policy_id"] for row in rows[:-1]] == sorted(
        row["policy_id"] for row in rows[:-1]
    )
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
    from decimal import Decimal

    root, zones = dataset
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
    for policy_id, row in raw_policies.items():
        expected_premium += Decimal(row["premium"])
        expected_commission += (
            Decimal(row["premium"]) * Decimal(row["commission_rate"])
        ).quantize(Decimal("0.01"))
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
