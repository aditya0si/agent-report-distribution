"""Shared fixtures.

Three kinds of test live here:

* **pure unit** - no AWS at all (key conventions, retry maths, CSV shaping, template rendering);
* **moto unit/integration** - ``mock_aws`` with the same buckets/queues/SES identities Terraform
  creates, so queue attributes (visibility timeout, redrive policy) are exercised for real;
* **spark** - a real local Spark session against the filesystem, marked ``requires_jvm``.

Nothing here reaches the network or needs AWS credentials.
"""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from agent_reports.common.aws import reset_client_cache
from agent_reports.common.settings import Settings
from agent_reports.common.storage import LocalStorage, Zones, open_zones

REGION = "us-east-1"
REPORT_DATE = "2026-09-20"
VERIFIED_RECIPIENTS = 120

#: Queue names must match ``infra/terraform/sqs.tf`` so the fixture proves the same attributes.
MAIN_QUEUE_NAME = "agent-reports-fanout"
DLQ_NAME = "agent-reports-fanout-dlq"


def base_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "region": REGION,
        "raw_bucket": "agent-reports-raw",
        "processed_bucket": "agent-reports-processed",
        "reports_bucket": "agent-reports-out",
        "agent_queue_url": f"https://sqs.{REGION}.amazonaws.com/000000000000/{MAIN_QUEUE_NAME}",
        "dlq_url": f"https://sqs.{REGION}.amazonaws.com/000000000000/{DLQ_NAME}",
        "ses_sender": "reports@example.com",
        "ses_configuration_set": "agent-reports",
        "presign_ttl_seconds": 900,
        "sqs_batch_size": 10,
        "report_date": REPORT_DATE,
    }
    values.update(overrides)
    return Settings(**values)


@pytest.fixture
def settings() -> Settings:
    return base_settings().validate()


@pytest.fixture
def aws(settings: Settings) -> Iterator[Settings]:
    """moto AWS with the pipeline's buckets, queues (redrive) and SES identities provisioned."""
    from moto import mock_aws

    from agent_reports.testing import provision_local_resources

    with mock_aws():
        created = provision_local_resources(settings, verify_recipients=VERIFIED_RECIPIENTS)
        resolved = replace(
            settings,
            agent_queue_url=created["main_queue_url"],
            dlq_url=created["dlq_url"],
        )
        reset_client_cache()
        yield resolved


@pytest.fixture
def zones(aws: Settings) -> Zones:
    return open_zones(aws)


@pytest.fixture
def handler_env(aws: Settings, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Export the fixture settings as ``AGENT_REPORTS_*`` env vars.

    The Lambda ``handler`` entry points call ``load_settings()`` themselves (that is the real
    production path), so tests of those handlers must drive them through the environment rather than
    by injecting a Settings object.
    """
    from agent_reports.common.settings import ENV_PREFIX

    for key, value in aws.redacted().items():
        if value is None:
            continue
        monkeypatch.setenv(f"{ENV_PREFIX}{key.upper()}", str(value))
    return aws


@pytest.fixture
def local_zones(tmp_path: Path) -> Zones:
    """Filesystem-backed zones (used by the Spark test, which cannot use moto S3)."""
    return Zones(
        raw=LocalStorage(tmp_path / "raw"),
        processed=LocalStorage(tmp_path / "processed"),
        reports=LocalStorage(tmp_path / "reports"),
    )


@pytest.fixture
def small_dataset(aws: Settings, zones: Zones) -> dict[str, int]:
    """A tiny day of raw data written into the (mocked) raw bucket."""
    from agent_reports.ingest.generator import DatasetConfig, generate_dataset

    stats = generate_dataset(
        DatasetConfig(report_date=REPORT_DATE, agents=6, policies_per_agent=3, seed=11),
        zones.raw,
    )
    return {
        "agents": stats.agents,
        "policies": stats.policies,
        "claims": stats.claims,
        "rows_total": stats.rows_total,
    }


# --------------------------------------------------------------------------- spark
def _java_available() -> bool:
    if shutil.which("java"):
        return True
    java_home = os.environ.get("JAVA_HOME")
    return bool(java_home and (Path(java_home) / "bin" / "java.exe").exists())


@pytest.fixture(scope="session")
def spark() -> Iterator[Any]:
    """A real local SparkSession, or a loud skip when no JVM exists on this machine."""
    if not _java_available():
        banner = (
            "\n" + "=" * 78 + "\n"
            "SKIPPING SPARK TESTS: no JVM found (java not on PATH and JAVA_HOME unset).\n"
            "Install one (per-user, no admin):\n"
            "  winget install -e --id EclipseAdoptium.Temurin.21.JRE\n"
            "or unpack the Adoptium JRE zip and export JAVA_HOME.\n"
            "CI runs these tests with actions/setup-java, so they are covered there.\n"
            + "=" * 78
            + "\n"
        )
        print(banner, file=sys.stderr)
        pytest.skip("no JVM available (requires_jvm)", allow_module_level=True)

    pytest.importorskip("pyspark", reason="pyspark is not installed")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from agent_reports.emr.jobs.agent_report_job import build_spark_session

    session = build_spark_session(app_name="agent-report-tests", master="local[2]")
    try:
        yield session
    finally:
        session.stop()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark every test in the spark module so ``-m 'not requires_jvm'`` can exclude it."""
    for item in items:
        if "test_spark_job" in str(item.fspath):
            item.add_marker(pytest.mark.requires_jvm)
            item.add_marker(pytest.mark.spark)
