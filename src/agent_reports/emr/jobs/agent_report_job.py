"""EMR-deployable PySpark job: raw partitions -> per-agent CSV reports.

The same file runs unchanged on EMR Serverless, EMR on EC2, and locally (``local[*]``), which is the
point of keeping it free of anything but Spark + the shared key/report helpers::

    # EMR Serverless
    aws emr-serverless start-job-run --application-id <id> --execution-role-arn <arn> \\
      --job-driver '{"sparkSubmit":{"entryPoint":"s3://<artifacts>/agent_report_job.py",
      "sparkSubmitParameters":"--py-files s3://<artifacts>/agent_reports.zip",
      "arguments":["--raw-uri","s3://<raw>","--reports-uri","s3://<out>","--report-date","2026-09-20"]}}'

    # local, exactly what the tests run
    spark-submit agent_report_job.py --raw-uri file:///... --reports-uri file:///... --report-date 2026-09-20

Design notes:

* the output layout matches the chunker's byte for byte
  (``reports/dt=<date>/agent_id=<id>/report.csv``, DETAIL rows sorted by ``policy_id`` then one
  TOTAL row) so the free-tier and paid paths are interchangeable;
* raw columns are read as strings and cast explicitly - no schema inference pass over the day's
  data, which is what keeps a 5M-row read cheap;
* Spark writes ``part-*`` files; :func:`finalize_report_layout` renames each agent's single part file
  to ``report.csv`` through the Hadoop FileSystem API (an object copy on s3a, a rename on local FS).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from typing import Any, Sequence

__all__ = [
    "REPORT_COLUMNS",
    "aggregate_details",
    "build_report_frame",
    "build_spark_session",
    "finalize_report_layout",
    "main",
    "run_job",
    "write_reports",
]

# The job ships the package with --py-files on EMR; locally the installed package is used.
from agent_reports.common.report import REPORT_COLUMNS  # noqa: E402

DETAIL, TOTAL = "DETAIL", "TOTAL"
DETAIL_RANK, TOTAL_RANK = 0, 1
DEFAULT_SHUFFLE_PARTITIONS = "16"


def build_spark_session(
    app_name: str = "agent-report-aggregation",
    *,
    master: str | None = None,
    extra_config: dict[str, str] | None = None,
) -> Any:
    """Create a SparkSession that behaves on a laptop, on Windows, and on EMR.

    ``SPARK_LOCAL_IP``/``PYSPARK_PYTHON`` are honoured when set (the local test fixture sets them);
    on EMR they are irrelevant because the master is not ``local``. ``fs.file.impl`` is pinned to
    ``RawLocalFileSystem`` so local runs do not litter output directories with Hadoop ``.crc``
    checksum files.
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415 - import cost paid only when the job runs

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    config: dict[str, str] = {
        "spark.ui.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": DEFAULT_SHUFFLE_PARTITIONS,
        "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem",
        "spark.python.worker.timeout": "120",
    }
    if master and master.startswith("local"):
        # Windows/laptop hardening: the driver must advertise an address the Python worker can reach.
        config.setdefault("spark.driver.host", os.environ.get("SPARK_LOCAL_IP", "127.0.0.1"))
        config.setdefault("spark.driver.bindAddress", os.environ.get("SPARK_LOCAL_IP", "127.0.0.1"))
    config.update(extra_config or {})
    for key, value in config.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _raw_path(raw_uri: str, report_date: str, source: str) -> str:
    return f"{raw_uri.rstrip('/')}/raw/dt={report_date}/source={source}/"


def read_policies(spark: Any, raw_uri: str, report_date: str) -> Any:
    """Raw policy partitions, typed (no inference pass)."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.sql.types import DecimalType, StringType, StructField, StructType  # noqa: PLC0415

    schema = StructType(
        [
            StructField("policy_id", StringType()),
            StructField("agent_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product", StringType()),
            StructField("policy_start", StringType()),
            StructField("policy_end", StringType()),
            StructField("sum_insured", DecimalType(18, 2)),
            StructField("premium", DecimalType(18, 2)),
            StructField("commission_rate", DecimalType(9, 4)),
            StructField("status", StringType()),
        ]
    )
    return spark.read.schema(schema).option("header", True).csv(
        _raw_path(raw_uri, report_date, "policies")
    ).select(
        "policy_id",
        "agent_id",
        "product",
        "policy_start",
        "policy_end",
        F.col("sum_insured").cast(DecimalType(18, 2)).alias("sum_insured"),
        F.col("premium").cast(DecimalType(18, 2)).alias("premium"),
        F.col("commission_rate").cast(DecimalType(9, 4)).alias("commission_rate"),
    )


def read_agents(spark: Any, raw_uri: str, report_date: str) -> Any:
    from pyspark.sql.types import StringType, StructField, StructType  # noqa: PLC0415

    schema = StructType(
        [
            StructField("agent_id", StringType()),
            StructField("agent_name", StringType()),
            StructField("email", StringType()),
            StructField("region", StringType()),
            StructField("branch", StringType()),
            StructField("manager_id", StringType()),
            StructField("joined_on", StringType()),
        ]
    )
    return spark.read.schema(schema).option("header", True).csv(
        _raw_path(raw_uri, report_date, "agents")
    ).select("agent_id", "agent_name", "region", "branch")


def read_claims(spark: Any, raw_uri: str, report_date: str) -> Any:
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.sql.types import DecimalType, StringType, StructField, StructType  # noqa: PLC0415

    schema = StructType(
        [
            StructField("claim_id", StringType()),
            StructField("policy_id", StringType()),
            StructField("agent_id", StringType()),
            StructField("claim_date", StringType()),
            StructField("claim_type", StringType()),
            StructField("diagnosis_chapter", StringType()),
            StructField("claimed_amount", DecimalType(18, 2)),
            StructField("settled_amount", DecimalType(18, 2)),
            StructField("status", StringType()),
            StructField("tat_days", StringType()),
            StructField("hospital_tier", StringType()),
        ]
    )
    return spark.read.schema(schema).option("header", True).csv(
        _raw_path(raw_uri, report_date, "claims")
    ).select(
        "policy_id",
        "agent_id",
        F.col("claimed_amount").cast(DecimalType(18, 2)).alias("claimed_amount"),
        F.col("settled_amount").cast(DecimalType(18, 2)).alias("settled_amount"),
    )


def aggregate_details(policies: Any, claims: Any, agents: Any) -> Any:
    """One row per policy with its claim roll-up and commission, typed for formatting."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.sql.types import DecimalType  # noqa: PLC0415

    claim_totals = claims.groupBy("policy_id").agg(
        F.count("*").alias("claim_count"),
        F.coalesce(F.sum("claimed_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("claim_amount"),
        F.coalesce(F.sum("settled_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("settled_amount"),
    )
    joined = (
        policies.join(claim_totals, on="policy_id", how="left")
        .join(agents, on="agent_id", how="left")
        .fillna({"claim_count": 0, "claim_amount": 0, "settled_amount": 0})
        .withColumn("claim_count", F.col("claim_count").cast("int"))
        .withColumn(
            "commission",
            F.round(F.col("premium") * F.col("commission_rate"), 2).cast(DecimalType(18, 2)),
        )
    )
    return joined


def build_report_frame(details: Any) -> Any:
    """DETAIL + TOTAL rows, ordered, all columns rendered as strings (CSV-ready)."""
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.sql.types import DecimalType  # noqa: PLC0415

    premium = F.col("premium").cast(DecimalType(18, 2))
    claim_amount = F.col("claim_amount").cast(DecimalType(18, 2))
    loss_ratio = (
        F.when(premium == 0, F.lit("0.0000"))
        .otherwise(F.round(claim_amount / premium, 4).cast(DecimalType(18, 4)).cast("string"))
    )

    detail_rows = details.select(
        F.lit(DETAIL).alias("row_type"),
        F.lit(DETAIL_RANK).alias("row_type_rank"),
        F.col("agent_id"),
        F.coalesce(F.col("agent_name"), F.lit("")).alias("agent_name"),
        F.coalesce(F.col("region"), F.lit("")).alias("region"),
        F.coalesce(F.col("branch"), F.lit("")).alias("branch"),
        F.col("policy_id"),
        F.coalesce(F.col("product"), F.lit("")).alias("product"),
        F.coalesce(F.col("policy_start"), F.lit("")).alias("policy_start"),
        F.coalesce(F.col("policy_end"), F.lit("")).alias("policy_end"),
        premium.cast("string").alias("sum_insured_str"),
        premium.cast("string").alias("premium_str"),
        F.col("commission").cast("string").alias("commission_str"),
        F.lit("1").alias("policy_count_str"),
        F.col("claim_count").cast("string").alias("claim_count_str"),
        claim_amount.cast("string").alias("claim_amount_str"),
        F.col("settled_amount").cast("string").alias("settled_amount_str"),
        loss_ratio.alias("loss_ratio_str"),
    )

    totals = details.groupBy("agent_id").agg(
        F.count("*").alias("policy_count"),
        F.sum(F.col("premium").cast(DecimalType(18, 2))).cast(DecimalType(18, 2)).alias("premium"),
        F.sum(F.col("commission").cast(DecimalType(18, 2))).cast(DecimalType(18, 2)).alias("commission"),
        F.sum(F.col("claim_count")).cast("int").alias("claim_count"),
        F.sum(F.col("claim_amount").cast(DecimalType(18, 2))).cast(DecimalType(18, 2)).alias("claim_amount"),
        F.sum(F.col("settled_amount").cast(DecimalType(18, 2))).cast(DecimalType(18, 2)).alias("settled_amount"),
    ).withColumn(
        "loss_ratio",
        F.when(F.col("premium") == 0, F.lit("0.0000"))
        .otherwise(F.round(F.col("claim_amount") / F.col("premium"), 4).cast(DecimalType(18, 4)).cast("string")),
    )

    total_rows = totals.select(
        F.lit(TOTAL).alias("row_type"),
        F.lit(TOTAL_RANK).alias("row_type_rank"),
        F.col("agent_id"),
        F.lit("").alias("agent_name"),
        F.lit("").alias("region"),
        F.lit("").alias("branch"),
        F.lit("").alias("policy_id"),
        F.lit("").alias("product"),
        F.lit("").alias("policy_start"),
        F.lit("").alias("policy_end"),
        F.lit("").alias("sum_insured_str"),
        F.col("premium").cast("string").alias("premium_str"),
        F.col("commission").cast("string").alias("commission_str"),
        F.col("policy_count").cast("string").alias("policy_count_str"),
        F.col("claim_count").cast("string").alias("claim_count_str"),
        F.col("claim_amount").cast("string").alias("claim_amount_str"),
        F.col("settled_amount").cast("string").alias("settled_amount_str"),
        F.col("loss_ratio").alias("loss_ratio_str"),
    )

    return detail_rows.unionByName(total_rows).select(
        "row_type",
        "row_type_rank",
        F.col("agent_id"),
        "agent_name",
        "region",
        "branch",
        "policy_id",
        "product",
        "policy_start",
        "policy_end",
        F.col("sum_insured_str").alias("sum_insured"),
        F.col("premium_str").alias("premium"),
        F.col("commission_str").alias("commission"),
        F.col("policy_count_str").alias("policy_count"),
        F.col("claim_count_str").alias("claim_count"),
        F.col("claim_amount_str").alias("claim_amount"),
        F.col("settled_amount_str").alias("settled_amount"),
        F.col("loss_ratio_str").alias("loss_ratio"),
    )


def write_reports(frame: Any, reports_uri: str, report_date: str) -> str:
    """Write one CSV part per agent under ``reports/dt=<date>/agent_id=<id>/``."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    destination = f"{reports_uri.rstrip('/')}/reports/dt={report_date}"
    ordered = frame.repartition(F.col("agent_id")).sortWithinPartitions(
        "row_type_rank", "policy_id"
    )
    ordered.drop("row_type_rank").write.mode("overwrite").option("header", True).partitionBy(
        "agent_id"
    ).csv(destination)
    return destination


def finalize_report_layout(spark: Any, reports_uri: str, report_date: str) -> list[str]:
    """Rename each agent's ``part-*`` file to ``report.csv`` (S3 object copy / local rename)."""
    destination = f"{reports_uri.rstrip('/')}/reports/dt={report_date}"
    jvm = spark.sparkContext._jvm  # noqa: SLF001 - the supported way to reach the Hadoop FS API
    path_cls = jvm.org.apache.hadoop.fs.Path
    root = path_cls(destination)
    fs = root.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    renamed: list[str] = []
    for agent_status in fs.listStatus(root):
        if not agent_status.isDirectory():
            continue
        agent_dir = agent_status.getPath()
        for entry in fs.listStatus(agent_dir):
            name = entry.getPath().getName()
            if name.startswith("part-") and name.endswith(".csv"):
                target = path_cls(agent_dir, "report.csv")
                if fs.rename(entry.getPath(), target):
                    renamed.append(f"{agent_dir.getName()}/report.csv")
                else:  # pragma: no cover - rename failures surface as missing reports downstream
                    raise RuntimeError(f"could not rename {entry.getPath()} to {target}")
    return sorted(renamed)


def run_job(
    spark: Any,
    *,
    raw_uri: str,
    reports_uri: str,
    report_date: str,
    write_summary: bool = True,
) -> dict[str, Any]:
    """Full aggregation: read -> aggregate -> write -> rename -> summary."""
    started = time.perf_counter()
    policies = read_policies(spark, raw_uri, report_date)
    agents = read_agents(spark, raw_uri, report_date)
    claims = read_claims(spark, raw_uri, report_date)

    rows_in = policies.count() + agents.count() + claims.count()
    details = aggregate_details(policies, claims, agents)
    frame = build_report_frame(details)
    destination = write_reports(frame, reports_uri, report_date)
    report_keys = finalize_report_layout(spark, reports_uri, report_date)

    summary = {
        "report_date": report_date,
        "raw_uri": raw_uri,
        "reports_uri": reports_uri,
        "destination": destination,
        "rows_in": rows_in,
        "policies": int(policies.count()),
        "agents_reported": len(report_keys),
        "report_keys": report_keys[:10],
        "spark_app_id": spark.sparkContext.applicationId,
        "spark_master": spark.sparkContext.master,
        "duration_seconds": round(time.perf_counter() - started, 3),
        "completed_at": datetime.now(tz=UTC).isoformat(),
    }
    if write_summary:
        summary_path = f"{reports_uri.rstrip('/')}/reports/dt={report_date}/_job_summary.json"
        _write_json(spark, summary_path, summary)
    return summary


def _write_json(spark: Any, path: str, payload: dict[str, Any]) -> None:
    """Write a small JSON document through the Hadoop FS API (works on s3a and local FS)."""
    jvm = spark.sparkContext._jvm  # noqa: SLF001
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(target, True)
    try:
        stream.write(bytearray(json.dumps(payload, default=str, indent=2).encode("utf-8")))
    finally:
        stream.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agent_report_job", description=__doc__)
    parser.add_argument("--raw-uri", required=True, help="raw zone URI (s3://bucket or file:///dir)")
    parser.add_argument("--reports-uri", required=True, help="reports zone URI")
    parser.add_argument("--report-date", required=True, help="dt partition, YYYY-MM-DD")
    parser.add_argument("--master", default=None, help="spark master, e.g. local[2] (default: env)")
    parser.add_argument("--no-summary", action="store_true", help="skip the _job_summary.json object")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spark = build_spark_session(master=args.master)
    try:
        summary = run_job(
            spark,
            raw_uri=args.raw_uri,
            reports_uri=args.reports_uri,
            report_date=args.report_date,
            write_summary=not args.no_summary,
        )
    finally:
        spark.stop()
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
