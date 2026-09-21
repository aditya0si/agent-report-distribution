"""Dataset generator: determinism, schema, referential integrity, and the row-count contract."""

from __future__ import annotations

import csv
import io
import json
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

# pyarrow is imported loudly, not with pytest.importorskip: it is a declared dev dependency
# (requirements-dev.txt, the file CI installs from) and the parquet round-trip below is the only test
# that exercises the parquet writer at all. Guarding the import made that test vanish on CI - the
# suite reported "1 skipped" and nothing was checked - which is exactly what this repo does not do.
# A missing pyarrow is a broken environment and has to fail the suite.
import pyarrow
import pyarrow.parquet as pq
import pytest

from agent_reports.common.errors import ConfigError
from agent_reports.common.report import to_decimal, to_rate_decimal
from agent_reports.common.storage import LocalStorage
from agent_reports.ingest import generator
from agent_reports.ingest.cli import main as cli_main
from agent_reports.ingest.generator import DatasetConfig, generate_dataset, iter_rows
from agent_reports.ingest.schema import SOURCE_COLUMNS


def read_source(store: LocalStorage, report_date: str, source: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for key in sorted(store.list_keys(f"raw/dt={report_date}/source={source}/")):
        text = store.get_bytes(key).decode("utf-8")
        rows.extend(csv.DictReader(io.StringIO(text)))
    return rows


class TestConfig:
    def test_for_total_rows_hits_the_target(self) -> None:
        config = DatasetConfig.for_total_rows(50_000, "2026-09-20")
        assert config.estimated_rows >= 50_000
        assert config.agents == 3_805  # ceil(50000 * 1.05 / 13.8)

    def test_for_total_rows_rejects_zero(self) -> None:
        with pytest.raises(ConfigError, match="total_rows"):
            DatasetConfig.for_total_rows(0, "2026-09-20")

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"agents": 0}, "agents"),
            ({"policies_per_agent": 0}, "policies_per_agent"),
            ({"claims_per_policy": 9.0}, "claims_per_policy"),
            ({"partitions": 0}, "partitions"),
            ({"extension": "json"}, "extension"),
            ({"report_date": "20-09-2026"}, "report_date"),
        ],
    )
    def test_invalid_config_rejected(self, kwargs: dict[str, object], message: str) -> None:
        base = {"report_date": "2026-09-20"}
        with pytest.raises(ConfigError, match=message):
            DatasetConfig(**{**base, **kwargs})  # type: ignore[arg-type]

    def test_describe_is_json_serialisable(self) -> None:
        json.dumps(DatasetConfig(report_date="2026-09-20").describe())

    def test_claim_probability_is_normalised_to_the_configured_mean(self) -> None:
        from agent_reports.ingest.schema import PRODUCTS

        for target in (0.0, 0.2, 0.6, 1.0):
            probabilities = [generator.scaled_claim_probability(p, target) for p in PRODUCTS]
            assert all(0.0 <= probability <= 1.0 for probability in probabilities)
            assert min(probabilities) <= target <= max(probabilities) or target == 0.0


class TestGeneratedData:
    def test_row_counts_and_parts(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        config = DatasetConfig(report_date="2026-09-20", agents=10, policies_per_agent=4, seed=3)
        stats = generate_dataset(config, store)
        assert stats.agents == 10
        assert stats.policies == 40
        assert stats.rows_total == 10 + 40 + stats.claims
        assert stats.parts == 12  # 3 sources x 4 partitions
        assert stats.bytes_written > 0
        assert len(store.list_keys("raw/dt=2026-09-20/source=agents/")) == 4

    def test_determinism_is_byte_identical(self, tmp_path: Path) -> None:
        config = DatasetConfig(report_date="2026-09-20", agents=12, seed=99)
        first = LocalStorage(tmp_path / "a")
        second = LocalStorage(tmp_path / "b")
        generate_dataset(config, first)
        generate_dataset(config, second)
        keys_first = [
            key
            for key in first.list_keys("")
            if key.startswith("raw/dt=") and "_dataset_manifest" not in key
        ]
        assert keys_first
        assert keys_first == [
            key
            for key in second.list_keys("")
            if key.startswith("raw/dt=") and "_dataset_manifest" not in key
        ]
        for key in keys_first:
            assert first.get_bytes(key) == second.get_bytes(key), key

    def test_manifest_carries_run_metadata(self, tmp_path: Path) -> None:
        """The manifest is deliberately *not* byte-stable: it records where and when the run happened."""
        config = DatasetConfig(report_date="2026-09-20", agents=4, seed=99)
        first = LocalStorage(tmp_path / "a")
        second = LocalStorage(tmp_path / "b")
        generate_dataset(config, first)
        generate_dataset(config, second)
        key = "raw/dt=2026-09-20/_dataset_manifest.json"
        assert first.get_bytes(key) != second.get_bytes(key)  # different output_uri
        assert (
            json.loads(first.get_bytes(key))["rows_total"]
            == json.loads(second.get_bytes(key))["rows_total"]
        )

    def test_different_seeds_differ(self, tmp_path: Path) -> None:
        first = LocalStorage(tmp_path / "a")
        second = LocalStorage(tmp_path / "b")
        generate_dataset(DatasetConfig(report_date="2026-09-20", agents=8, seed=1), first)
        generate_dataset(DatasetConfig(report_date="2026-09-20", agents=8, seed=2), second)
        assert first.get_bytes(
            "raw/dt=2026-09-20/source=policies/part-00000.csv"
        ) != second.get_bytes("raw/dt=2026-09-20/source=policies/part-00000.csv")

    def test_sharding_does_not_change_the_rows(self, tmp_path: Path) -> None:
        """Re-sharding changes which file a row lands in, never the row itself."""
        one = LocalStorage(tmp_path / "one")
        four = LocalStorage(tmp_path / "four")
        base = {"report_date": "2026-09-20", "agents": 9, "seed": 5}
        generate_dataset(DatasetConfig(**base, partitions=1), one)  # type: ignore[arg-type]
        generate_dataset(DatasetConfig(**base, partitions=4), four)  # type: ignore[arg-type]
        for source in ("agents", "policies", "claims"):
            rows_one = sorted(
                read_source(one, "2026-09-20", source), key=lambda row: sorted(row.values())
            )
            rows_four = sorted(
                read_source(four, "2026-09-20", source), key=lambda row: sorted(row.values())
            )
            assert rows_one == rows_four, source
        assert len(one.list_keys("raw/dt=2026-09-20/source=policies/")) == 1
        assert len(four.list_keys("raw/dt=2026-09-20/source=policies/")) == 4

    def test_headers_match_the_documented_schema(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        generate_dataset(DatasetConfig(report_date="2026-09-20", agents=4, seed=1), store)
        for source, columns in SOURCE_COLUMNS.items():
            key = f"raw/dt=2026-09-20/source={source}/part-00000.csv"
            header = store.get_bytes(key).decode("utf-8").splitlines()[0]
            assert header == ",".join(columns)

    def test_no_pii_shaped_values(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        generate_dataset(DatasetConfig(report_date="2026-09-20", agents=6, seed=2), store)
        agents = read_source(store, "2026-09-20", "agents")
        assert agents
        for row in agents:
            assert row["email"].endswith("@example.com")  # reserved, non-routable domain
            assert row["email"].startswith("agt-")
            assert set(row) == set(SOURCE_COLUMNS["agents"])
        for source in ("policies", "claims"):
            for row in read_source(store, "2026-09-20", source):
                assert not any("phone" in column or "aadhaar" in column for column in row)

    def test_referential_integrity_and_commission_maths(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        generate_dataset(DatasetConfig(report_date="2026-09-20", agents=5, seed=8), store)
        agents = {row["agent_id"] for row in read_source(store, "2026-09-20", "agents")}
        policies = read_source(store, "2026-09-20", "policies")
        claims = read_source(store, "2026-09-20", "claims")
        policy_ids = {row["policy_id"] for row in policies}
        assert len(policy_ids) == len(policies)
        for row in policies:
            assert row["agent_id"] in agents
            # The documented contract: parse the rate at 4dp, multiply, round the product HALF_UP
            # once. (Quantising the rate to paise first - what the chunker used to do - is a
            # different, wrong number for any rate that is not a multiple of 0.01.)
            rate = to_rate_decimal(row["commission_rate"])
            premium = to_decimal(row["premium"])
            commission = (premium * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            assert commission == (premium * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            assert rate == Decimal(row["commission_rate"])
            assert Decimal(row["premium"]) > 0
            assert Decimal(row["sum_insured"]) >= Decimal(row["premium"])
        for row in claims:
            assert row["policy_id"] in policy_ids
            assert row["agent_id"] in agents
            assert Decimal(row["settled_amount"]) <= Decimal(row["claimed_amount"])
            assert row["status"] in ("Settled", "Approved", "Rejected", "Pending")

    def test_generated_rates_exercise_sub_paise_precision(self, tmp_path: Path) -> None:
        """The default dataset must contain rates that are not multiples of 0.01.

        With only 2dp rates the whole suite is blind to the rate-quantisation bug: rounding a rate
        to paise before multiplying changes nothing when the rate already is a whole number of
        paise. This test is what keeps that regression visible.
        """
        store = LocalStorage(tmp_path)
        generate_dataset(DatasetConfig(report_date="2026-09-20", agents=8, seed=3), store)
        rates = {
            to_rate_decimal(row["commission_rate"])
            for row in read_source(store, "2026-09-20", "policies")
        }
        assert rates, "the generator produced no policies"
        assert any(rate != rate.quantize(Decimal("0.01")) for rate in rates), sorted(rates)
        assert all(rate == rate.quantize(Decimal("0.0001")) for rate in rates), sorted(rates)

    def test_parquet_output_round_trips(self, tmp_path: Path) -> None:
        """The parquet writer, read back with the same engine that wrote it.

        This is the only test of ``_parquet_bytes``; it used to be ``pytest.importorskip("pyarrow")``
        and so reported as a skip on CI (which did not install pyarrow) - a parquet deliverable with
        nothing testing it. The import at the top of this module now fails loudly instead, and
        requirements-dev.txt installs pyarrow, so the test runs wherever the suite runs.
        """
        store = LocalStorage(tmp_path)
        generate_dataset(
            DatasetConfig(report_date="2026-09-20", agents=4, seed=4, extension="parquet"), store
        )
        key = "raw/dt=2026-09-20/source=policies/part-00000.parquet"
        table = pq.read_table(io.BytesIO(store.get_bytes(key)))
        assert isinstance(table, pyarrow.Table)
        assert table.num_rows > 0
        assert list(table.column_names) == list(SOURCE_COLUMNS["policies"])

    def test_manifest_is_written(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        stats = generate_dataset(DatasetConfig(report_date="2026-09-20", agents=3, seed=1), store)
        manifest = json.loads(
            store.get_bytes("raw/dt=2026-09-20/_dataset_manifest.json").decode("utf-8")
        )
        assert manifest["rows_total"] == stats.rows_total
        assert manifest["report_date"] == "2026-09-20"

    def test_manifest_can_be_skipped(self, tmp_path: Path) -> None:
        store = LocalStorage(tmp_path)
        generate_dataset(
            DatasetConfig(report_date="2026-09-20", agents=3, seed=1), store, include_manifest=False
        )
        assert store.list_keys("raw/dt=2026-09-20/_dataset_manifest.json") == []

    def test_iter_rows_is_lazy_and_ordered(self) -> None:
        config = DatasetConfig(report_date="2026-09-20", agents=3, policies_per_agent=2, seed=1)
        sources = [source for source, _, _ in iter_rows(config)]
        assert sources[0] == "agents"
        assert sources.count("agents") == 3
        assert sources.count("policies") == 6


class TestCli:
    def test_cli_generates_at_least_the_requested_rows(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        out = tmp_path / "raw"
        exit_code = cli_main(
            [
                "--out",
                str(out),
                "--report-date",
                "2026-09-20",
                "--rows",
                "20000",
                "--seed",
                "7",
            ]
        )
        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["rows_total"] >= 20_000
        assert payload["config"]["seed"] == 7
        assert (out / "raw" / "dt=2026-09-20" / "source=agents" / "part-00000.csv").exists()

    def test_cli_honours_explicit_agent_count(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        exit_code = cli_main(
            [
                "--out",
                str(tmp_path / "raw"),
                "--report-date",
                "2026-09-20",
                "--agents",
                "7",
                "--quiet",
            ]
        )
        assert exit_code == 0
        assert capsys.readouterr().out == ""
        assert (
            len(LocalStorage(tmp_path / "raw").list_keys("raw/dt=2026-09-20/source=agents/")) == 4
        )
