"""``agent-reports generate`` - CLI entry point for the synthetic dataset generator."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from ..common.logging_utils import configure_logging, get_logger
from ..common.settings import Settings
from ..common.storage import open_store
from .generator import DatasetConfig, DatasetStats, generate_dataset

__all__ = ["build_parser", "generate", "main"]

_LOG = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-reports generate",
        description="Generate the synthetic agent/policy/claim dataset used by the pipeline.",
    )
    parser.add_argument("--out", required=True, help="output URI: s3://bucket/prefix or a local dir")
    parser.add_argument("--report-date", required=True, help="dt partition, YYYY-MM-DD")
    parser.add_argument("--rows", type=int, default=50_000, help="target total rows (>= 50000 for the demo)")
    parser.add_argument("--agents", type=int, default=None, help="explicit agent count (overrides --rows)")
    parser.add_argument("--policies-per-agent", type=int, default=8)
    parser.add_argument("--claims-per-policy", type=float, default=0.6)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--format", dest="extension", choices=("csv", "parquet"), default="csv")
    parser.add_argument("--no-manifest", action="store_true", help="skip the dataset manifest object")
    parser.add_argument("--quiet", action="store_true")
    return parser


def generate(args: argparse.Namespace, settings: Settings | None = None) -> DatasetStats:
    """Run the generator against ``args.out`` (raises on any failure)."""
    resolved = settings or Settings.from_env()
    if args.agents is not None:
        config = DatasetConfig(
            report_date=args.report_date,
            agents=args.agents,
            policies_per_agent=args.policies_per_agent,
            claims_per_policy=args.claims_per_policy,
            seed=args.seed,
            partitions=args.partitions,
            extension=args.extension,
        )
    else:
        config = DatasetConfig.for_total_rows(
            args.rows,
            args.report_date,
            seed=args.seed,
            partitions=args.partitions,
            extension=args.extension,
            policies_per_agent=args.policies_per_agent,
            claims_per_policy=args.claims_per_policy,
        )
    store = open_store(args.out, resolved)
    return generate_dataset(config, store, include_manifest=not args.no_manifest)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging("WARNING" if args.quiet else "INFO")
    stats = generate(args)
    if not args.quiet:
        print(json.dumps(stats.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
