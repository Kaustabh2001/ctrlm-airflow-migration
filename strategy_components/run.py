"""CLI runner for the components strategy.

Usage:
    python strategy_components/run.py examples/exports -o output/components
"""
from __future__ import annotations

import argparse
from pathlib import Path

try:                                    # package import (python -m strategy_components.run)
    from . import partitioner
except ImportError:                     # script import (python strategy_components/run.py)
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import partitioner  # type: ignore[no-redef]

from ctrlm_core.model import PartitionConfig
from ctrlm_core.pipeline import run_pipeline


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Control-M -> Airflow migration, components strategy",
    )
    parser.add_argument("inputs", nargs="+", help="DEFTABLE XML files or directories")
    parser.add_argument(
        "-o", "--out", default="output/components", help="output directory",
    )
    args = parser.parse_args(argv)
    run_pipeline("components", partitioner.partition, args.inputs, args.out, PartitionConfig())


if __name__ == "__main__":
    main()
