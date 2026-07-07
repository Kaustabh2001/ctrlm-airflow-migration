"""Single-entry strategy runner.

Usage:
    python strategy_single_entry/run.py examples/exports -o output/single_entry
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ctrlm_core.model import PartitionConfig
from ctrlm_core.pipeline import run_pipeline

if __package__ in (None, ""):        # invoked as a script
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from partitioner import partition
else:                                # imported as strategy_single_entry.run
    from .partitioner import partition


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Control-M -> Airflow migration, single-entry (ownership) strategy"
    )
    parser.add_argument("inputs", nargs="+", help="DEFTABLE XML export files or directories")
    parser.add_argument("-o", "--out", default="output/single_entry", help="output directory")
    args = parser.parse_args(argv)
    run_pipeline("single_entry", partition, args.inputs, args.out, PartitionConfig())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
