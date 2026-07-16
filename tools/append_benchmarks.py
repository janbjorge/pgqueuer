"""
Append fresh benchmark results to the yearly NDJSON history files.
Idempotent: rows are deduped on (created_at, driver, strategy) and
non-main refs are dropped.

Usage (from the repository root)::

    python3 -m tools.append_benchmarks --data-dir benchmark-data --new-dir current
"""

from __future__ import annotations

import argparse
from pathlib import Path

from tools.benchmark_data import load_json_dir, load_ndjson_dir, merge_rows, write_yearly_ndjson


def append(data_dir: Path, new_dir: Path) -> None:
    existing = load_ndjson_dir(data_dir)
    merged = merge_rows(existing, load_json_dir(new_dir))
    written = write_yearly_ndjson(merged, data_dir)
    print(f"Appended {len(merged) - len(existing)} rows, total {len(merged)}.")
    for file in written:
        print(file)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--new-dir", type=Path, required=True)
    args = parser.parse_args()
    append(args.data_dir, args.new_dir)


if __name__ == "__main__":
    main()
