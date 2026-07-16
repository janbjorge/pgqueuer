"""Append fresh benchmark results to the monthly NDJSON history files, idempotently."""

from __future__ import annotations

import argparse
from pathlib import Path

from tools.benchmark_data import load_json_dir, load_ndjson_dir, merge_rows, write_monthly_ndjson


def append(data_dir: Path, new_dir: Path) -> list[Path]:
    existing = load_ndjson_dir(data_dir)
    merged = merge_rows(existing, load_json_dir(new_dir))
    written = write_monthly_ndjson(merged, data_dir)
    print(f"Appended {len(merged) - len(existing)} rows, total {len(merged)}.")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--new-dir", type=Path, required=True)
    args = parser.parse_args()
    for file in append(args.data_dir, args.new_dir):
        print(file)


if __name__ == "__main__":
    main()
