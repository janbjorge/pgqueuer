"""
Gate fresh benchmark results against the main-branch history window.

Usage (from the repository root, with the yearly NDJSON history fetched
into `<data-dir>/benchmark/`)::

    python3 -m tools.compare_rps --data-dir benchmark-data --current-dir current
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from typing_extensions import assert_never

from tools.benchmark_data import (
    GateResult,
    baseline_window,
    gate,
    load_json_dir,
    load_ndjson_dir,
)

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"


def status_indicator(result: GateResult) -> str:
    if result.verdict == "pass":
        return f"{GREEN}✓ PASS{RESET}"
    if result.verdict == "warn":
        return f"{YELLOW}⚠ WARN{RESET}"
    if result.verdict == "fail":
        return f"{RED}✗ FAIL{RESET}"
    if result.verdict == "skip":
        return f"{YELLOW}- SKIP (baseline has {result.sample_count} samples){RESET}"
    assert_never(result.verdict)


def print_result(driver: str, strategy: str, result: GateResult) -> None:
    print(f"{BOLD}Driver: {driver} ({strategy}){RESET} {status_indicator(result)}")
    if result.median is not None and result.mad is not None:
        print(
            f"Baseline (n={result.sample_count}): median {result.median:.1f} | "
            f"MAD {result.mad:.1f} | "
            f"warn < {result.warn_threshold:.1f} | fail < {result.fail_threshold:.1f}"
        )
    print(f"Current rate: {result.current_rate:.1f}")
    print("-" * 40)


def compare(data_dir: Path, current_dir: Path, window_size: int) -> int:
    history = load_ndjson_dir(data_dir)
    current = load_json_dir(current_dir)
    if not current:
        print(f"{RED}No benchmark results found in {current_dir}{RESET}")
        return 1

    exit_status = 0
    for row in sorted(current, key=lambda r: r.combo()):
        result = gate(
            baseline_window(history, row.combo(), window_size),
            row.rate,
        )
        print_result(row.driver, row.strategy, result)
        if result.verdict == "fail":
            exit_status = 1
    return exit_status


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--current-dir", type=Path, required=True)
    parser.add_argument("--window-size", type=int, default=30)
    args = parser.parse_args()
    sys.exit(compare(args.data_dir, args.current_dir, args.window_size))


if __name__ == "__main__":
    main()
