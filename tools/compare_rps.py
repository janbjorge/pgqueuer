"""Gate fresh benchmark results against the main-branch history window."""

from __future__ import annotations

import argparse
import sys
from itertools import groupby
from pathlib import Path

from typing_extensions import assert_never

from tools.benchmark_data import (
    RUNS_PER_JOB,
    DriftResult,
    GateResult,
    baseline_window,
    drift,
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


def drift_indicator(result: DriftResult) -> str:
    if result.verdict == "ok":
        return f"{GREEN}steady{RESET}"
    if result.verdict == "drift":
        return f"{RED}DRIFTING{RESET}"
    if result.verdict == "skip":
        return f"{YELLOW}skip (history too thin){RESET}"
    assert_never(result.verdict)


def print_result(driver: str, strategy: str, result: GateResult, trend: DriftResult) -> None:
    print(f"{BOLD}Driver: {driver} ({strategy}){RESET} {status_indicator(result)}")
    if result.median is not None:
        print(
            f"Baseline (n={result.sample_count}): median {result.median:.1f} | "
            f"warn < {result.warn_threshold:.1f} | fail < {result.fail_threshold:.1f}"
        )
    print(f"Current rate (median of {result.current_count} runs): {result.current_rate:.1f}")
    if result.current_count != RUNS_PER_JOB:
        print(
            f"{YELLOW}Expected {RUNS_PER_JOB} fresh runs, got {result.current_count} — "
            f"the gate ratios are calibrated for {RUNS_PER_JOB}{RESET}"
        )
    line = f"Main drift: {drift_indicator(trend)}"
    if trend.recent_median is not None:
        line += (
            f" | recent median {trend.recent_median:.1f} vs "
            f"prior {trend.prior_median:.1f} (alert < {trend.threshold:.1f})"
        )
    print(line)
    print("-" * 40)


def compare(
    data_dir: Path,
    current_dir: Path,
    window_size: int,
    fail_on_drift: bool = False,
) -> int:
    history = load_ndjson_dir(data_dir)
    current = load_json_dir(current_dir)
    if not current:
        print(f"{RED}No benchmark results found in {current_dir}{RESET}")
        return 1

    exit_status = 0
    for (driver, strategy), rows in groupby(
        sorted(current, key=lambda r: r.combo()),
        key=lambda r: r.combo(),
    ):
        window = baseline_window(history, (driver, strategy), window_size)
        result = gate(window, [row.rate for row in rows])
        trend = drift(window)
        print_result(driver, strategy, result, trend)
        if result.verdict == "fail" or (fail_on_drift and trend.verdict == "drift"):
            exit_status = 1
    return exit_status


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--current-dir", type=Path, required=True)
    parser.add_argument("--window-size", type=int, default=30)
    parser.add_argument(
        "--fail-on-drift",
        action="store_true",
        help="Exit non-zero when main history drifts; for main/scheduled runs, "
        "where drift is actionable, not pull requests.",
    )
    args = parser.parse_args()
    sys.exit(compare(args.data_dir, args.current_dir, args.window_size, args.fail_on_drift))


if __name__ == "__main__":
    main()
