from __future__ import annotations

import os
import statistics
import sys
from datetime import timedelta
from itertools import groupby
from pathlib import Path
from typing import Generator, Literal

from pydantic import AwareDatetime, BaseModel


class BenchmarkResult(BaseModel):
    created_at: AwareDatetime
    driver: Literal["apg", "apgpool", "psy"]
    elapsed: timedelta
    github_ref_name: str
    rate: float
    steps: int


def loader() -> Generator[BenchmarkResult, None, None]:
    """
    Load benchmark results from JSON files in the benchmark directory.
    """
    for file in Path("pgqueuer/benchmark").rglob("*.json"):
        with file.open() as f:
            yield BenchmarkResult.model_validate_json(f.read())


def grouped() -> Generator[tuple[str, list[BenchmarkResult]], None, None]:
    """
    Group benchmark results by driver.
    """
    for driver, group in groupby(
        sorted(loader(), key=lambda x: x.driver),
        key=lambda x: x.driver,
    ):
        yield (
            driver,
            sorted(group, key=lambda x: x.created_at),
        )


def compare_with_dev_branch(
    data: list[tuple[str, list[BenchmarkResult]]],
) -> None:
    """
    Compare benchmark results between the main branch and the current branch.
    Exit with status 1 if the latest benchmark rate from the current branch
    is lower than the threshold.
    """
    branch_name = os.getenv("REF_NAME", "main")
    exit_status = 0

    for driver, results in data:
        # Filter results for the main branch and the current branch
        main = [result for result in results if result.github_ref_name == "main"]
        other = [result for result in results if result.github_ref_name == branch_name]

        if main and other:
            # Calculate mean and standard deviation for main branch results
            main_mean = statistics.mean([result.rate for result in main])
            main_std = statistics.stdev([result.rate for result in main])
            mean_one_std = main_mean - main_std

            # Get the rate for the latest result from the current branch
            last_other_rate = max(other, key=lambda x: x.created_at).rate

            # Print comparison details
            print(f"Driver: {driver}")
            print(
                f"Main branch mean rate: {main_mean:.1f} | "
                f"Std dev: {main_std:.1f} | "
                f"Mean minus one std: {mean_one_std:.1f}"
            )
            print(f"Latest rate ({branch_name} branch): {last_other_rate:.1f}")
            print("-" * 40)

            # Exit with status 1 if the latest rate from the current
            # branch is lower than the threshold
            if last_other_rate < mean_one_std:
                exit_status = 1

    sys.exit(exit_status)


if __name__ == "__main__":
    print("Loading benchmark results...")
    data = list(grouped())
    print("Comparing benchmark results...")
    compare_with_dev_branch(data)
