"""Shared model, NDJSON storage and gate math for benchmark history tooling."""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel

Verdict = Literal["pass", "warn", "fail", "skip"]

MAIN_REF = "main"
WINDOW_SIZE = 30
MIN_SAMPLES = 5
FAIL_RATIO = 0.7
WARN_RATIO = 0.8
RUNS_PER_JOB = 3  # keep in sync with the benchmark loop in ci.yml
DRIFT_RECENT = 3 * RUNS_PER_JOB  # three CI runs of history
DRIFT_RATIO = 0.85


class BenchmarkRow(BaseModel):
    """One benchmark sample; tolerant of fields absent in pre-2025 rows."""

    created_at: AwareDatetime
    driver: Literal["apg", "apgpool", "psy", "mem"]
    strategy: Literal["throughput", "drain"] = "throughput"
    elapsed: timedelta
    github_ref_name: str
    rate: float
    steps: int
    queued: int | None = None

    def dedupe_key(self) -> tuple[AwareDatetime, str, str]:
        return (self.created_at, self.driver, self.strategy)

    def combo(self) -> tuple[str, str]:
        return (self.driver, self.strategy)


def load_ndjson_dir(data_dir: Path) -> list[BenchmarkRow]:
    """Load all rows from `benchmark/*.ndjson` under a history directory."""
    rows = list[BenchmarkRow]()
    for file in sorted((data_dir / "benchmark").glob("*.ndjson")):
        with file.open() as fh:
            rows.extend(BenchmarkRow.model_validate_json(line) for line in fh if line.strip())
    return rows


def load_json_dir(json_dir: Path) -> list[BenchmarkRow]:
    """Load rows from a directory tree of single-result `*.json` files."""
    return [
        BenchmarkRow.model_validate_json(file.read_text())
        for file in sorted(json_dir.rglob("*.json"))
    ]


def merge_rows(
    existing: list[BenchmarkRow],
    incoming: list[BenchmarkRow],
) -> list[BenchmarkRow]:
    """Merge main-ref rows, deduped on (created_at, driver, strategy), sorted by time."""
    merged = {
        row.dedupe_key(): row
        for rows in (existing, incoming)
        for row in rows
        if row.github_ref_name == MAIN_REF
    }
    return sorted(merged.values(), key=lambda row: row.created_at)


def month_key(row: BenchmarkRow) -> str:
    return row.created_at.strftime("%Y-%m")


def write_monthly_ndjson(rows: list[BenchmarkRow], data_dir: Path) -> list[Path]:
    """Rewrite `benchmark/<YYYY-MM>.ndjson` files; return the paths written."""
    out_dir = data_dir / "benchmark"
    out_dir.mkdir(parents=True, exist_ok=True)
    written = list[Path]()
    for month in sorted({month_key(row) for row in rows}):
        file = out_dir / f"{month}.ndjson"
        with file.open("w") as fh:
            fh.writelines(f"{row.model_dump_json()}\n" for row in rows if month_key(row) == month)
        written.append(file)
    return written


@dataclass(frozen=True)
class GateResult:
    verdict: Verdict
    current_rate: float
    current_count: int
    sample_count: int
    median: float | None = None

    @property
    def fail_threshold(self) -> float | None:
        return None if self.median is None else self.median * FAIL_RATIO

    @property
    def warn_threshold(self) -> float | None:
        return None if self.median is None else self.median * WARN_RATIO


def gate(baseline_rates: list[float], current_rates: list[float]) -> GateResult:
    """
    Gate the median of the fresh rates against the baseline-window median:
    fail below FAIL_RATIO of it, warn below WARN_RATIO, skip on a thin
    baseline. The ratios are calibrated for a median of RUNS_PER_JOB fresh
    runs; see docs/comparisons/benchmarks.md for the backtest behind them.
    """
    current_rate = statistics.median(current_rates)
    if len(baseline_rates) < MIN_SAMPLES:
        return GateResult(
            verdict="skip",
            current_rate=current_rate,
            current_count=len(current_rates),
            sample_count=len(baseline_rates),
        )

    median = statistics.median(baseline_rates)
    if current_rate < median * FAIL_RATIO:
        verdict: Verdict = "fail"
    elif current_rate < median * WARN_RATIO:
        verdict = "warn"
    else:
        verdict = "pass"

    return GateResult(
        verdict=verdict,
        current_rate=current_rate,
        current_count=len(current_rates),
        sample_count=len(baseline_rates),
        median=median,
    )


@dataclass(frozen=True)
class DriftResult:
    verdict: Literal["ok", "drift", "skip"]
    recent_median: float | None = None
    prior_median: float | None = None

    @property
    def threshold(self) -> float | None:
        return None if self.prior_median is None else self.prior_median * DRIFT_RATIO


def drift(baseline_rates: list[float]) -> DriftResult:
    """
    Detect sustained decay inside the baseline window: drift when the median
    of the newest DRIFT_RECENT samples falls below DRIFT_RATIO of the prior
    samples' median. Catches gradual regressions too mild for the per-run
    gate; see docs/comparisons/benchmarks.md for the backtest behind it.
    """
    if len(baseline_rates) < DRIFT_RECENT + MIN_SAMPLES:
        return DriftResult(verdict="skip")

    recent_median = statistics.median(baseline_rates[-DRIFT_RECENT:])
    prior_median = statistics.median(baseline_rates[:-DRIFT_RECENT])
    return DriftResult(
        verdict="drift" if recent_median < prior_median * DRIFT_RATIO else "ok",
        recent_median=recent_median,
        prior_median=prior_median,
    )


def baseline_window(
    history: list[BenchmarkRow],
    combo: tuple[str, str],
    window_size: int = WINDOW_SIZE,
) -> list[float]:
    """Rates of the newest `window_size` main-ref samples for a (driver, strategy)."""
    matching = sorted(
        (row for row in history if row.combo() == combo and row.github_ref_name == MAIN_REF),
        key=lambda row: row.created_at,
    )
    return [row.rate for row in matching[-window_size:]]
