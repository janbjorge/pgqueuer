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
MAD_FAIL_MULTIPLIER = 3.0
MAD_WARN_MULTIPLIER = 2.0
ZERO_MAD_FAIL_RATIO = 0.95
ZERO_MAD_WARN_RATIO = 0.975


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


def write_yearly_ndjson(rows: list[BenchmarkRow], data_dir: Path) -> list[Path]:
    """Rewrite `benchmark/<year>.ndjson` files; return the paths written."""
    out_dir = data_dir / "benchmark"
    out_dir.mkdir(parents=True, exist_ok=True)
    written = list[Path]()
    for year in sorted({row.created_at.year for row in rows}):
        file = out_dir / f"{year}.ndjson"
        with file.open("w") as fh:
            fh.writelines(
                f"{row.model_dump_json()}\n" for row in rows if row.created_at.year == year
            )
        written.append(file)
    return written


@dataclass(frozen=True)
class GateResult:
    verdict: Verdict
    current_rate: float
    sample_count: int
    median: float | None = None
    mad: float | None = None
    fail_threshold: float | None = None
    warn_threshold: float | None = None


def gate(baseline_rates: list[float], current_rate: float) -> GateResult:
    """
    Gate a fresh rate against a baseline window.

    Fail below median - 3*MAD, warn below median - 2*MAD, skip when the
    baseline is too thin to be meaningful. A zero MAD (degenerate window)
    falls back to fixed ratios of the median.
    """
    if len(baseline_rates) < MIN_SAMPLES:
        return GateResult(
            verdict="skip",
            current_rate=current_rate,
            sample_count=len(baseline_rates),
        )

    median = statistics.median(baseline_rates)
    mad = statistics.median([abs(rate - median) for rate in baseline_rates])
    if mad > 0:
        fail_threshold = median - MAD_FAIL_MULTIPLIER * mad
        warn_threshold = median - MAD_WARN_MULTIPLIER * mad
    else:
        fail_threshold = median * ZERO_MAD_FAIL_RATIO
        warn_threshold = median * ZERO_MAD_WARN_RATIO

    if current_rate < fail_threshold:
        verdict: Verdict = "fail"
    elif current_rate < warn_threshold:
        verdict = "warn"
    else:
        verdict = "pass"

    return GateResult(
        verdict=verdict,
        current_rate=current_rate,
        sample_count=len(baseline_rates),
        median=median,
        mad=mad,
        fail_threshold=fail_threshold,
        warn_threshold=warn_threshold,
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
