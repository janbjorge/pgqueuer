from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tools.append_benchmarks import append
from tools.benchmark_data import (
    BenchmarkRow,
    baseline_window,
    gate,
    load_ndjson_dir,
    merge_rows,
    write_yearly_ndjson,
)
from tools.compare_rps import compare


def make_row(
    rate: float,
    created_at: datetime,
    driver: str = "apg",
    strategy: str = "throughput",
    github_ref_name: str = "main",
) -> BenchmarkRow:
    return BenchmarkRow.model_validate(
        {
            "created_at": created_at,
            "driver": driver,
            "strategy": strategy,
            "elapsed": timedelta(seconds=10),
            "github_ref_name": github_ref_name,
            "rate": rate,
            "steps": int(rate * 10),
        }
    )


def utc(hours: int) -> datetime:
    return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=hours)


def test_gate_skips_thin_baseline() -> None:
    assert gate([100.0] * 4, 50.0).verdict == "skip"


def test_gate_passes_stable_baseline() -> None:
    baseline = [100.0, 101.0, 99.0, 100.5, 99.5, 100.0]
    assert gate(baseline, 99.0).verdict == "pass"


def test_gate_warns_between_two_and_three_mad() -> None:
    baseline = [95.0, 98.0, 100.0, 102.0, 105.0]
    result = gate(baseline, 95.0)
    assert result.median == 100.0
    assert result.mad == 2.0
    assert result.warn_threshold == 96.0
    assert result.fail_threshold == 94.0
    assert result.verdict == "warn"


def test_gate_fails_below_three_mad() -> None:
    baseline = [95.0, 98.0, 100.0, 102.0, 105.0]
    assert gate(baseline, 90.0).verdict == "fail"


def test_gate_tolerates_single_outlier() -> None:
    """One crazy-slow CI run must not widen the gate the way mean-std would."""
    baseline = [100.0, 101.0, 99.0, 100.0, 12.0, 100.5, 99.5]
    result = gate(baseline, 95.0)
    assert result.verdict == "fail"


def test_gate_zero_mad_falls_back_to_ratio() -> None:
    baseline = [100.0] * 10
    result = gate(baseline, 96.0)
    assert result.fail_threshold == 95.0
    assert result.warn_threshold == 97.5
    assert result.verdict == "warn"
    assert gate(baseline, 94.0).verdict == "fail"
    assert gate(baseline, 98.0).verdict == "pass"


def test_baseline_window_takes_newest_main_rows() -> None:
    history = [make_row(float(i), utc(i)) for i in range(40)]
    history.append(make_row(999.0, utc(50), github_ref_name="feature"))
    history.append(make_row(555.0, utc(51), driver="psy"))
    window = baseline_window(history, ("apg", "throughput"), window_size=30)
    assert window == [float(i) for i in range(10, 40)]


def test_merge_rows_dedupes_and_drops_non_main() -> None:
    row = make_row(100.0, utc(0))
    merged = merge_rows(
        [row],
        [row, make_row(200.0, utc(1)), make_row(300.0, utc(2), github_ref_name="pr")],
    )
    assert [r.rate for r in merged] == [100.0, 200.0]


def test_write_and_load_ndjson_roundtrip(tmp_path: Path) -> None:
    rows = [
        make_row(100.0, datetime(2025, 12, 31, tzinfo=timezone.utc)),
        make_row(101.0, datetime(2026, 1, 1, tzinfo=timezone.utc)),
    ]
    written = write_yearly_ndjson(rows, tmp_path)
    assert [file.name for file in written] == ["2025.ndjson", "2026.ndjson"]
    assert load_ndjson_dir(tmp_path) == rows


def test_load_pre_2025_row_without_strategy_and_queued() -> None:
    legacy = json.dumps(
        {
            "created_at": "2024-09-28T11:51:23.738829Z",
            "driver": "apg",
            "elapsed": "PT9.97222S",
            "github_ref_name": "main",
            "rate": 1479.98,
            "steps": 20700,
        }
    )
    row = BenchmarkRow.model_validate_json(legacy)
    assert row.strategy == "throughput"
    assert row.queued is None


def write_current(current_dir: Path, rows: list[BenchmarkRow]) -> None:
    current_dir.mkdir(parents=True, exist_ok=True)
    for i, row in enumerate(rows):
        (current_dir / f"benchmark-{i}.json").write_text(row.model_dump_json())


def test_append_is_idempotent(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    new_dir = tmp_path / "new"
    write_current(new_dir, [make_row(100.0, utc(0)), make_row(101.0, utc(1))])

    append(data_dir, new_dir)
    first = load_ndjson_dir(data_dir)
    append(data_dir, new_dir)
    assert load_ndjson_dir(data_dir) == first
    assert len(first) == 2


@pytest.mark.parametrize(
    ("current_rate", "expected_exit"),
    [(100.0, 0), (50.0, 1)],
)
def test_compare_exit_status(
    tmp_path: Path,
    current_rate: float,
    expected_exit: int,
) -> None:
    data_dir = tmp_path / "data"
    write_yearly_ndjson([make_row(100.0 + i, utc(i)) for i in range(10)], data_dir)
    current_dir = tmp_path / "current"
    write_current(current_dir, [make_row(current_rate, utc(100))])
    assert compare(data_dir, current_dir, window_size=30) == expected_exit


def test_compare_empty_current_dir_fails(tmp_path: Path) -> None:
    (tmp_path / "current").mkdir()
    assert compare(tmp_path, tmp_path / "current", window_size=30) == 1
