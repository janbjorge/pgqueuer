from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tools.append_benchmarks import append
from tools.benchmark_data import (
    BenchmarkRow,
    baseline_window,
    drift,
    gate,
    load_ndjson_dir,
    merge_rows,
    write_monthly_ndjson,
)
from tools.compare_rps import compare
from tools.fetch_history import (
    ArtifactListing,
    convert_archive,
    extract_history,
    newest_artifact,
)


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
    assert gate([100.0] * 4, [50.0]).verdict == "skip"


def test_gate_passes_stable_baseline() -> None:
    baseline = [100.0, 101.0, 99.0, 100.5, 99.5, 100.0]
    assert gate(baseline, [99.0, 98.0, 101.0]).verdict == "pass"


def test_gate_thresholds_are_ratios_of_median() -> None:
    baseline = [95.0, 98.0, 100.0, 102.0, 105.0]
    result = gate(baseline, [75.0, 76.0, 74.0])
    assert result.median == 100.0
    assert result.fail_threshold == 70.0
    assert result.warn_threshold == 80.0
    assert result.verdict == "warn"


def test_gate_fails_below_ratio() -> None:
    baseline = [95.0, 98.0, 100.0, 102.0, 105.0]
    assert gate(baseline, [65.0, 60.0, 69.0]).verdict == "fail"


def test_gate_scores_median_of_current_runs() -> None:
    """One crazy-slow run out of three must not fail the gate."""
    baseline = [100.0] * 10
    result = gate(baseline, [12.0, 99.0, 101.0])
    assert result.current_rate == 99.0
    assert result.current_count == 3
    assert result.verdict == "pass"


def test_gate_single_current_run_still_gates() -> None:
    baseline = [100.0] * 10
    assert gate(baseline, [65.0]).verdict == "fail"
    assert gate(baseline, [99.0]).verdict == "pass"


def test_drift_skips_thin_history() -> None:
    assert drift([100.0] * 10).verdict == "skip"


def test_drift_steady_history_is_ok() -> None:
    window = [100.0, 101.0, 99.0] * 10
    assert drift(window).verdict == "ok"


def test_drift_flags_sustained_regression() -> None:
    """Slow 20% decay must trip the drift check even though the gate passes."""
    window = [100.0] * 21 + [80.0] * 9
    result = drift(window)
    assert result.verdict == "drift"
    assert result.prior_median == 100.0
    assert result.recent_median == 80.0
    assert result.threshold == 85.0
    assert gate(window, [80.0, 81.0, 79.0]).verdict == "pass"


def test_drift_single_outlier_run_does_not_trip() -> None:
    window = [100.0] * 24 + [10.0, 10.0, 10.0] + [100.0] * 3
    assert drift(window).verdict == "ok"


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
    written = write_monthly_ndjson(rows, tmp_path)
    assert [file.name for file in written] == ["2025-12.ndjson", "2026-01.ndjson"]
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


def make_listing(*artifacts: dict[str, object]) -> ArtifactListing:
    defaults: dict[str, object] = {
        "name": "benchmark-history",
        "expired": False,
        "created_at": "2026-07-16T00:00:00Z",
        "archive_download_url": "https://api.github.invalid/zip",
    }
    return ArtifactListing.model_validate({"artifacts": [defaults | a for a in artifacts]})


def test_newest_artifact_picks_latest_non_expired() -> None:
    listing = make_listing(
        {"created_at": "2026-07-14T00:00:00Z", "archive_download_url": "old"},
        {"created_at": "2026-07-16T00:00:00Z", "expired": True, "archive_download_url": "dead"},
        {"created_at": "2026-07-15T00:00:00Z", "archive_download_url": "newest-live"},
        {"created_at": "2026-07-16T00:00:00Z", "name": "other", "archive_download_url": "wrong"},
    )
    artifact = newest_artifact(listing)
    assert artifact is not None
    assert artifact.archive_download_url == "newest-live"


def test_newest_artifact_empty_listing() -> None:
    assert newest_artifact(make_listing()) is None


def test_extract_history_roundtrip(tmp_path: Path) -> None:
    row = make_row(100.0, utc(0))
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("2026-01.ndjson", f"{row.model_dump_json()}\n")
        archive.writestr("README.md", "ignored")

    extracted = extract_history(buffer.getvalue(), tmp_path)

    assert [file.name for file in extracted] == ["2026-01.ndjson"]
    assert load_ndjson_dir(tmp_path) == [row]


def test_convert_archive_from_single_json_files(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    write_current(
        archive_dir,
        [make_row(100.0, utc(0)), make_row(200.0, utc(1), github_ref_name="pr")],
    )

    written = convert_archive(archive_dir, tmp_path / "dest")

    assert [file.name for file in written] == ["2026-01.ndjson"]
    assert [row.rate for row in load_ndjson_dir(tmp_path / "dest")] == [100.0]


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
    write_monthly_ndjson([make_row(100.0 + i, utc(i)) for i in range(10)], data_dir)
    current_dir = tmp_path / "current"
    write_current(current_dir, [make_row(current_rate, utc(100))])
    assert compare(data_dir, current_dir, window_size=30) == expected_exit


def test_compare_gates_median_of_runs_per_combo(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    write_monthly_ndjson([make_row(100.0 + i, utc(i)) for i in range(10)], data_dir)
    current_dir = tmp_path / "current"
    write_current(
        current_dir,
        [make_row(10.0, utc(100)), make_row(100.0, utc(101)), make_row(101.0, utc(102))],
    )
    assert compare(data_dir, current_dir, window_size=30) == 0


def test_compare_fail_on_drift_only_when_asked(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    rows = [make_row(100.0, utc(i)) for i in range(21)]
    rows += [make_row(80.0, utc(21 + i)) for i in range(9)]
    write_monthly_ndjson(rows, data_dir)
    current_dir = tmp_path / "current"
    write_current(current_dir, [make_row(80.0, utc(100))])
    assert compare(data_dir, current_dir, window_size=30) == 0
    assert compare(data_dir, current_dir, window_size=30, fail_on_drift=True) == 1


def test_compare_empty_current_dir_fails(tmp_path: Path) -> None:
    (tmp_path / "current").mkdir()
    assert compare(tmp_path, tmp_path / "current", window_size=30) == 1
