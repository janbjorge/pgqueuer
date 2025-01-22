# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "plotly",
#     "pydantic",
#     "httpx",
# ]
# ///
from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import datetime, timedelta

import httpx
import plotly.graph_objects as go
import plotly.subplots as sp
from pydantic import BaseModel, Field


def pepy_api_key() -> str:
    if key := os.environ.get("PEPY_API_KEY"):
        return key
    raise RuntimeError("Missing env: `PEPY_API_KEY`")


class PackageStats(BaseModel):
    total_downloads: int = Field(
        ...,
        description="Total number of downloads across all versions",
    )
    id: str = Field(
        ...,
        description="Package name",
    )
    versions: list[str] = Field(
        ...,
        description="List of available versions",
    )
    downloads: dict[datetime, dict[str, int]] = Field(
        ...,
        description="Daily download counts per version",
    )

    class Config:
        extra = "forbid"


def fetch() -> PackageStats:
    return PackageStats.model_validate_json(
        httpx.get(
            "https://api.pepy.tech/api/v2/projects/pgqueuer",
            headers={"X-API-Key": pepy_api_key()},
        ).content
    )


def plot_downloads(data: PackageStats) -> None:
    # Group downloads by minor version
    downloads: defaultdict[
        datetime,
        defaultdict[str, int],
    ] = defaultdict(lambda: defaultdict(int))

    for date, version_counts in data.downloads.items():
        for version, count in version_counts.items():
            if mv := re.match(r"^\d+\.\d+", version):
                downloads[date][mv.group(0)] += count

    # Accumulated
    accumulated = defaultdict[datetime, int](int)
    for date, version_counts in downloads.items():
        accumulated[date] = sum(version_counts.values()) + accumulated[date - timedelta(days=1)]

    # Prepare data for pie chart
    totals = defaultdict[str, int](int)
    for version_counts in downloads.values():
        for version, count in version_counts.items():
            totals[version] += count

    # Downloads over time by minor version
    dates = sorted(downloads.keys())
    versions = sorted(
        {v for versions in downloads.values() for v in versions},
        key=lambda x: (len(x), x),
    )

    # Create a combined figure with subplots of different types
    fig = sp.make_subplots(
        rows=2,
        cols=2,
        specs=[
            [{"type": "xy"}, {"type": "xy"}],
            [{"type": "domain"}, None],
        ],
        horizontal_spacing=0.1,  # Adjust spacing between subplots
        subplot_titles=(
            "Downloads by versions",
            "Accumulated downloads",
            "Downloads by versions",
        ),
    )

    # Line plot for downloads over time by minor version
    for version in versions:
        counts = [downloads[date][version] for date in dates]
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=counts,
                mode="lines+markers",
                name=f"v{version} / {sum(counts)}",
            ),
            row=1,
            col=1,
        )

    # Add accumulated downloads on a secondary y-axis
    fig.add_trace(
        go.Scatter(
            x=list(accumulated.keys()),
            y=list(accumulated.values()),
            mode="lines+markers",
            name="Accumulated Downloads",
        ),
        row=1,
        col=2,
    )

    fig.update_xaxes(title_text="Date", row=1, col=1)
    fig.update_xaxes(title_text="Date", row=1, col=2)
    fig.update_yaxes(title_text="Download Count", row=1, col=1)
    fig.update_yaxes(title_text="Accumulated Downloads", row=1, col=2)

    # Pie chart of downloads by version
    fig.add_trace(
        go.Pie(
            labels=[f"v{version} / {count}" for version, count in totals.items()],
            values=list(totals.values()),
            hole=0.3,
        ),
        row=2,
        col=1,
    )

    # Adjust title positions to make them closer to their respective plots
    fig.update_annotations({"xanchor": "center", "yanchor": "bottom"})

    fig.update_layout(
        title={"text": f"Downloads Analysis for Package: {data.id}", "x": 0.5},
        template="plotly_white",
    )
    fig.show()


if __name__ == "__main__":
    plot_downloads(fetch())
