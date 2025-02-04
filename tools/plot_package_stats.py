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
from collections import Counter, defaultdict
from datetime import datetime, timedelta

import httpx
import plotly.graph_objects as go
import plotly.subplots as sp
from pydantic import BaseModel, Field


def pepy_api_key() -> str:
    """Return PEPY API key from env var PEPY_API_KEY."""
    if key := os.environ.get("PEPY_API_KEY"):
        return key
    raise RuntimeError("Missing env: `PEPY_API_KEY`")


class PackageStats(BaseModel):
    """Represent package download statistics from PEPY."""

    total_downloads: int = Field(..., description="Total downloads of all versions")
    id: str = Field(..., description="Package name")
    versions: list[str] = Field(..., description="List of available versions")
    downloads: dict[datetime, dict[str, int]] = Field(
        ..., description="Daily download counts per version"
    )

    class Config:
        extra = "forbid"


def fetch() -> PackageStats:
    """Fetch and return package stats from PEPY."""
    return PackageStats.model_validate_json(
        httpx.get(
            "https://api.pepy.tech/api/v2/projects/pgqueuer",
            headers={"X-API-Key": pepy_api_key()},
        ).content
    )


def plot_downloads(data: PackageStats) -> None:
    """Create and display a Plotly figure of the package download stats."""
    downloads: defaultdict[datetime, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
    for date, vers_counts in data.downloads.items():
        for version, count in vers_counts.items():
            if mv := re.match(r"^\d+\.\d+", version):
                downloads[date][mv.group(0)] += count

    accumulated = defaultdict[datetime, int](int)
    for date in sorted(downloads.keys()):
        accumulated[date] = sum(downloads[date].values()) + accumulated.get(
            date - timedelta(days=1), 0
        )

    totals = Counter[str]()
    for vers_counts in downloads.values():
        for version, count in vers_counts.items():
            totals[version] += count

    dates = sorted(downloads.keys())
    versions = sorted({v for vc in downloads.values() for v in vc}, key=lambda x: (len(x), x))

    fig = sp.make_subplots(
        rows=2,
        cols=2,
        specs=[[{"type": "xy"}, {"type": "xy"}], [{"type": "xy", "colspan": 2}, None]],
        subplot_titles=[
            "Downloads by Version",
            "Accumulated Downloads",
            "Version Distribution (Bar)",
        ],
    )

    for version in versions:
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=[downloads[d][version] for d in dates],
                mode="lines+markers",
                name=f"v{version}",
            ),
            row=1,
            col=1,
        )

    fig.add_trace(
        go.Scatter(
            x=list(accumulated.keys()),
            y=list(accumulated.values()),
            mode="lines+markers",
            name="Accumulated",
        ),
        row=1,
        col=2,
    )

    def sort_key(v: str) -> tuple[int, ...]:
        return tuple(map(int, v.split(".")))

    xy = [(x, y) for x, y in sorted(totals.items(), key=lambda i: sort_key(i[0]))]

    fig.add_trace(
        go.Bar(
            x=[f"v{x}" for x, _ in xy],
            y=[y for _, y in xy],
            name="Total Downloads",
        ),
        row=2,
        col=1,
    )

    fig.update_xaxes(title_text="Date", row=1, col=1)
    fig.update_xaxes(title_text="Date", row=1, col=2)
    fig.update_xaxes(title_text="Version", row=2, col=1)

    fig.update_yaxes(title_text="Count", row=1, col=1)
    fig.update_yaxes(title_text="Accumulated", row=1, col=2)
    fig.update_yaxes(title_text="Downloads", row=2, col=1)

    fig.update_layout(
        title={"text": f"Downloads Analysis: {data.id}", "x": 0.5},
        template="plotly_white",
        margin={"l": 40, "r": 40, "t": 80, "b": 40},
    )
    fig.show()


if __name__ == "__main__":
    plot_downloads(fetch())
