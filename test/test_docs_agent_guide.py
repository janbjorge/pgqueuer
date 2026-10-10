"""The published site serves the agent guide as raw Markdown."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_agent_guide_is_published_as_raw_markdown(tmp_path: Path) -> None:
    build = pytest.importorskip("mkdocs.commands.build").build
    load_config = pytest.importorskip("mkdocs.config").load_config
    config = load_config(config_file="mkdocs.yml")
    config.site_dir = str(tmp_path / "site")
    build(config)
    published = Path(config.site_dir) / "agent-guide.md"
    source = Path("docs/agent-guide.md")
    assert published.read_bytes() == source.read_bytes()
    assert not (Path(config.site_dir) / "agent-guide" / "index.html").exists()
