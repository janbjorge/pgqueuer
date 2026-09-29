"""Copy the agent guide to the site root as raw Markdown."""

from __future__ import annotations

import shutil
from pathlib import Path

from mkdocs.config.defaults import MkDocsConfig


def copy_agent_guide(docs_dir: Path, site_dir: Path) -> None:
    shutil.copyfile(docs_dir / "agent-guide.md", site_dir / "agent-guide.md")


def on_post_build(config: MkDocsConfig) -> None:
    copy_agent_guide(Path(config.docs_dir), Path(config.site_dir))
