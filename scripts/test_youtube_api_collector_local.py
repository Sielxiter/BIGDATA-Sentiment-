"""Alias entrypoint for the local YouTube API collector test script."""

from __future__ import annotations

import runpy
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LEGACY_SCRIPT = PROJECT_ROOT / "scripts" / "test_reddit_scraper_local.py"


if __name__ == "__main__":
    runpy.run_path(str(LEGACY_SCRIPT), run_name="__main__")
