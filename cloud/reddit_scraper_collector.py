"""Compatibility wrapper for the legacy Reddit collector import path.

The project has moved to the YouTube Data API collector, but older imports
still resolve through this module so existing scripts do not break.
"""

from __future__ import annotations

from cloud.youtube_api_collector import YouTubeAPICollector as RedditScraperCollector
from cloud.youtube_api_collector import run_youtube_collection as run_reddit_collection

__all__ = ["RedditScraperCollector", "run_reddit_collection"]
