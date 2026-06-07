"""Compatibility wrapper for the legacy Reddit Lambda entrypoint path.

The active collector now uses the YouTube Data API. This module preserves the
old deployment import path so existing AWS Lambda configuration can keep
pointing here during migration.
"""

from __future__ import annotations

from cloud.lambda_youtube_to_kafka import lambda_handler

__all__ = ["lambda_handler"]
