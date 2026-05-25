"""Reusable text preprocessing helpers."""

from __future__ import annotations

import html
import re


_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")


def clean_text(text: str | None, lowercase: bool = False) -> str:
    """Clean text conservatively for multilingual social media content."""

    if text is None:
        return ""

    cleaned = html.unescape(str(text))
    cleaned = cleaned.replace("\r", " ").replace("\n", " ")
    cleaned = _URL_RE.sub(" ", cleaned)
    cleaned = _MULTISPACE_RE.sub(" ", cleaned).strip()

    if lowercase:
        cleaned = cleaned.lower()

    return cleaned
