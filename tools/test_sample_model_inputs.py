"""Run quick sample-text predictions using the configured sentiment service.

Usage:
    python tools/test_sample_model_inputs.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nlp.sentiment_service import SentimentService


SAMPLE_TEXTS = [
    "I love this match, Morocco played very well!",
    "This is terrible and disappointing.",
    "I am scared about what will happen next.",
    "This video is very informative.",
    "I feel sad and angry.",
]


def main() -> int:
    service = SentimentService()
    print("[test_sample_model_inputs] Model prediction smoke test")

    for text in SAMPLE_TEXTS:
        prediction = service.predict(text)
        print("-" * 80)
        print(f"TEXT: {text}")
        print(f"PRED: {prediction}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
