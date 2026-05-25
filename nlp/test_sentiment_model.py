"""Quick standalone test for the sentiment model layer."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nlp.sentiment_service import SentimentService  # noqa: E402


SAMPLE_TEXTS = [
    "I love this match, Morocco played very well!",
    "This is terrible and disappointing.",
    "The game starts at 8 PM.",
]


def main() -> int:
    service = SentimentService()

    for text in SAMPLE_TEXTS:
        result = service.predict(text)
        print("-" * 80)
        print(f"text: {text}")
        print(f"predicted label: {result['label']}")
        print(f"score: {result['score']}")
        print(f"model name: {result['model_name']}")
        print(f"model version: {result['model_version']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
