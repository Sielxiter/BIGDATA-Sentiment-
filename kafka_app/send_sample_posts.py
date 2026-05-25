"""Send sample posts from CSV to the raw Kafka topic.

This script is for Step 2 verification only. It proves the producer can push
clean JSON messages into Kafka before any collector or Spark work exists.
"""

from __future__ import annotations

import csv
import json
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from kafka_app.producer import KafkaPostProducer  # noqa: E402


SAMPLE_CSV_PATH = PROJECT_ROOT / "data" / "sample_events.csv"


def build_post(row: dict[str, str]) -> dict:
    """Convert a CSV row into the raw message format expected by Kafka."""

    metadata_value = row.get("metadata", "").strip()
    if metadata_value:
        try:
            metadata = json.loads(metadata_value)
        except json.JSONDecodeError:
            metadata = {
                "author": "sample_user",
                "url": None,
                "likes": 0,
                "comments_count": 0,
            }
    else:
        metadata = {
            "author": "sample_user",
            "url": None,
            "likes": 0,
            "comments_count": 0,
        }

    return {
        "id": row["id"].strip(),
        "event": row["event"].strip(),
        "text": row["text"].strip(),
        "source": row["source"].strip(),
        "language": row.get("language", "").strip() or None,
        "created_at": row["created_at"].strip(),
        "metadata": metadata,
    }


def main() -> None:
    if not SAMPLE_CSV_PATH.exists():
        raise FileNotFoundError(f"Sample CSV not found: {SAMPLE_CSV_PATH}")

    producer = KafkaPostProducer()

    try:
        with SAMPLE_CSV_PATH.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)

            for row in reader:
                post = build_post(row)
                print(f"[send_sample_posts] Sending: {json.dumps(post, ensure_ascii=False)}")
                producer.send_post(post)
                time.sleep(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
