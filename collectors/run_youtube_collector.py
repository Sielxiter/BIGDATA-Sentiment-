"""Run the YouTube collector and send normalized comments to Kafka."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from collectors.youtube_collector import YouTubeCollector  # noqa: E402
from config.settings import (  # noqa: E402
    DEFAULT_EVENT_NAME,
    DEFAULT_YOUTUBE_KEYWORDS,
    DEFAULT_YOUTUBE_MAX_COMMENTS,
    DEFAULT_YOUTUBE_MAX_VIDEOS,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect YouTube comments and send them to Kafka.")
    parser.add_argument(
        "--event",
        default=DEFAULT_EVENT_NAME,
        help="Event name used in the normalized Kafka messages.",
    )
    parser.add_argument(
        "--keywords",
        default=DEFAULT_YOUTUBE_KEYWORDS,
        help="Comma-separated YouTube search keywords.",
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=DEFAULT_YOUTUBE_MAX_VIDEOS,
        help="Maximum number of videos to inspect per keyword.",
    )
    parser.add_argument(
        "--max-comments",
        type=int,
        default=DEFAULT_YOUTUBE_MAX_COMMENTS,
        help="Maximum number of comments to collect per video.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    keywords = [keyword.strip() for keyword in args.keywords.split(",") if keyword.strip()]

    try:
        collector = YouTubeCollector(
            event_name=args.event,
            keywords=keywords,
            max_videos=args.max_videos,
            max_comments=args.max_comments,
        )
    except RuntimeError as exc:
        print(f"[run_youtube_collector] {exc}")
        return 1

    from kafka_app.producer import KafkaPostProducer  # noqa: E402

    producer = KafkaPostProducer()
    sent_count = 0

    try:
        for item in collector.collect():
            if not item or not item.get("text"):
                continue
            producer.send_post(item)
            sent_count += 1
    finally:
        producer.close()

    print(f"[run_youtube_collector] Sent {sent_count} message(s) to Kafka topic raw_social_posts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
