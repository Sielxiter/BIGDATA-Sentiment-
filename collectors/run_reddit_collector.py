"""Run the Reddit collector and send normalized items to Kafka."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from collectors.reddit_collector import RedditCollector  # noqa: E402
from config.settings import DEFAULT_EVENT_NAME, DEFAULT_REDDIT_KEYWORDS, DEFAULT_REDDIT_LIMIT  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Reddit posts and send them to Kafka.")
    parser.add_argument(
        "--event",
        default=DEFAULT_EVENT_NAME,
        help="Event name used in the normalized Kafka messages.",
    )
    parser.add_argument(
        "--keywords",
        default=DEFAULT_REDDIT_KEYWORDS,
        help="Comma-separated Reddit search keywords.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_REDDIT_LIMIT,
        help="Maximum number of search results per keyword.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    keywords = [keyword.strip() for keyword in args.keywords.split(",") if keyword.strip()]

    try:
        collector = RedditCollector(
            event_name=args.event,
            keywords=keywords,
            limit=args.limit,
        )
    except RuntimeError as exc:
        print(f"[run_reddit_collector] {exc}")
        return 1

    from kafka_app.producer import KafkaPostProducer  # noqa: E402

    producer = KafkaPostProducer()
    sent_count = 0

    try:
        for item in collector.collect_posts():
            if not item or not item.get("text"):
                continue
            producer.send_post(item)
            sent_count += 1
    finally:
        producer.close()

    print(f"[run_reddit_collector] Sent {sent_count} message(s) to Kafka topic raw_social_posts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
