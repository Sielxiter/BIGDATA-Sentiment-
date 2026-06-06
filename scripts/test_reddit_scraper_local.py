"""Local Reddit scraper test script.

This script is meant for workstation or EC2 validation before Lambda deployment.
It uses the same collector helper as the dashboard and Lambda entrypoint.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from cloud.reddit_scraper_collector import run_reddit_collection  # noqa: E402
from config.settings import settings  # noqa: E402


def _parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Test the Reddit scraper locally.")
    parser.add_argument("--event", default=settings.default_event_name, help="Event name to associate with the scrape.")
    parser.add_argument("--keywords", default=settings.default_reddit_keywords, help="Comma-separated keyword list.")
    parser.add_argument("--subreddits", default=settings.default_reddit_subreddits, help="Comma-separated subreddit list.")
    parser.add_argument("--max-posts", type=int, default=settings.reddit_max_posts, help="Maximum number of posts to scan.")
    parser.add_argument(
        "--max-comments",
        type=int,
        default=settings.reddit_max_comments_per_post,
        help="Maximum number of comments to collect per post.",
    )
    parser.add_argument(
        "--send-to-kafka",
        default="false",
        help="Send scraped documents to Kafka (true/false).",
    )
    parser.add_argument("--output-jsonl", default="", help="Optional JSONL output path.")
    parser.add_argument("--sort-mode", default=settings.default_reddit_sort_mode, choices=["relevance", "hot", "new", "top"])
    parser.add_argument("--time-filter", default=settings.default_reddit_time_filter, help="Optional Reddit time filter.")
    parser.add_argument("--engine", default=settings.reddit_scraper_engine, help="Scraper engine label for metadata.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    result = run_reddit_collection(
        event_name=args.event,
        keywords=args.keywords,
        subreddits=args.subreddits,
        max_posts=int(args.max_posts),
        max_comments_per_post=int(args.max_comments),
        sort_mode=str(args.sort_mode),
        time_filter=str(args.time_filter) or None,
        engine=str(args.engine),
        publish_to_kafka=_parse_bool(args.send_to_kafka),
        backup_to_s3=False,
        include_documents=True,
        kafka_topic=settings.kafka_raw_topic,
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    documents = result.get("documents", [])
    print(f"Status: {result.get('status')}")
    print(f"Posts scanned: {result.get('posts_scanned', 0)}")
    print(f"Comments collected: {result.get('comments_collected', 0)}")
    print(f"Messages published: {result.get('messages_published', 0)}")

    if result.get("errors"):
        print("Warnings / errors:")
        for error in result["errors"]:
            print(f"- {error}")

    print("Sample documents:")
    for sample in documents[:3]:
        print(json.dumps(sample, ensure_ascii=False, indent=2))

    output_jsonl = str(args.output_jsonl).strip()
    if output_jsonl:
        output_path = Path(output_jsonl)
        _write_jsonl(output_path, documents)
        print(f"Wrote {len(documents)} document(s) to {output_path}")

    return 0 if result.get("comments_collected", 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
