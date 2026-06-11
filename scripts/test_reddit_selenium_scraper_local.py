"""Local runner for the standalone Selenium Reddit scraper."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from cloud.reddit_selenium_scraper import run_reddit_selenium_collection  # noqa: E402


def _bool_arg(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError("Expected true or false.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the standalone Selenium Reddit scraper.")
    parser.add_argument("--event", default="", help="Event name used in normalized records.")
    parser.add_argument("--keywords", default="", help="Comma-separated search keywords.")
    parser.add_argument("--subreddits", default="", help="Optional comma-separated subreddit names.")
    parser.add_argument("--max-posts", type=int, default=20, help="Maximum posts to open per keyword/subreddit.")
    parser.add_argument(
        "--max-comments",
        type=int,
        default=50,
        help="Maximum comments to collect per post.",
    )
    parser.add_argument("--sort", default="relevance", help="Reddit search sort mode.")
    parser.add_argument("--time-filter", default="", help="Reddit time filter such as day, week, month, year.")
    parser.add_argument("--headless", type=_bool_arg, default=True, help="Run the browser headlessly.")
    parser.add_argument(
        "--batch-comment-limit",
        type=int,
        default=200,
        help="Publish a Kafka batch after this many new comments are buffered.",
    )
    parser.add_argument(
        "--batch-sleep-seconds",
        type=float,
        default=100.0,
        help="Sleep between continuous collection cycles.",
    )
    parser.add_argument(
        "--continuous",
        type=_bool_arg,
        default=True,
        help="Keep collecting until the script is stopped manually.",
    )
    parser.add_argument(
        "--use-chrome-profile",
        type=_bool_arg,
        default=False,
        help="Launch Selenium with a real local Chrome profile.",
    )
    parser.add_argument(
        "--chrome-user-data-dir",
        default="",
        help="Chrome user data directory, for example C:\\Users\\My-PC\\AppData\\Local\\Google\\Chrome\\User Data",
    )
    parser.add_argument(
        "--chrome-profile-directory",
        default="Default",
        help="Chrome profile directory inside the user data dir, for example Default or Profile 1.",
    )
    parser.add_argument(
        "--use-s3-config",
        type=_bool_arg,
        default=True,
        help="Load defaults from COLLECTOR_CONFIG_S3_KEY before applying CLI overrides.",
    )
    parser.add_argument(
        "--send-to-kafka",
        type=_bool_arg,
        default=False,
        help="Publish normalized comments to Kafka.",
    )
    parser.add_argument(
        "--output-jsonl",
        default="",
        help="Optional output path. Defaults to raw/youtube/reddit_<event>_...jsonl",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_reddit_selenium_collection(
        event_name=args.event or None,
        keywords=args.keywords or None,
        subreddits=args.subreddits or None,
        max_posts=args.max_posts,
        max_comments_per_post=args.max_comments,
        sort_mode=args.sort,
        time_filter=args.time_filter,
        batch_comment_limit=args.batch_comment_limit,
        batch_sleep_seconds=args.batch_sleep_seconds,
        continuous=args.continuous,
        headless=args.headless,
        use_chrome_profile=args.use_chrome_profile,
        chrome_user_data_dir=args.chrome_user_data_dir or None,
        chrome_profile_directory=args.chrome_profile_directory or None,
        send_to_kafka=args.send_to_kafka,
        output_jsonl_path=args.output_jsonl or None,
        use_s3_runtime_config=args.use_s3_config,
    )

    print(f"Status: {result['status']}")
    print(f"Session ID: {result['session_id']}")
    print(f"Posts scanned: {result['posts_scanned']}")
    print(f"Comments collected: {result['comments_collected']}")
    print(f"Messages published: {result['messages_published']}")
    print(f"Kafka topic: {result['kafka_topic']}")
    print(f"Output JSONL: {result['output_jsonl_path']}")
    if result["errors"]:
        print("Warnings / errors:")
        for item in result["errors"]:
            print(f"- {item}")
    print("Sample documents:")
    for doc in result["sample_documents"]:
        print(json.dumps(doc, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
