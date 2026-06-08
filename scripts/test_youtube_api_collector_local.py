"""Local YouTube API collector test script.

This script is meant for workstation or EC2 validation before Lambda deployment.
It uses the same collector helper as the dashboard and Lambda entrypoint.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from cloud.youtube_api_collector import run_youtube_collection  # noqa: E402
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
    parser = argparse.ArgumentParser(description="Test the YouTube API collector locally.")
    parser.add_argument("--event", default=settings.default_event_name, help="Event name to associate with the scrape.")
    parser.add_argument(
        "--search-query",
        "--keywords",
        dest="search_query",
        default=settings.default_youtube_search_queries,
        help="Comma-separated search query list.",
    )
    parser.add_argument(
        "--video-ids",
        default=settings.default_youtube_video_ids,
        help="Comma-separated YouTube video IDs.",
    )
    parser.add_argument(
        "--channel-ids",
        "--subreddits",
        dest="channel_ids",
        default=settings.default_youtube_channel_ids,
        help="Comma-separated YouTube channel IDs.",
    )
    parser.add_argument("--max-videos", "--max-posts", dest="max_videos", type=int, default=settings.youtube_max_videos, help="Maximum number of videos to scan.")
    parser.add_argument(
        "--max-comments",
        "--max-comments-per-video",
        dest="max_comments_per_video",
        type=int,
        default=settings.youtube_max_comments_per_video,
        help="Maximum number of comments to collect per video.",
    )
    parser.add_argument(
        "--send-to-kafka",
        default="false",
        help="Send scraped documents to Kafka (true/false).",
    )
    parser.add_argument("--output-jsonl", default="", help="Optional JSONL output path.")
    parser.add_argument("--order", default=settings.default_youtube_order, choices=["relevance", "date", "viewCount", "rating"])
    parser.add_argument("--region-code", default=settings.default_youtube_region_code, help="Optional YouTube region code.")
    parser.add_argument(
        "--relevance-language",
        default=settings.default_youtube_relevance_language,
        help="Optional YouTube relevance language.",
    )
    parser.add_argument("--published-after", default=settings.youtube_published_after, help="Optional ISO 8601 lower bound.")
    parser.add_argument("--published-before", default=settings.youtube_published_before, help="Optional ISO 8601 upper bound.")
    parser.add_argument("--engine", default="youtube_api", help="Collector engine label for metadata.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    result = run_youtube_collection(
        event_name=args.event,
        search_query=args.search_query,
        video_ids=args.video_ids,
        channel_ids=args.channel_ids,
        max_videos=int(args.max_videos),
        max_comments_per_video=int(args.max_comments_per_video),
        order=str(args.order),
        region_code=str(args.region_code) or None,
        relevance_language=str(args.relevance_language) or None,
        published_after=str(args.published_after) or None,
        published_before=str(args.published_before) or None,
        engine=str(args.engine),
        publish_to_kafka=_parse_bool(args.send_to_kafka),
        backup_to_s3=False,
        include_documents=True,
        kafka_topic=settings.kafka_raw_topic,
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    documents = result.get("documents", [])
    print(f"Status: {result.get('status')}")
    print(f"Videos scanned: {result.get('videos_scanned', result.get('posts_scanned', 0))}")
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
