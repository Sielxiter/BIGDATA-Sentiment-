"""AWS Lambda entrypoint for Reddit -> Kafka collection."""

from __future__ import annotations

import json
from typing import Any

from cloud.reddit_scraper_collector import run_reddit_collection
from config.settings import settings


def _as_csv_or_list(value: Any) -> str | list[str] | tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return str(value)


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    """Collect Reddit comments, publish them to Kafka, and optionally upload JSONL to S3."""

    event = event or {}
    event_name = str(event.get("event_name") or settings.default_event_name).strip()
    keywords = _as_csv_or_list(event.get("keywords") or settings.default_reddit_keywords)
    subreddits = _as_csv_or_list(event.get("subreddits") or settings.default_reddit_subreddits)
    max_posts = int(event.get("max_posts", settings.reddit_max_posts))
    max_comments_per_post = int(event.get("max_comments_per_post", settings.reddit_max_comments_per_post))
    sort_mode = str(event.get("sort_mode") or settings.default_reddit_sort_mode).strip().lower()
    time_filter = str(event.get("time_filter") or settings.default_reddit_time_filter).strip() or None
    engine = str(event.get("engine") or settings.reddit_scraper_engine).strip().lower()

    try:
        result = run_reddit_collection(
            event_name=event_name,
            keywords=keywords or settings.default_reddit_keywords,
            subreddits=subreddits or settings.default_reddit_subreddits,
            max_posts=max_posts,
            max_comments_per_post=max_comments_per_post,
            sort_mode=sort_mode,
            time_filter=time_filter,
            engine=engine,
            publish_to_kafka=True,
            backup_to_s3=settings.enable_s3_backup,
            include_documents=False,
            kafka_topic=settings.kafka_raw_topic,
            kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
            s3_bucket=settings.s3_bucket_name,
            s3_prefix=settings.s3_prefix,
            aws_region=settings.aws_region,
        )
    except Exception as exc:
        response = {
            "status": "error",
            "message": f"Unhandled Lambda error: {exc}",
            "details": {
                "error_type": type(exc).__name__,
            },
        }
    else:
        error_list = result.get("errors", [])
        response = {
            "status": result.get("status", "error"),
            "session_id": result.get("session_id"),
            "posts_scanned": result.get("posts_scanned", 0),
            "comments_collected": result.get("comments_collected", 0),
            "messages_published": result.get("messages_published", 0),
            "kafka_topic": result.get("kafka_topic", settings.kafka_raw_topic),
            "s3_backup_key": result.get("s3_backup_key"),
            "errors": error_list,
        }
        if error_list:
            response["message"] = error_list[0]
        if result.get("backup_error"):
            response["backup_error"] = result.get("backup_error")

    return {
        "statusCode": 200 if response.get("status") == "ok" else 500,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response, ensure_ascii=False),
    }
