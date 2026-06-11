"""AWS Lambda entrypoint for YouTube -> Kafka collection."""

from __future__ import annotations

import json
from typing import Any

from cloud.collector_config import load_collector_runtime_config
from cloud.youtube_api_collector import run_youtube_collection
from config.settings import settings


def _as_csv_or_list(value: Any) -> str | list[str] | tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return str(value)


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None and str(value).strip():
            return value
    return None


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    """Collect YouTube comments, publish them to Kafka, and optionally upload JSONL to S3."""

    event = event or {}
    stored_config, config_error = load_collector_runtime_config()
    runtime_config = {**stored_config, **event}

    event_name = str(runtime_config.get("event_name") or settings.default_event_name).strip()
    search_query = _as_csv_or_list(
        _coalesce(
            runtime_config.get("search_query"),
            runtime_config.get("keywords"),
            settings.default_youtube_search_queries,
        )
    )
    video_ids = _as_csv_or_list(_coalesce(runtime_config.get("video_ids"), settings.default_youtube_video_ids))
    channel_ids = _as_csv_or_list(_coalesce(runtime_config.get("channel_ids"), settings.default_youtube_channel_ids))
    max_videos = int(runtime_config.get("max_videos", settings.youtube_max_videos))
    max_comments_per_video = int(runtime_config.get("max_comments_per_video", settings.youtube_max_comments_per_video))
    order = str(runtime_config.get("order") or settings.default_youtube_order).strip().lower()
    region_code = str(_coalesce(runtime_config.get("region_code"), settings.default_youtube_region_code) or "").strip() or None
    relevance_language = str(
        _coalesce(runtime_config.get("relevance_language"), settings.default_youtube_relevance_language) or ""
    ).strip() or None
    published_after = str(_coalesce(runtime_config.get("published_after"), settings.youtube_published_after) or "").strip() or None
    published_before = str(_coalesce(runtime_config.get("published_before"), settings.youtube_published_before) or "").strip() or None
    engine = str(runtime_config.get("engine") or "youtube_api").strip().lower()

    try:
        result = run_youtube_collection(
            event_name=event_name,
            search_query=search_query or settings.default_youtube_search_queries,
            video_ids=video_ids,
            channel_ids=channel_ids,
            max_videos=max_videos,
            max_comments_per_video=max_comments_per_video,
            order=order,
            region_code=region_code,
            relevance_language=relevance_language,
            published_after=published_after,
            published_before=published_before,
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
        warnings = []
        if config_error:
            warnings.append(config_error)
        response = {
            "status": result.get("status", "error"),
            "config_loaded_from_s3": bool(stored_config),
            "session_id": result.get("session_id"),
            "videos_scanned": result.get("videos_scanned", result.get("posts_scanned", 0)),
            "posts_scanned": result.get("posts_scanned", result.get("videos_scanned", 0)),
            "comments_collected": result.get("comments_collected", 0),
            "messages_published": result.get("messages_published", 0),
            "kafka_topic": result.get("kafka_topic", settings.kafka_raw_topic),
            "s3_backup_key": result.get("s3_backup_key"),
            "collector_state_key": result.get("collector_state_key"),
            "errors": error_list,
        }
        if warnings:
            response["warnings"] = warnings
        if error_list:
            response["message"] = error_list[0]
        if result.get("backup_error"):
            response["backup_error"] = result.get("backup_error")

    return {
        "statusCode": 200 if response.get("status") == "ok" else 500,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response, ensure_ascii=False),
    }
