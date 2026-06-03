"""Example AWS Lambda collector: YouTube -> S3 JSONL backup.

This is an example integration file. It does not deploy infrastructure by itself.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from googleapiclient.discovery import build


def _bool_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() == "true"


def _safe_text(value: str) -> str:
    return " ".join((value or "").split())


def _normalize_comment(event_name: str, keyword: str, video_id: str, video_title: str, raw_item: dict) -> dict | None:
    snippet = raw_item.get("snippet", {})
    comment_id = raw_item.get("id")
    text = _safe_text(snippet.get("textOriginal") or snippet.get("textDisplay") or "")
    if not comment_id or not text:
        return None

    created_at = snippet.get("publishedAt") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    likes = snippet.get("likeCount", 0)
    try:
        likes = int(likes or 0)
    except Exception:
        likes = 0

    return {
        "id": f"youtube_comment_{comment_id}",
        "event": event_name,
        "text": text,
        "source": "youtube",
        "language": "unknown",
        "created_at": created_at,
        "metadata": {
            "author": snippet.get("authorDisplayName", "anonymous"),
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "likes": likes,
            "comments_count": 0,
            "video_id": video_id,
            "video_title": video_title,
            "youtube_type": "comment",
            "keyword": keyword,
        },
    }


def _upload_jsonl(s3_client, bucket: str, key: str, records: list[dict]) -> bool:
    body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        return True
    except (ClientError, BotoCoreError) as exc:
        print(f"[lambda] S3 upload failed: {exc}")
        return False


def lambda_handler(event, context):
    # Event payload
    mode = str(event.get("mode", "limited")).strip().lower()
    event_name = str(event.get("event_name", "Morocco football")).strip()
    keywords_raw = event.get("keywords", "Morocco football,Morocco match,CAN Morocco")
    if isinstance(keywords_raw, list):
        keywords = [str(value).strip() for value in keywords_raw if str(value).strip()]
    else:
        keywords = [value.strip() for value in str(keywords_raw).split(",") if value.strip()]

    max_videos = int(event.get("max_videos", 5))
    max_comments = int(event.get("max_comments", 50))
    duration_minutes = int(event.get("duration_minutes", 10))
    poll_interval_seconds = int(event.get("poll_interval_seconds", 5))

    # Environment
    youtube_api_key = os.getenv("YOUTUBE_API_KEY", "")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket = os.getenv("S3_BUCKET_NAME", "")
    s3_prefix = os.getenv("S3_PREFIX", "raw/youtube").strip("/")
    enable_s3_backup = _bool_env("ENABLE_S3_BACKUP", "false")
    if not youtube_api_key:
        return {"status": "error", "message": "YOUTUBE_API_KEY is missing."}
    if not keywords:
        return {"status": "error", "message": "No keywords provided."}

    youtube = build("youtube", "v3", developerKey=youtube_api_key, cache_discovery=False)
    s3_client = boto3.client("s3", region_name=aws_region) if enable_s3_backup and s3_bucket else None

    session_id = uuid4().hex
    date_value = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    safe_event = event_name.replace(" ", "_")
    s3_key = (
        f"{s3_prefix}/event={safe_event}/date={date_value}/session_id={session_id}/comments.jsonl"
        if s3_client
        else None
    )

    comments_collected = 0
    buffered_for_s3: list[dict] = []
    seen_ids: set[str] = set()

    stop_at = datetime.now(timezone.utc) + timedelta(minutes=max(1, duration_minutes))
    run_timed = mode == "timed_continuous"

    while True:
        for keyword in keywords:
            search_response = (
                youtube.search()
                .list(part="snippet", q=keyword, type="video", maxResults=max_videos, safeSearch="none", order="relevance")
                .execute()
            )
            for video in search_response.get("items", []):
                video_id = video.get("id", {}).get("videoId")
                video_title = video.get("snippet", {}).get("title", "")
                if not video_id:
                    continue

                comments_response = (
                    youtube.commentThreads()
                    .list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=min(100, max_comments),
                        textFormat="plainText",
                        order="relevance",
                    )
                    .execute()
                )
                for item in comments_response.get("items", []):
                    top = item.get("snippet", {}).get("topLevelComment", {})
                    normalized = _normalize_comment(
                        event_name=event_name,
                        keyword=keyword,
                        video_id=video_id,
                        video_title=video_title,
                        raw_item=top,
                    )
                    if not normalized:
                        continue
                    if normalized["id"] in seen_ids:
                        continue
                    seen_ids.add(normalized["id"])

                    if s3_client is not None:
                        buffered_for_s3.append(normalized)
                        if len(buffered_for_s3) >= 50 and s3_key:
                            _upload_jsonl(s3_client, s3_bucket, s3_key, buffered_for_s3)
                            buffered_for_s3.clear()

                    comments_collected += 1

        if not run_timed:
            break
        if datetime.now(timezone.utc) >= stop_at:
            break
        time.sleep(max(1, poll_interval_seconds))

    if buffered_for_s3 and s3_client is not None and s3_key:
        _upload_jsonl(s3_client, s3_bucket, s3_key, buffered_for_s3)

    return {
        "status": "ok",
        "session_id": session_id,
        "mode": mode,
        "comments_collected": comments_collected,
        "s3_key": s3_key,
    }
