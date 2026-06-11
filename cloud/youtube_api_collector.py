"""YouTube Data API collector for public comments.

This module is the active collector implementation. Legacy Reddit imports are
kept as thin wrappers elsewhere so the rest of the repo can migrate without
breaking old entry points.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from html import unescape
from typing import Any
from urllib.parse import urljoin
from uuid import uuid4

import requests

from cloud.kafka_client import KafkaJsonPublisher
from cloud.collector_state import (
    extract_seen_comment_ids,
    load_collector_state,
    merge_seen_comment_ids,
    save_collector_state,
)
from cloud.s3_backup import upload_records_to_s3
from config.settings import settings

try:  # pragma: no cover - optional dependency
    from langdetect import detect, LangDetectException
except Exception:  # pragma: no cover - optional dependency
    detect = None
    LangDetectException = Exception


YOUTUBE_API_BASE_URL = settings.youtube_api_base_url
BLOCKED_STATUS_CODES = {403, 429}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _strip_html_tags(value: str) -> str:
    return _safe_text(re.sub(r"<[^>]+>", " ", unescape(value or "")))


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _coerce_csv_list(value: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _detect_language(text: str | None) -> str | None:
    if not text:
        return None
    if detect is None:
        return None
    try:
        return detect(text)
    except LangDetectException:
        return None
    except Exception:
        return None


def _extract_video_id(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 11 and "/" not in text and "?" not in text:
        return text

    patterns = [
        r"[?&]v=([A-Za-z0-9_-]{11})",
        r"youtu\.be/([A-Za-z0-9_-]{11})",
        r"/shorts/([A-Za-z0-9_-]{11})",
        r"/embed/([A-Za-z0-9_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def _extract_youtube_error_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        return response.text[:300].strip() or f"HTTP {response.status_code}"

    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    if isinstance(error, dict):
        message = _safe_text(error.get("message"))
        errors = error.get("errors")
        reason = None
        if isinstance(errors, list) and errors:
            first = errors[0]
            if isinstance(first, dict):
                reason = _safe_text(first.get("reason"))
        if message and reason:
            return f"{message} (reason={reason})"
        if message:
            return message
        if reason:
            return reason
    return f"HTTP {response.status_code}"


class YouTubeAPICollector:
    """Collect public YouTube comments using the YouTube Data API."""

    def __init__(
        self,
        *,
        engine: str | None = None,
        request_delay_seconds: float | None = None,
        timeout_seconds: float | None = None,
        api_key: str | None = None,
        user_agent: str | None = None,
        region_code: str | None = None,
        relevance_language: str | None = None,
        published_after: str | None = None,
        published_before: str | None = None,
    ) -> None:
        requested_engine = (engine or "youtube_api").strip().lower()
        self.engine = "youtube_api"
        self.requested_engine = requested_engine
        self.request_delay_seconds = max(
            0.0,
            float(
                request_delay_seconds
                if request_delay_seconds is not None
                else settings.youtube_request_delay_seconds
            ),
        )
        self.timeout_seconds = max(
            1.0,
            float(timeout_seconds if timeout_seconds is not None else settings.youtube_api_timeout_seconds),
        )
        self.api_key = (api_key or settings.youtube_api_key).strip()
        self.user_agent = user_agent or settings.youtube_user_agent
        self.region_code = (region_code or settings.default_youtube_region_code).strip() or None
        self.relevance_language = (relevance_language or settings.default_youtube_relevance_language).strip() or None
        self.published_after = (published_after or settings.youtube_published_after).strip() or None
        self.published_before = (published_before or settings.youtube_published_before).strip() or None
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            }
        )

    def _sleep(self) -> None:
        if self.request_delay_seconds > 0:
            time.sleep(self.request_delay_seconds)

    def _request(self, endpoint: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("YOUTUBE_API_KEY is missing.")

        url = urljoin(f"{YOUTUBE_API_BASE_URL}/", endpoint.lstrip("/"))
        request_params = dict(params or {})
        request_params["key"] = self.api_key
        response = self.session.get(url, params=request_params, timeout=self.timeout_seconds)
        if response.status_code in BLOCKED_STATUS_CODES:
            raise RuntimeError(
                f"YouTube API blocked or rate-limited the request ({response.status_code}) for {url}: "
                f"{_extract_youtube_error_message(response)}"
            )
        if response.status_code >= 400:
            raise RuntimeError(
                f"YouTube API request failed ({response.status_code}) for {url}: {_extract_youtube_error_message(response)}"
            )
        self._sleep()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected YouTube API payload for {url}")
        return payload

    def _video_search_params(
        self,
        *,
        query: str,
        channel_id: str | None,
        order: str,
        max_results: int,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "order": order,
            "maxResults": max(1, min(50, int(max_results))),
            "safeSearch": "none",
        }
        if channel_id:
            params["channelId"] = channel_id
        if self.region_code:
            params["regionCode"] = self.region_code
        if self.relevance_language:
            params["relevanceLanguage"] = self.relevance_language
        if self.published_after:
            params["publishedAfter"] = self.published_after
        if self.published_before:
            params["publishedBefore"] = self.published_before
        if page_token:
            params["pageToken"] = page_token
        return params

    def _parse_search_results(
        self,
        payload: dict[str, Any],
        *,
        query: str,
        channel_id: str | None,
        search_url: str,
    ) -> list[dict[str, Any]]:
        videos: list[dict[str, Any]] = []
        for item in payload.get("items", []):
            if not isinstance(item, dict):
                continue
            identifier = item.get("id", {})
            if not isinstance(identifier, dict):
                continue
            video_id = _safe_text(identifier.get("videoId"))
            snippet = item.get("snippet", {})
            if not video_id or not isinstance(snippet, dict):
                continue
            videos.append(
                {
                    "video_id": video_id,
                    "video_title": _safe_text(snippet.get("title")),
                    "video_description": _safe_text(snippet.get("description")),
                    "video_url": f"https://www.youtube.com/watch?v={video_id}",
                    "channel_id": _safe_text(snippet.get("channelId") or channel_id),
                    "channel_title": _safe_text(snippet.get("channelTitle")),
                    "video_published_at": snippet.get("publishedAt"),
                    "keyword": query,
                    "search_url": search_url,
                    "youtube_type": "video_search_result",
                }
            )
        return videos

    def _search_videos(
        self,
        *,
        query: str,
        channel_id: str | None,
        order: str,
        max_videos: int,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        videos: list[dict[str, Any]] = []
        errors: list[str] = []
        seen_ids: set[str] = set()
        page_token: str | None = None

        while len(videos) < max_videos:
            params = self._video_search_params(
                query=query,
                channel_id=channel_id,
                order=order,
                max_results=min(50, max_videos - len(videos)),
                page_token=page_token,
            )
            try:
                payload = self._request("search", params=params)
                candidates = self._parse_search_results(
                    payload,
                    query=query,
                    channel_id=channel_id,
                    search_url=f"{YOUTUBE_API_BASE_URL}/search",
                )
            except Exception as exc:
                errors.append(f"Search failed for query={query!r}, channel_id={channel_id!r}: {exc}")
                break

            if not candidates:
                break

            for candidate in candidates:
                video_id = candidate.get("video_id")
                if not video_id or video_id in seen_ids:
                    continue
                seen_ids.add(str(video_id))
                videos.append(candidate)
                if len(videos) >= max_videos:
                    break

            page_token = payload.get("nextPageToken")
            if not page_token:
                break

        return videos, errors

    def _fetch_videos_by_ids(self, video_ids: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
        videos: list[dict[str, Any]] = []
        errors: list[str] = []
        chunk_size = 50
        for offset in range(0, len(video_ids), chunk_size):
            chunk = video_ids[offset : offset + chunk_size]
            params = {
                "part": "snippet,statistics",
                "id": ",".join(chunk),
                "maxResults": len(chunk),
            }
            try:
                payload = self._request("videos", params=params)
            except Exception as exc:
                errors.append(f"Video lookup failed for ids={chunk!r}: {exc}")
                continue

            for item in payload.get("items", []):
                if not isinstance(item, dict):
                    continue
                video_id = _safe_text(item.get("id"))
                snippet = item.get("snippet", {})
                statistics = item.get("statistics", {})
                if not video_id or not isinstance(snippet, dict):
                    continue
                videos.append(
                    {
                        "video_id": video_id,
                        "video_title": _safe_text(snippet.get("title")),
                        "video_description": _safe_text(snippet.get("description")),
                        "video_url": f"https://www.youtube.com/watch?v={video_id}",
                        "channel_id": _safe_text(snippet.get("channelId")),
                        "channel_title": _safe_text(snippet.get("channelTitle")),
                        "video_published_at": snippet.get("publishedAt"),
                        "video_view_count": _to_int(statistics.get("viewCount"), 0),
                        "video_like_count": _to_int(statistics.get("likeCount"), 0),
                        "video_comment_count": _to_int(statistics.get("commentCount"), 0),
                        "search_url": f"{YOUTUBE_API_BASE_URL}/videos",
                        "youtube_type": "video_lookup",
                    }
                )

        return videos, errors

    def _comments_params(
        self,
        *,
        video_id: str,
        order: str,
        max_results: int,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": max(1, min(100, int(max_results))),
            "order": order,
            "textFormat": "plainText",
        }
        if page_token:
            params["pageToken"] = page_token
        return params

    def _parse_comment_threads(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        for item in payload.get("items", []):
            if not isinstance(item, dict):
                continue
            snippet = item.get("snippet", {})
            if not isinstance(snippet, dict):
                continue
            top_comment = snippet.get("topLevelComment", {})
            if not isinstance(top_comment, dict):
                continue
            top_snippet = top_comment.get("snippet", {})
            if not isinstance(top_snippet, dict):
                continue
            comments.append(
                {
                    "comment_id": _safe_text(top_comment.get("id")),
                    "text": _safe_text(top_snippet.get("textOriginal") or top_snippet.get("textDisplay")),
                    "author": _safe_text(top_snippet.get("authorDisplayName")) or None,
                    "author_channel_id": _safe_text(top_snippet.get("authorChannelId", {}).get("value"))
                    if isinstance(top_snippet.get("authorChannelId"), dict)
                    else None,
                    "created_at": top_snippet.get("publishedAt"),
                    "comment_like_count": _to_int(top_snippet.get("likeCount"), 0),
                    "reply_count": _to_int(snippet.get("totalReplyCount"), 0),
                    "permalink": top_snippet.get("authorChannelUrl"),
                }
            )
        return comments

    def _collect_comments_for_video(
        self,
        *,
        event_name: str,
        query: str,
        video: dict[str, Any],
        max_comments_per_video: int,
        session_id: str,
        order: str,
        exclude_comment_ids: set[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        errors: list[str] = []
        documents: list[dict[str, Any]] = []
        video_id = _safe_text(video.get("video_id"))
        if not video_id:
            return documents, ["Missing video_id in video record."]

        page_token: str | None = None
        seen_comment_ids: set[str] = {str(item).strip() for item in (exclude_comment_ids or set()) if str(item).strip()}
        collected_count = 0
        search_url = video.get("search_url") or f"{YOUTUBE_API_BASE_URL}/search"

        while collected_count < max_comments_per_video:
            params = self._comments_params(
                video_id=video_id,
                order=order,
                max_results=min(100, max_comments_per_video - collected_count),
                page_token=page_token,
            )
            try:
                payload = self._request("commentThreads", params=params)
                candidates = self._parse_comment_threads(payload)
            except Exception as exc:
                errors.append(f"Comments fetch failed for video_id={video_id!r}: {exc}")
                break

            if not candidates:
                break

            for comment in candidates:
                comment_id = comment.get("comment_id")
                if not comment_id or comment_id in seen_comment_ids:
                    continue
                seen_comment_ids.add(str(comment_id))
                normalized = self._normalize_comment(
                    event_name=event_name,
                    query=query,
                    video=video,
                    comment=comment,
                    session_id=session_id,
                    order=order,
                    search_url=search_url,
                )
                if normalized:
                    documents.append(normalized)
                    collected_count += 1
                if collected_count >= max_comments_per_video:
                    break

            page_token = payload.get("nextPageToken")
            if not page_token:
                break

        if not documents:
            errors.append(f"No comments found for video_id={video_id!r}")
        return documents, errors

    def _normalize_comment(
        self,
        *,
        event_name: str,
        query: str,
        video: dict[str, Any],
        comment: dict[str, Any],
        session_id: str,
        order: str,
        search_url: str,
    ) -> dict[str, Any] | None:
        comment_id = _safe_text(comment.get("comment_id"))
        body = _safe_text(comment.get("text"))
        if not comment_id or not body:
            return None

        created_at = _safe_text(comment.get("created_at")) or None
        collected_at = _now_iso()
        language = _detect_language(body)
        author = comment.get("author")
        author_value = None if author in {None, "[deleted]", ""} else str(author)
        video_id = _safe_text(video.get("video_id"))
        video_title = _safe_text(video.get("video_title"))
        channel_id = _safe_text(video.get("channel_id")) or None
        channel_title = _safe_text(video.get("channel_title")) or None
        video_url = _safe_text(video.get("video_url")) or f"https://www.youtube.com/watch?v={video_id}"
        comment_like_count = _to_int(comment.get("comment_like_count"), 0)
        reply_count = _to_int(comment.get("reply_count"), 0)
        video_view_count = _to_int(video.get("video_view_count"), 0)
        video_like_count = _to_int(video.get("video_like_count"), 0)
        video_comment_count = _to_int(video.get("video_comment_count"), 0)
        comment_permalink = f"{video_url}&lc={comment_id}" if video_url else None

        metadata = {
            "author": author_value,
            "url": video_url,
            "likes": comment_like_count,
            "score": comment_like_count,
            "comments_count": video_comment_count,
            "comments_count_estimated": video_comment_count,
            "video_id": video_id,
            "video_title": video_title,
            "youtube_type": "comment",
            "keyword": query,
            "subreddit": None,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "search_query": query,
            "post_id": video_id,
            "post_title": video_title,
            "post_url": video_url,
            "comment_id": comment_id,
            "scraper": self.engine,
            "scraper_requested_engine": self.requested_engine,
            "sort_mode": order,
            "time_filter": None,
            "search_url": search_url,
            "post_score": video_view_count,
            "post_created_at": video.get("video_published_at"),
            "comment_score": comment_like_count,
            "permalink": comment_permalink,
            "parent_id": None,
            "session_id": session_id,
            "video_view_count": video_view_count,
            "video_like_count": video_like_count,
            "reply_count": reply_count,
        }

        return {
            "id": f"youtube_comment_{comment_id}",
            "event": event_name,
            "event_name": event_name,
            "source": "youtube",
            "keyword": query,
            "search_query": query,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "video_id": video_id,
            "video_title": video_title,
            "post_id": video_id,
            "post_title": video_title,
            "post_url": video_url,
            "comment_id": comment_id,
            "text": body,
            "author": author_value,
            "subreddit": None,
            "created_at": created_at,
            "collected_at": collected_at,
            "language": language,
            "likes": comment_like_count,
            "metadata": metadata,
        }

    def collect(
        self,
        *,
        event_name: str,
        search_query: str | list[str] | tuple[str, ...],
        video_ids: str | list[str] | tuple[str, ...] | None = None,
        channel_ids: str | list[str] | tuple[str, ...] | None = None,
        max_videos: int = 20,
        max_comments_per_video: int = 50,
        order: str = "relevance",
        region_code: str | None = None,
        relevance_language: str | None = None,
        published_after: str | None = None,
        published_before: str | None = None,
        exclude_comment_ids: set[str] | list[str] | tuple[str, ...] | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """Collect and normalize public YouTube comments."""

        session_id = session_id or uuid4().hex
        query_list = _coerce_csv_list(search_query) or [event_name]
        channel_list = _coerce_csv_list(channel_ids)
        video_id_list = [_extract_video_id(item) or item for item in _coerce_csv_list(video_ids)]
        video_id_list = [str(item).strip() for item in video_id_list if str(item).strip()]

        original_region = self.region_code
        original_language = self.relevance_language
        original_published_after = self.published_after
        original_published_before = self.published_before
        if region_code is not None:
            self.region_code = region_code.strip() or None
        if relevance_language is not None:
            self.relevance_language = relevance_language.strip() or None
        if published_after is not None:
            self.published_after = published_after.strip() or None
        if published_before is not None:
            self.published_before = published_before.strip() or None

        all_documents: list[dict[str, Any]] = []
        video_errors: list[str] = []
        comment_errors: list[str] = []
        seen_videos: set[str] = set()
        seen_comments: set[str] = {str(item).strip() for item in (exclude_comment_ids or set()) if str(item).strip()}
        videos_scanned = 0
        engine_warning = None

        if self.requested_engine != "youtube_api":
            engine_warning = (
                f"Requested scraper engine {self.requested_engine!r} is not bundled in this build; "
                "using the YouTube API collector."
            )

        try:
            if video_id_list:
                videos, errors = self._fetch_videos_by_ids(video_id_list[:max_videos])
                video_errors.extend(errors)
                videos_scanned = len(videos)
            else:
                videos = []
                search_targets = channel_list or [None]
                for query in query_list:
                    if videos_scanned >= max_videos:
                        break
                    for channel_id in search_targets:
                        if videos_scanned >= max_videos:
                            break
                        remaining = max_videos - videos_scanned
                        candidates, errors = self._search_videos(
                            query=query,
                            channel_id=channel_id,
                            order=order,
                            max_videos=remaining,
                        )
                        video_errors.extend(errors)
                        for candidate in candidates:
                            video_id = str(candidate.get("video_id") or "")
                            if not video_id or video_id in seen_videos:
                                continue
                            seen_videos.add(video_id)
                            videos.append(candidate)
                            videos_scanned += 1
                            if videos_scanned >= max_videos:
                                break
        except Exception as exc:
            video_errors.append(str(exc))
            videos = []

        for video in videos[:max_videos]:
            video_id = str(video.get("video_id") or "")
            if video_id:
                seen_videos.add(video_id)
            remaining_comments = max_comments_per_video
            documents, errors = self._collect_comments_for_video(
                event_name=event_name,
                query=str(video.get("keyword") or query_list[0]),
                video=video,
                max_comments_per_video=remaining_comments,
                session_id=session_id,
                order=order,
                exclude_comment_ids=seen_comments,
            )
            if errors:
                comment_errors.extend(errors)

            for document in documents:
                comment_key = document.get("comment_id")
                if not comment_key or comment_key in seen_comments:
                    continue
                seen_comments.add(str(comment_key))
                all_documents.append(document)

        warnings: list[str] = []
        if video_errors:
            warnings.extend(video_errors)
        if comment_errors:
            warnings.extend(comment_errors)
        if engine_warning:
            warnings.append(engine_warning)

        if not all_documents and not warnings:
            warnings.append("No YouTube comments collected.")

        status = "ok" if all_documents else "error"
        if not all_documents and any("blocked or rate-limited" in warning.lower() for warning in warnings):
            status = "blocked"

        self.region_code = original_region
        self.relevance_language = original_language
        self.published_after = original_published_after
        self.published_before = original_published_before

        return {
            "status": status,
            "session_id": session_id,
            "videos_scanned": videos_scanned,
            "posts_scanned": videos_scanned,
            "comments_collected": len(all_documents),
            "documents": all_documents,
            "warnings": warnings,
        }


def run_youtube_collection(
    *,
    event_name: str,
    search_query: str | list[str] | tuple[str, ...],
    video_ids: str | list[str] | tuple[str, ...] | None = None,
    channel_ids: str | list[str] | tuple[str, ...] | None = None,
    max_videos: int = 20,
    max_comments_per_video: int = 50,
    order: str = "relevance",
    region_code: str | None = None,
    relevance_language: str | None = None,
    published_after: str | None = None,
    published_before: str | None = None,
    engine: str | None = None,
    publish_to_kafka: bool = True,
    backup_to_s3: bool | None = None,
    include_documents: bool = True,
    session_id: str | None = None,
    kafka_topic: str | None = None,
    kafka_bootstrap_servers: str | None = None,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
    aws_region: str | None = None,
) -> dict[str, Any]:
    """Collect YouTube comments, optionally publish them to Kafka, and optionally back them up to S3."""

    collector = YouTubeAPICollector(engine=engine, region_code=region_code, relevance_language=relevance_language, published_after=published_after, published_before=published_before)
    collector_state, collector_state_error = load_collector_state(
        bucket=settings.collector_state_bucket_name,
        key=settings.collector_state_s3_key,
        region=settings.aws_region,
    )
    exclude_comment_ids = extract_seen_comment_ids(collector_state, event_name)
    collection = collector.collect(
        event_name=event_name,
        search_query=search_query,
        video_ids=video_ids,
        channel_ids=channel_ids,
        max_videos=max_videos,
        max_comments_per_video=max_comments_per_video,
        order=order,
        region_code=region_code,
        relevance_language=relevance_language,
        published_after=published_after,
        published_before=published_before,
        exclude_comment_ids=exclude_comment_ids,
        session_id=session_id,
    )

    documents = collection.get("documents", [])
    if not include_documents:
        documents = []

    kafka_topic = (kafka_topic or settings.kafka_raw_topic).strip() or settings.kafka_raw_topic
    kafka_bootstrap_servers = (kafka_bootstrap_servers or settings.kafka_bootstrap_servers).strip()
    messages_published = 0
    kafka_error = None
    if publish_to_kafka and collection.get("documents"):
        publisher = None
        try:
            publisher = KafkaJsonPublisher(topic=kafka_topic, bootstrap_servers=kafka_bootstrap_servers)
            messages_published = publisher.publish_batch(collection.get("documents", []))
        except Exception as exc:
            kafka_error = str(exc)
        finally:
            if publisher is not None:
                try:
                    publisher.close()
                except Exception as exc:
                    kafka_error = kafka_error or str(exc)

    s3_backup_key = None
    backup_error = None
    should_backup = settings.enable_s3_backup if backup_to_s3 is None else bool(backup_to_s3)
    if should_backup and collection.get("documents"):
        try:
            s3_backup_key, backup_error = upload_records_to_s3(
                records=collection.get("documents", []),
                event_name=event_name,
                session_id=collection["session_id"],
                bucket=(s3_bucket or settings.s3_bucket_name).strip(),
                prefix=(s3_prefix or settings.s3_prefix).strip(),
                region=(aws_region or settings.aws_region).strip(),
            )
        except Exception as exc:  # pragma: no cover - upload helper already catches common errors
            s3_backup_key = None
            backup_error = str(exc)

    collector_state_key = None
    collector_state_save_error = None
    if collection.get("documents") and publish_to_kafka and kafka_error is None:
        new_comment_ids = [str(document.get("comment_id") or "").strip() for document in collection.get("documents", [])]
        collector_state = merge_seen_comment_ids(
            collector_state,
            event_name=event_name,
            new_comment_ids=new_comment_ids,
            max_ids_per_event=settings.collector_state_max_comment_ids,
        )
        collector_state_key, collector_state_save_error = save_collector_state(
            collector_state,
            bucket=settings.collector_state_bucket_name,
            key=settings.collector_state_s3_key,
            region=settings.aws_region,
        )

    errors = list(collection.get("warnings", []))
    if collector_state_error:
        errors.append(f"Collector state load issue: {collector_state_error}")
    if collector_state_save_error:
        errors.append(f"Collector state save issue: {collector_state_save_error}")
    if kafka_error:
        errors.append(f"Kafka publish failed: {kafka_error}")
    if backup_error and "missing" not in backup_error.lower():
        errors.append(f"S3 backup failed: {backup_error}")

    status = collection["status"]
    if kafka_error or (should_backup and backup_error and "missing" not in backup_error.lower()):
        status = "error"

    result = {
        "status": status,
        "session_id": collection["session_id"],
        "videos_scanned": collection["videos_scanned"],
        "posts_scanned": collection["posts_scanned"],
        "comments_collected": collection["comments_collected"],
        "messages_published": messages_published,
        "kafka_topic": kafka_topic,
        "kafka_bootstrap_servers": kafka_bootstrap_servers,
        "s3_backup_key": s3_backup_key,
        "collector_state_key": collector_state_key,
        "backup_error": backup_error,
        "errors": errors,
    }
    if include_documents:
        result["documents"] = documents
    return result
