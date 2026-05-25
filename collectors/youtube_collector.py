"""YouTube collector for event-related comments."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from collectors.base_collector import BaseCollector
from config.settings import (
    DEFAULT_EVENT_NAME,
    DEFAULT_YOUTUBE_KEYWORDS,
    DEFAULT_YOUTUBE_MAX_COMMENTS,
    DEFAULT_YOUTUBE_MAX_VIDEOS,
    YOUTUBE_API_KEY,
)

try:
    from googleapiclient.discovery import build
except ImportError:  # pragma: no cover - handled at runtime with a clear message
    build = None


class YouTubeCollector(BaseCollector):
    """Collect comments from YouTube videos related to an event."""

    def __init__(self, event_name: str, keywords: list[str], max_videos: int = 5, max_comments: int = 50):
        self.event_name = (event_name or DEFAULT_EVENT_NAME).strip() or DEFAULT_EVENT_NAME
        self.keywords = [keyword.strip() for keyword in keywords if keyword and keyword.strip()]
        if not self.keywords:
            self.keywords = [keyword.strip() for keyword in DEFAULT_YOUTUBE_KEYWORDS.split(",") if keyword.strip()]
        self.max_videos = max(1, int(max_videos or DEFAULT_YOUTUBE_MAX_VIDEOS))
        self.max_comments = max(1, int(max_comments or DEFAULT_YOUTUBE_MAX_COMMENTS))
        self._youtube = self._build_client()
        self._seen_ids: set[str] = set()
        self._current_keyword = ""
        self._current_video_id = ""
        self._current_video_title = ""
        self._current_comments_count = 0
        print(
            f"[YouTubeCollector] Ready for event={self.event_name!r}, "
            f"keywords={self.keywords}, max_videos={self.max_videos}, max_comments={self.max_comments}"
        )

    @staticmethod
    def _is_placeholder(value: str) -> bool:
        normalized = (value or "").strip().lower()
        return not normalized or normalized.startswith("your_")

    def _build_client(self):
        if self._is_placeholder(YOUTUBE_API_KEY):
            raise RuntimeError(
                "YouTube API key is missing. Set YOUTUBE_API_KEY in .env before running the collector."
            )

        if build is None:
            raise RuntimeError(
                "google-api-python-client is not installed. Install dependencies with "
                "`pip install -r requirements.txt`."
            )

        return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

    @staticmethod
    def _utc_iso(value: Any) -> str:
        if isinstance(value, str) and value.strip():
            return value.strip()
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _safe_author(value: Any) -> str:
        author = str(value) if value is not None else "anonymous"
        if author.lower() in {"[deleted]", "[removed]", "none", ""}:
            return "anonymous"
        return author

    @staticmethod
    def _clean_text(value: Any) -> str:
        text = str(value or "").strip()
        return " ".join(text.split())

    def normalize(self, item: dict) -> dict:
        return item

    def search_videos(self, keyword: str) -> list[dict]:
        """Search videos for a keyword and return a small normalized list."""

        print(f"[YouTubeCollector] Searching videos for keyword={keyword!r}")
        videos: list[dict] = []

        try:
            response = (
                self._youtube.search()
                .list(
                    part="snippet",
                    q=keyword,
                    type="video",
                    maxResults=self.max_videos,
                    safeSearch="none",
                    order="relevance",
                )
                .execute()
            )
        except Exception as exc:
            print(f"[YouTubeCollector] Search failed for keyword={keyword!r}: {exc}")
            return videos

        for item in response.get("items", []):
            video_id = item.get("id", {}).get("videoId")
            snippet = item.get("snippet", {})
            if not video_id:
                continue

            videos.append(
                {
                    "video_id": video_id,
                    "video_title": snippet.get("title", ""),
                    "published_at": snippet.get("publishedAt"),
                }
            )

        return videos

    def _get_video_comment_count(self, video_id: str) -> int:
        try:
            response = (
                self._youtube.videos()
                .list(part="statistics,snippet", id=video_id)
                .execute()
            )
        except Exception as exc:
            print(f"[YouTubeCollector] Could not load statistics for video_id={video_id!r}: {exc}")
            return 0

        items = response.get("items", [])
        if not items:
            return 0

        statistics = items[0].get("statistics", {})
        try:
            return int(statistics.get("commentCount", 0) or 0)
        except (TypeError, ValueError):
            return 0

    def collect_comments_from_video(self, video_id: str, video_title: str, keyword: str) -> list[dict]:
        """Collect comments from a single YouTube video."""

        self._current_keyword = keyword
        self._current_video_id = video_id
        self._current_video_title = video_title
        self._current_comments_count = self._get_video_comment_count(video_id)

        collected: list[dict] = []
        next_page_token: str | None = None

        while len(collected) < self.max_comments:
            remaining = self.max_comments - len(collected)
            page_size = min(100, remaining)

            try:
                response = (
                    self._youtube.commentThreads()
                    .list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=page_size,
                        textFormat="plainText",
                        order="relevance",
                        pageToken=next_page_token,
                    )
                    .execute()
                )
            except Exception as exc:
                print(f"[YouTubeCollector] Could not load comments for video_id={video_id!r}: {exc}")
                break

            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                top_comment = snippet.get("topLevelComment", {})
                normalized = self.normalize_comment(
                    top_comment,
                    video_id=video_id,
                    video_title=video_title,
                    keyword=keyword,
                )
                if normalized and normalized["id"] not in self._seen_ids:
                    collected.append(normalized)
                    self._seen_ids.add(normalized["id"])

                if len(collected) >= self.max_comments:
                    break

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return collected

    def collect(self) -> list[dict]:
        """Collect normalized comments for all configured keywords."""

        all_comments: list[dict] = []
        seen_videos: set[str] = set()

        for keyword in self.keywords:
            self._current_keyword = keyword
            for video in self.search_videos(keyword):
                video_id = video.get("video_id", "")
                if not video_id or video_id in seen_videos:
                    continue
                seen_videos.add(video_id)

                video_title = video.get("video_title", "")
                comments = self.collect_comments_from_video(video_id, video_title, keyword)
                all_comments.extend(comments)
                print(
                    f"[YouTubeCollector] Collected {len(comments)} comment(s) from video_id={video_id!r}"
                )

        return all_comments

    def normalize_comment(self, comment, video_id: str, video_title: str, keyword: str) -> dict | None:
        """Normalize a YouTube comment to the raw Kafka message format."""

        snippet = comment.get("snippet", {}) if isinstance(comment, dict) else {}
        comment_id = comment.get("id") if isinstance(comment, dict) else None
        text = self._clean_text(snippet.get("textOriginal") or snippet.get("textDisplay"))

        if not comment_id or not text:
            return None

        author = self._safe_author(snippet.get("authorDisplayName"))
        created_at = self._utc_iso(snippet.get("publishedAt"))
        likes = snippet.get("likeCount", 0)
        url = f"https://www.youtube.com/watch?v={video_id}"

        try:
            likes_value = int(likes or 0)
        except (TypeError, ValueError):
            likes_value = 0

        return {
            "id": f"youtube_comment_{comment_id}",
            "event": self.event_name,
            "text": text,
            "source": "youtube",
            "language": "unknown",
            "created_at": created_at,
            "metadata": {
                "author": author,
                "url": url,
                "likes": likes_value,
                "comments_count": self._current_comments_count,
                "video_id": video_id,
                "video_title": video_title,
                "youtube_type": "comment",
                "keyword": keyword,
            },
        }
