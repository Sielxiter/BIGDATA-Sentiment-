"""Reddit collector that normalizes posts and comments for Kafka ingestion."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from collectors.base_collector import BaseCollector
from config.settings import (
    DEFAULT_EVENT_NAME,
    DEFAULT_REDDIT_KEYWORDS,
    DEFAULT_REDDIT_LIMIT,
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
)

try:
    import praw
except ImportError:  # pragma: no cover - handled at runtime for clearer messages
    praw = None


class RedditCollector(BaseCollector):
    """Collect Reddit posts and comments for a selected event."""

    def __init__(self, event_name: str, keywords: list[str], limit: int = 20):
        self.event_name = (event_name or DEFAULT_EVENT_NAME).strip() or DEFAULT_EVENT_NAME
        self.keywords = [keyword.strip() for keyword in keywords if keyword and keyword.strip()]
        if not self.keywords:
            self.keywords = [keyword.strip() for keyword in DEFAULT_REDDIT_KEYWORDS.split(",") if keyword.strip()]
        self.limit = max(1, int(limit or DEFAULT_REDDIT_LIMIT))
        self._current_keyword = ""
        self._reddit = self._build_client()
        print(
            f"[RedditCollector] Ready for event={self.event_name!r}, "
            f"keywords={self.keywords}, limit={self.limit}"
        )

    def _build_client(self):
        if praw is None:
            raise RuntimeError(
                "praw is not installed. Install dependencies with `pip install -r requirements.txt`."
            )

        if self._credentials_missing():
            raise RuntimeError(
                "Reddit credentials are missing. Set REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, "
                "and REDDIT_USER_AGENT in .env before running the collector."
            )

        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
        )
        reddit.read_only = True
        return reddit

    @staticmethod
    def _credentials_missing() -> bool:
        def _is_placeholder(value: str) -> bool:
            lowered = (value or "").strip().lower()
            return not lowered or lowered.startswith("your_")

        return _is_placeholder(REDDIT_CLIENT_ID) or _is_placeholder(REDDIT_CLIENT_SECRET)

    @staticmethod
    def _utc_iso_from_timestamp(timestamp: Any) -> str:
        try:
            ts = float(timestamp)
        except (TypeError, ValueError):
            return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _safe_author(value: Any) -> str:
        author = str(value) if value is not None else "anonymous"
        if author.lower() in {"[deleted]", "[removed]", "none", ""}:
            return "anonymous"
        return author

    @staticmethod
    def _clean_text(*parts: str) -> str:
        cleaned_parts = []
        for part in parts:
            if not part:
                continue
            stripped = part.strip()
            if not stripped or stripped.lower() in {"[deleted]", "[removed]"}:
                continue
            cleaned_parts.append(stripped)
        return " ".join(cleaned_parts).strip()

    @staticmethod
    def _detect_language(text: str) -> str | None:
        try:
            from langdetect import detect
        except Exception:
            return None

        try:
            return detect(text)
        except Exception:
            return None

    def collect(self):
        return self.collect_posts()

    def normalize(self, item: dict) -> dict:
        if hasattr(item, "body"):
            return self.normalize_comment(item)
        return self.normalize_post(item)

    def collect_posts(self) -> list[dict]:
        """Search Reddit for each keyword and normalize posts plus comments."""

        collected: list[dict] = []
        seen_ids: set[str] = set()

        for keyword in self.keywords:
            self._current_keyword = keyword
            print(f"[RedditCollector] Searching keyword={keyword!r}")
            try:
                submissions = self._reddit.subreddit("all").search(
                    query=keyword,
                    sort="new",
                    time_filter="month",
                    limit=self.limit,
                )

                for submission in submissions:
                    normalized_post = self.normalize_post(submission)
                    if normalized_post and normalized_post["id"] not in seen_ids:
                        collected.append(normalized_post)
                        seen_ids.add(normalized_post["id"])
                        print(
                            f"[RedditCollector] Collected post id={normalized_post['id']} "
                            f"keyword={keyword!r}"
                        )

                    for normalized_comment in self.collect_comments_from_post(submission, max_comments=10):
                        if normalized_comment["id"] not in seen_ids:
                            collected.append(normalized_comment)
                            seen_ids.add(normalized_comment["id"])
                            print(
                                f"[RedditCollector] Collected comment id={normalized_comment['id']} "
                                f"keyword={keyword!r}"
                            )
            except Exception as exc:
                print(f"[RedditCollector] Error while searching keyword={keyword!r}: {exc}")

        return collected

    def collect_comments_from_post(self, post, max_comments: int = 10) -> list[dict]:
        """Collect a limited number of comments from one Reddit submission."""

        normalized_comments: list[dict] = []

        try:
            post.comments.replace_more(limit=0)
            comments = post.comments.list()
        except Exception as exc:
            print(f"[RedditCollector] Could not load comments for post={getattr(post, 'id', None)}: {exc}")
            return normalized_comments

        for comment in comments:
            if len(normalized_comments) >= max_comments:
                break

            normalized_comment = self.normalize_comment(comment, parent_post=post)
            if normalized_comment:
                normalized_comments.append(normalized_comment)

        return normalized_comments

    def normalize_post(self, post) -> dict | None:
        """Normalize a PRAW submission to the raw Kafka message format."""

        title = getattr(post, "title", "") or ""
        selftext = getattr(post, "selftext", "") or ""
        text = self._clean_text(title, selftext)
        if not text:
            return None

        post_id = getattr(post, "id", None)
        if not post_id:
            return None

        created_at = self._utc_iso_from_timestamp(getattr(post, "created_utc", None))
        subreddit = getattr(getattr(post, "subreddit", None), "display_name", None)
        language = self._detect_language(text)

        return {
            "id": f"reddit_post_{post_id}",
            "event": self.event_name,
            "text": text,
            "source": "reddit",
            "language": language,
            "created_at": created_at,
            "metadata": {
                "author": self._safe_author(getattr(post, "author", None)),
                "url": getattr(post, "url", None),
                "likes": int(getattr(post, "score", 0) or 0),
                "comments_count": int(getattr(post, "num_comments", 0) or 0),
                "subreddit": subreddit,
                "reddit_type": "post",
                "keyword": self._current_keyword,
            },
        }

    def normalize_comment(self, comment, parent_post=None) -> dict | None:
        """Normalize a Reddit comment to the raw Kafka message format."""

        body = getattr(comment, "body", "") or ""
        if body.strip().lower() in {"[deleted]", "[removed]"}:
            return None

        text = self._clean_text(body)
        if not text:
            return None

        comment_id = getattr(comment, "id", None)
        if not comment_id:
            return None

        created_at = self._utc_iso_from_timestamp(getattr(comment, "created_utc", None))
        subreddit = None
        if parent_post is not None:
            subreddit = getattr(getattr(parent_post, "subreddit", None), "display_name", None)
        if subreddit is None:
            subreddit = getattr(getattr(comment, "subreddit", None), "display_name", None)

        language = self._detect_language(text)
        parent_post_id = getattr(parent_post, "id", None) if parent_post is not None else None
        permalink = getattr(comment, "permalink", None)
        url = f"https://www.reddit.com{permalink}" if permalink else None

        return {
            "id": f"reddit_comment_{comment_id}",
            "event": self.event_name,
            "text": text,
            "source": "reddit",
            "language": language,
            "created_at": created_at,
            "metadata": {
                "author": self._safe_author(getattr(comment, "author", None)),
                "url": url,
                "likes": int(getattr(comment, "score", 0) or 0),
                "comments_count": int(getattr(parent_post, "num_comments", 0) or 0) if parent_post is not None else 0,
                "subreddit": subreddit,
                "reddit_type": "comment",
                "keyword": self._current_keyword,
                "parent_post_id": f"reddit_post_{parent_post_id}" if parent_post_id else None,
            },
        }
