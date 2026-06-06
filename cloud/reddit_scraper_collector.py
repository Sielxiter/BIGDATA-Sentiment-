"""
Reddit scraping collector for public posts and comments.
This implementation uses requests and Reddit's public JSON endpoints because it
is the simplest option that works locally and inside Lambda-compatible
runtimes. Selenium or Playwright can be added later if Reddit changes its page
structure, but browser automation is intentionally not required for the
current build.
"""

from __future__ import annotations

import json
import re
import time
from html import unescape
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse
from uuid import uuid4

import requests

from cloud.kafka_client import KafkaJsonPublisher
from cloud.s3_backup import upload_records_to_s3
from config.settings import settings

try:  # pragma: no cover - optional dependency
    from langdetect import detect, LangDetectException
except Exception:  # pragma: no cover - optional dependency
    detect = None
    LangDetectException = Exception


REDDIT_BASE_URL = "https://www.reddit.com"
OLD_REDDIT_BASE_URL = "https://old.reddit.com"


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


def _extract_post_id(url: str | None, fallback: str | None = None) -> str | None:
    if not url:
        return fallback
    match = re.search(r"/comments/([a-z0-9]+)/", url)
    if match:
        return match.group(1)
    parsed = urlparse(url)
    slug = parsed.path.strip("/").split("/")
    if "comments" in slug:
        index = slug.index("comments")
        if index + 1 < len(slug):
            return slug[index + 1]
    return fallback


class RedditScraperCollector:
    """Collect public Reddit posts and comments without using Reddit API credentials."""

    def __init__(
        self,
        *,
        engine: str | None = None,
        request_delay_seconds: float | None = None,
        timeout_seconds: float | None = None,
        user_agent: str | None = None,
        headless: bool | None = None,
    ) -> None:
        requested_engine = (engine or settings.reddit_scraper_engine or "requests").strip().lower()
        self.engine = requested_engine if requested_engine == "requests" else "requests"
        self.requested_engine = requested_engine
        self.request_delay_seconds = max(0.0, float(request_delay_seconds or settings.reddit_request_delay_seconds))
        self.timeout_seconds = max(1.0, float(timeout_seconds or settings.reddit_timeout_seconds))
        self.user_agent = user_agent or settings.reddit_user_agent
        self.headless = settings.reddit_headless if headless is None else bool(headless)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8",
            }
        )

    def _sleep(self) -> None:
        if self.request_delay_seconds > 0:
            time.sleep(self.request_delay_seconds)

    def _request(self, url: str, *, params: dict[str, Any] | None = None, as_json: bool = False) -> Any:
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        if response.status_code in {429, 403}:
            raise RuntimeError(f"Reddit blocked or rate-limited the request ({response.status_code}) for {url}")
        response.raise_for_status()
        self._sleep()
        if as_json:
            return response.json()
        return response.text

    def _search_urls(self, keyword: str, subreddit: str | None, sort_mode: str, time_filter: str | None) -> list[tuple[str, str]]:
        sort_mode = (sort_mode or "relevance").strip().lower()
        time_filter = (time_filter or "").strip().lower()

        urls: list[tuple[str, str]] = []
        if subreddit:
            path = f"/r/{subreddit}/search/"
            params = {
                "q": keyword,
                "restrict_sr": "on",
                "sort": sort_mode,
            }
            if time_filter:
                params["t"] = time_filter
            urls.append((urljoin(OLD_REDDIT_BASE_URL, path), json.dumps(params)))
            urls.append((f"{REDDIT_BASE_URL}/r/{subreddit}/search.json", json.dumps({**params, "limit": 25})))
        else:
            params = {"q": keyword, "sort": sort_mode}
            if time_filter:
                params["t"] = time_filter
            urls.append((f"{OLD_REDDIT_BASE_URL}/search/", json.dumps(params)))
            urls.append((f"{REDDIT_BASE_URL}/search.json", json.dumps({**params, "limit": 25})))

        return urls

    def _parse_old_reddit_search(self, html: str, keyword: str, subreddit: str | None) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        pattern = re.compile(
            r'<a[^>]+class="[^"]*search-title[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
            re.IGNORECASE | re.DOTALL,
        )
        for match in pattern.finditer(html):
            post_url = match.group(1)
            title = _strip_html_tags(match.group(2))
            if not post_url or not title:
                continue
            subreddit_match = re.search(r"/r/([^/]+)/comments/", post_url)
            detected_subreddit = subreddit_match.group(1) if subreddit_match else subreddit

            post_id = _extract_post_id(post_url)
            posts.append(
                {
                    "post_id": post_id,
                    "post_title": title,
                    "post_url": post_url if post_url.startswith("http") else urljoin(REDDIT_BASE_URL, post_url),
                    "subreddit": detected_subreddit,
                    "keyword": keyword,
                    "comments_hint": None,
                    "score_hint": None,
                    "search_url": f"{OLD_REDDIT_BASE_URL}/search/",
                }
            )
        return posts

    def _parse_json_search(self, payload: dict[str, Any], keyword: str, subreddit: str | None, search_url: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        for child in payload.get("data", {}).get("children", []):
            if not isinstance(child, dict):
                continue
            data = child.get("data", {})
            if not isinstance(data, dict):
                continue
            post_url = data.get("permalink") or data.get("url")
            if not post_url:
                continue
            if not str(post_url).startswith("http"):
                post_url = urljoin(REDDIT_BASE_URL, str(post_url))
            post_id = data.get("id") or _extract_post_id(str(post_url))
            posts.append(
                {
                    "post_id": post_id,
                    "post_title": _safe_text(data.get("title")),
                    "post_url": str(post_url),
                    "subreddit": _safe_text(data.get("subreddit") or subreddit),
                    "keyword": keyword,
                    "comments_hint": data.get("num_comments"),
                    "score_hint": data.get("score"),
                    "search_url": search_url,
                    "post_created_at": data.get("created_utc"),
                    "post_author": data.get("author"),
                }
            )
        return posts

    def search_posts(
        self,
        *,
        keyword: str,
        subreddits: list[str],
        sort_mode: str,
        time_filter: str | None,
        max_posts: int,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        posts: list[dict[str, Any]] = []
        errors: list[str] = []
        seen_post_ids: set[str] = set()

        search_targets = subreddits or [None]
        for subreddit in search_targets:
            if len(posts) >= max_posts:
                break

            for search_url, params_json in self._search_urls(keyword=keyword, subreddit=subreddit, sort_mode=sort_mode, time_filter=time_filter):
                if len(posts) >= max_posts:
                    break

                params = json.loads(params_json)
                try:
                    if search_url.endswith(".json"):
                        payload = self._request(search_url, params=params, as_json=True)
                        candidates = self._parse_json_search(payload, keyword, subreddit, search_url)
                    else:
                        html = self._request(search_url, params=params, as_json=False)
                        candidates = self._parse_old_reddit_search(html, keyword, subreddit)
                        if not candidates:
                            fallback_url = f"{REDDIT_BASE_URL}/search.json"
                            fallback_params = {
                                "q": keyword,
                                "sort": sort_mode,
                                "limit": min(25, max_posts),
                            }
                            if time_filter:
                                fallback_params["t"] = time_filter
                            if subreddit:
                                fallback_params["restrict_sr"] = "on"
                                fallback_params["sr_detail"] = "1"
                            payload = self._request(fallback_url, params=fallback_params, as_json=True)
                            candidates = self._parse_json_search(payload, keyword, subreddit, fallback_url)
                except Exception as exc:
                    errors.append(f"Search failed for keyword={keyword!r}, subreddit={subreddit!r}: {exc}")
                    continue

                for candidate in candidates:
                    post_id = candidate.get("post_id")
                    if not post_id or post_id in seen_post_ids:
                        continue
                    seen_post_ids.add(str(post_id))
                    posts.append(candidate)
                    if len(posts) >= max_posts:
                        break

        if not posts:
            errors.append(f"No Reddit posts found for keyword={keyword!r}")
        return posts, errors

    def _comments_json_url(self, post_url: str) -> str:
        parsed = urlparse(post_url)
        path = parsed.path
        if not path.endswith(".json"):
            path = f"{path.rstrip('/')}.json"
        return urljoin(REDDIT_BASE_URL, path)

    def _iter_comment_nodes(self, children: list[dict[str, Any]]) -> list[dict[str, Any]]:
        queue = list(children)
        flattened: list[dict[str, Any]] = []

        while queue:
            node = queue.pop(0)
            if not isinstance(node, dict):
                continue
            if node.get("kind") != "t1":
                continue
            data = node.get("data", {})
            if not isinstance(data, dict):
                continue
            flattened.append(data)

            replies = data.get("replies")
            if isinstance(replies, dict):
                queue.extend(replies.get("data", {}).get("children", []))
        return flattened

    def _normalize_comment(
        self,
        *,
        event_name: str,
        keyword: str,
        post: dict[str, Any],
        comment: dict[str, Any],
        session_id: str,
        sort_mode: str,
        time_filter: str | None,
    ) -> dict[str, Any] | None:
        comment_id = comment.get("id")
        body = _safe_text(comment.get("body"))
        if not comment_id or not body:
            return None

        created_utc = comment.get("created_utc")
        created_at = None
        if created_utc is not None:
            try:
                created_at = datetime.fromtimestamp(float(created_utc), tz=timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                created_at = None

        post_url = post.get("post_url") or post.get("url") or ""
        post_id = post.get("post_id") or _extract_post_id(post_url)
        collected_at = _now_iso()
        language = _detect_language(body)
        author = comment.get("author")
        author_value = None if author in {None, "[deleted]"} else str(author)

        metadata = {
            "scraper": self.engine,
            "scraper_requested_engine": self.requested_engine,
            "comments_count_estimated": _to_int(post.get("comments_hint"), 0),
            "sort_mode": sort_mode,
            "time_filter": time_filter,
            "search_url": post.get("search_url"),
            "post_score": _to_int(post.get("score_hint"), 0),
            "post_created_at": post.get("post_created_at"),
            "comment_score": _to_int(comment.get("score"), 0),
            "permalink": comment.get("permalink"),
            "parent_id": comment.get("parent_id"),
            "session_id": session_id,
        }

        return {
            "id": f"reddit_comment_{comment_id}",
            "event": event_name,
            "event_name": event_name,
            "source": "reddit",
            "keyword": keyword,
            "subreddit": post.get("subreddit"),
            "post_id": post_id,
            "post_title": post.get("post_title"),
            "post_url": post_url,
            "comment_id": comment_id,
            "text": body,
            "author": author_value,
            "created_at": created_at,
            "collected_at": collected_at,
            "language": language,
            "metadata": metadata,
        }

    def _collect_comments_for_post(
        self,
        *,
        event_name: str,
        keyword: str,
        post: dict[str, Any],
        max_comments_per_post: int,
        session_id: str,
        sort_mode: str,
        time_filter: str | None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        errors: list[str] = []
        documents: list[dict[str, Any]] = []

        post_url = post.get("post_url")
        if not post_url:
            return documents, [f"Missing post_url for post_id={post.get('post_id')!r}"]

        comments_url = self._comments_json_url(str(post_url))
        params = {
            "sort": sort_mode,
            "limit": max_comments_per_post,
            "depth": 2,
        }
        if time_filter:
            params["t"] = time_filter

        try:
            payload = self._request(comments_url, params=params, as_json=True)
            if not isinstance(payload, list) or len(payload) < 2:
                return documents, [f"Unexpected comments payload for {comments_url}"]
            comment_listing = payload[1]
            comment_children = comment_listing.get("data", {}).get("children", [])
            flattened_comments = self._iter_comment_nodes(comment_children)
            if not flattened_comments:
                errors.append(f"No comments found for post_id={post.get('post_id')!r}")
                return documents, errors

            for comment in flattened_comments[:max_comments_per_post]:
                normalized = self._normalize_comment(
                    event_name=event_name,
                    keyword=keyword,
                    post=post,
                    comment=comment,
                    session_id=session_id,
                    sort_mode=sort_mode,
                    time_filter=time_filter,
                )
                if normalized:
                    documents.append(normalized)
        except Exception as exc:
            errors.append(f"Comments fetch failed for post_id={post.get('post_id')!r}: {exc}")

        return documents, errors

    def collect(
        self,
        *,
        event_name: str,
        keywords: str | list[str] | tuple[str, ...],
        subreddits: str | list[str] | tuple[str, ...] | None = None,
        max_posts: int = 20,
        max_comments_per_post: int = 50,
        sort_mode: str = "relevance",
        time_filter: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """Collect and normalize Reddit comments.

        The collector deliberately uses public web pages and JSON endpoints so it
        can run locally or in Lambda without Reddit API credentials.
        """

        session_id = session_id or uuid4().hex
        keyword_list = _coerce_csv_list(keywords) or [event_name]
        subreddit_list = _coerce_csv_list(subreddits)

        all_documents: list[dict[str, Any]] = []
        post_errors: list[str] = []
        comment_errors: list[str] = []
        seen_posts: set[str] = set()
        seen_comments: set[str] = set()
        posts_scanned = 0
        engine_warning = None
        if self.requested_engine != "requests":
            engine_warning = (
                f"Requested scraper engine {self.requested_engine!r} is not bundled in this build; "
                "using the requests fallback."
            )

        for keyword in keyword_list:
            if posts_scanned >= max_posts:
                break

            remaining_posts = max_posts - posts_scanned
            search_posts, search_errors = self.search_posts(
                keyword=keyword,
                subreddits=subreddit_list,
                sort_mode=sort_mode,
                time_filter=time_filter,
                max_posts=remaining_posts,
            )
            post_errors.extend(search_errors)

            for post in search_posts:
                post_id = str(post.get("post_id") or "")
                if not post_id or post_id in seen_posts:
                    continue
                seen_posts.add(post_id)
                posts_scanned += 1

                documents, errors = self._collect_comments_for_post(
                    event_name=event_name,
                    keyword=keyword,
                    post=post,
                    max_comments_per_post=max_comments_per_post,
                    session_id=session_id,
                    sort_mode=sort_mode,
                    time_filter=time_filter,
                )
                if errors:
                    comment_errors.extend(errors)

                for document in documents:
                    comment_key = document.get("comment_id")
                    if not comment_key or comment_key in seen_comments:
                        continue
                    seen_comments.add(str(comment_key))
                    all_documents.append(document)

                if posts_scanned >= max_posts:
                    break

        warnings: list[str] = []
        if post_errors:
            warnings.extend(post_errors)
        if comment_errors:
            warnings.extend(comment_errors)
        if engine_warning:
            warnings.append(engine_warning)

        if not all_documents and not warnings:
            warnings.append("No Reddit comments collected.")

        return {
            "status": "ok" if all_documents else "error",
            "session_id": session_id,
            "posts_scanned": posts_scanned,
            "comments_collected": len(all_documents),
            "documents": all_documents,
            "warnings": warnings,
        }


def run_reddit_collection(
    *,
    event_name: str,
    keywords: str | list[str] | tuple[str, ...],
    subreddits: str | list[str] | tuple[str, ...] | None = None,
    max_posts: int = 20,
    max_comments_per_post: int = 50,
    sort_mode: str = "relevance",
    time_filter: str | None = None,
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
    """Collect Reddit comments, optionally publish them to Kafka, and optionally back them up to S3."""

    collector = RedditScraperCollector(engine=engine)
    collection = collector.collect(
        event_name=event_name,
        keywords=keywords,
        subreddits=subreddits,
        max_posts=max_posts,
        max_comments_per_post=max_comments_per_post,
        sort_mode=sort_mode,
        time_filter=time_filter,
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

    errors = list(collection.get("warnings", []))
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
        "posts_scanned": collection["posts_scanned"],
        "comments_collected": collection["comments_collected"],
        "messages_published": messages_published,
        "kafka_topic": kafka_topic,
        "kafka_bootstrap_servers": kafka_bootstrap_servers,
        "s3_backup_key": s3_backup_key,
        "backup_error": backup_error,
        "errors": errors,
    }
    if include_documents:
        result["documents"] = documents
    return result
