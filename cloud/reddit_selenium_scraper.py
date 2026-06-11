"""Standalone Reddit scraper using Selenium for local or EC2-side collection.

This module is intentionally separate from the active YouTube collector path.
It scrapes public Reddit search results and comment pages, normalizes comments
into the existing Kafka schema shape, optionally publishes them to Kafka, and
writes a local JSONL backup.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus
import hashlib
import json
import time

from bs4 import BeautifulSoup
from langdetect import LangDetectException
from langdetect import detect
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from cloud.collector_config import load_collector_runtime_config, save_collector_runtime_config
from cloud.kafka_client import KafkaJsonPublisher
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_RAW_TOPIC, settings


DEFAULT_LOCAL_BACKUP_DIR = Path("raw") / "youtube"


@dataclass(slots=True)
class RedditSeleniumConfig:
    event_name: str
    keywords: list[str]
    subreddits: list[str]
    max_posts: int = 20
    max_comments_per_post: int = 50
    sort_mode: str = "relevance"
    time_filter: str = ""
    request_delay_seconds: float = 2.0
    headless: bool = True
    page_load_timeout_seconds: int = 30
    local_backup_dir: Path = DEFAULT_LOCAL_BACKUP_DIR
    kafka_bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    kafka_topic: str = KAFKA_RAW_TOPIC


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _slugify(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in value.strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "untitled"


def _parse_csv(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(part).strip() for part in value if str(part).strip()]


def _coalesce(*values: object) -> object:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return value
            continue
        return value
    return None


def _default_runtime_config() -> dict[str, str]:
    return {
        "event_name": settings.default_event_name,
        "search_query": settings.default_youtube_search_queries,
    }


def _build_driver(*, headless: bool = True, page_load_timeout_seconds: int = 30) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1600,2200")
    options.add_argument(
        "--user-agent="
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(page_load_timeout_seconds)
    return driver


def _wait_for_page(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))


def _search_url(keyword: str, subreddit: str | None, sort_mode: str, time_filter: str) -> str:
    base = f"https://old.reddit.com/r/{subreddit}/search" if subreddit else "https://old.reddit.com/search"
    params = [
        ("q", keyword),
        ("restrict_sr", "on" if subreddit else "off"),
        ("sort", sort_mode or "relevance"),
        ("t", time_filter or "all"),
        ("type", "link"),
    ]
    query = "&".join(f"{key}={quote_plus(value)}" for key, value in params)
    return f"{base}?{query}"


def _extract_post_candidates(page_source: str) -> list[dict]:
    soup = BeautifulSoup(page_source, "html.parser")
    posts: list[dict] = []
    seen_ids: set[str] = set()

    for thing in soup.select("div.thing[data-fullname][data-url]"):
        post_id = (thing.get("data-fullname") or "").strip()
        title_node = thing.select_one("a.title")
        if not post_id or title_node is None:
            continue

        post_url = (title_node.get("href") or "").strip()
        if not post_url:
            continue
        if post_url.startswith("/"):
            post_url = f"https://old.reddit.com{post_url}"

        comments_link = ""
        comments_anchor = thing.select_one("a.comments")
        if comments_anchor and comments_anchor.get("href"):
            comments_link = comments_anchor.get("href", "").strip()
            if comments_link.startswith("/"):
                comments_link = f"https://old.reddit.com{comments_link}"

        subreddit = ""
        subreddit_node = thing.select_one("a.subreddit")
        if subreddit_node:
            subreddit = subreddit_node.get_text(" ", strip=True).removeprefix("r/")

        if post_id in seen_ids:
            continue
        seen_ids.add(post_id)
        posts.append(
            {
                "post_id": post_id,
                "post_title": title_node.get_text(" ", strip=True),
                "post_url": post_url,
                "comments_url": comments_link or post_url,
                "subreddit": subreddit or None,
            }
        )

    return posts


def _extract_comments(
    page_source: str,
    *,
    event_name: str,
    keyword: str,
    fallback_subreddit: str | None,
    post: dict,
) -> list[dict]:
    soup = BeautifulSoup(page_source, "html.parser")
    documents: list[dict] = []
    collected_at = _now_iso()

    for comment_node in soup.select("div.thing.comment"):
        comment_id = (comment_node.get("data-fullname") or "").strip()
        if not comment_id:
            continue

        author_node = comment_node.select_one("a.author")
        author = author_node.get_text(" ", strip=True) if author_node else None

        time_node = comment_node.select_one("time")
        created_at = time_node.get("datetime") if time_node else None

        text_nodes = comment_node.select("div.entry div.usertext-body div.md p")
        text = " ".join(node.get_text(" ", strip=True) for node in text_nodes).strip()
        if not text:
            continue

        permalink_node = comment_node.select_one("a.bylink")
        permalink = permalink_node.get("href") if permalink_node else post["comments_url"]
        if permalink and permalink.startswith("/"):
            permalink = f"https://old.reddit.com{permalink}"

        raw_id = f"{post['post_id']}::{comment_id}::{text}"
        record_id = "reddit_comment_" + hashlib.md5(raw_id.encode("utf-8")).hexdigest()
        subreddit = post.get("subreddit") or fallback_subreddit

        documents.append(
            {
                "id": record_id,
                "event": event_name,
                "event_name": event_name,
                "source": "reddit",
                "text": text,
                "original_text": text,
                "cleaned_text": text.lower(),
                "author": author,
                "language": None,
                "created_at": created_at,
                "collected_at": collected_at,
                "metadata": {
                    "author": author,
                    "url": permalink,
                    "likes": 0,
                    "score": 0,
                    "comments_count": 0,
                    "comments_count_estimated": 0,
                    "video_id": post["post_id"],
                    "video_title": post["post_title"],
                    "youtube_type": "reddit_comment",
                    "keyword": keyword,
                    "search_query": keyword,
                    "channel_id": None,
                    "channel_title": None,
                    "subreddit": subreddit,
                    "post_id": post["post_id"],
                    "post_title": post["post_title"],
                    "post_url": post["post_url"],
                    "comment_id": comment_id,
                    "scraper": "reddit_selenium",
                    "scraper_requested_engine": "selenium",
                    "sort_mode": None,
                    "time_filter": None,
                    "search_url": post["comments_url"],
                    "post_score": 0,
                    "post_created_at": created_at,
                    "comment_score": 0,
                    "video_view_count": 0,
                    "video_like_count": 0,
                    "reply_count": 0,
                    "permalink": permalink,
                    "parent_id": None,
                    "session_id": None,
                },
                "keyword": keyword,
                "search_query": keyword,
                "channel_id": None,
                "channel_title": None,
                "subreddit": subreddit,
                "post_id": post["post_id"],
                "post_title": post["post_title"],
                "post_url": post["post_url"],
                "comment_id": comment_id,
                "video_id": post["post_id"],
                "video_title": post["post_title"],
                "likes": 0,
            }
        )

    return documents


def _write_local_jsonl(records: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _detect_language(text: str) -> str | None:
    candidate = (text or "").strip()
    if len(candidate) < 20:
        return None
    try:
        return detect(candidate)
    except LangDetectException:
        return None


def run_reddit_selenium_collection(
    *,
    event_name: str | None = None,
    keywords: str | Iterable[str] | None = None,
    subreddits: str | Iterable[str] | None = None,
    max_posts: int = 20,
    max_comments_per_post: int = 50,
    sort_mode: str = "relevance",
    time_filter: str = "",
    request_delay_seconds: float = 2.0,
    headless: bool = True,
    send_to_kafka: bool = False,
    kafka_bootstrap_servers: str | None = None,
    kafka_topic: str | None = None,
    output_jsonl_path: str | Path | None = None,
    use_s3_runtime_config: bool = True,
) -> dict:
    """Scrape Reddit with Selenium, optionally publish to Kafka, and save JSONL locally."""

    runtime_config: dict[str, object] = {}
    config_warning: str | None = None
    if use_s3_runtime_config:
        runtime_config, config_warning = load_collector_runtime_config()
        if not runtime_config:
            default_config = _default_runtime_config()
            saved_key, save_error = save_collector_runtime_config(
                default_config,
                bucket=settings.collector_config_bucket_name,
                key=settings.collector_config_s3_key,
                region=settings.aws_region,
            )
            if save_error:
                config_warning = (
                    f"Collector config missing in S3 and could not be created: {save_error}"
                )
            else:
                runtime_config = default_config
                config_warning = f"Collector config was missing and was created at `{saved_key}`."

    resolved_event_name = str(
        _coalesce(
            event_name,
            runtime_config.get("event_name"),
            settings.default_event_name,
        )
        or ""
    ).strip()
    resolved_keywords = _coalesce(
        keywords,
        runtime_config.get("keywords"),
        runtime_config.get("search_query"),
        settings.default_youtube_search_queries,
    )
    resolved_subreddits = _coalesce(
        subreddits,
        runtime_config.get("subreddits"),
        runtime_config.get("channel_ids"),
        settings.default_youtube_channel_ids,
    )
    resolved_max_posts = int(
        _coalesce(
            max_posts if max_posts > 0 else None,
            runtime_config.get("max_posts"),
            runtime_config.get("max_videos"),
            settings.youtube_max_videos,
        )
        or settings.youtube_max_videos
    )
    resolved_max_comments = int(
        _coalesce(
            max_comments_per_post if max_comments_per_post > 0 else None,
            runtime_config.get("max_comments_per_post"),
            runtime_config.get("max_comments_per_video"),
            settings.youtube_max_comments_per_video,
        )
        or settings.youtube_max_comments_per_video
    )
    resolved_sort_mode = str(
        _coalesce(
            sort_mode,
            runtime_config.get("sort_mode"),
            runtime_config.get("order"),
            "relevance",
        )
        or "relevance"
    ).strip()
    resolved_time_filter = str(_coalesce(time_filter, runtime_config.get("time_filter"), "") or "").strip()

    keyword_list = _parse_csv(resolved_keywords)
    subreddit_list = _parse_csv(resolved_subreddits)
    if not keyword_list:
        raise ValueError("At least one keyword is required.")

    config = RedditSeleniumConfig(
        event_name=resolved_event_name,
        keywords=keyword_list,
        subreddits=subreddit_list,
        max_posts=resolved_max_posts,
        max_comments_per_post=resolved_max_comments,
        sort_mode=resolved_sort_mode,
        time_filter=resolved_time_filter,
        request_delay_seconds=request_delay_seconds,
        headless=headless,
        kafka_bootstrap_servers=(kafka_bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS).strip(),
        kafka_topic=(kafka_topic or KAFKA_RAW_TOPIC).strip() or KAFKA_RAW_TOPIC,
    )

    session_id = hashlib.md5(f"{config.event_name}-{_now_iso()}".encode("utf-8")).hexdigest()
    warnings: list[str] = [config_warning] if config_warning else []
    documents: list[dict] = []
    posts_scanned = 0

    driver: webdriver.Chrome | None = None
    try:
        driver = _build_driver(headless=config.headless, page_load_timeout_seconds=config.page_load_timeout_seconds)
        targets = config.subreddits or [None]

        for keyword in config.keywords:
            for subreddit in targets:
                url = _search_url(keyword, subreddit, config.sort_mode, config.time_filter)
                try:
                    driver.get(url)
                    _wait_for_page(driver)
                    time.sleep(config.request_delay_seconds)
                except WebDriverException as exc:
                    warnings.append(f"Search failed for keyword='{keyword}', subreddit='{subreddit}': {exc}")
                    continue

                posts = _extract_post_candidates(driver.page_source)[: config.max_posts]
                if not posts:
                    warnings.append(f"No posts found for keyword='{keyword}', subreddit='{subreddit}'")
                    continue

                for post in posts:
                    posts_scanned += 1
                    try:
                        driver.get(post["comments_url"])
                        _wait_for_page(driver)
                        time.sleep(config.request_delay_seconds)
                    except WebDriverException as exc:
                        warnings.append(f"Post open failed for post='{post['post_id']}': {exc}")
                        continue

                    comments = _extract_comments(
                        driver.page_source,
                        event_name=config.event_name,
                        keyword=keyword,
                        fallback_subreddit=subreddit,
                        post=post,
                    )[: config.max_comments_per_post]

                    english_comments: list[dict] = []
                    for comment in comments:
                        language = _detect_language(comment.get("text", ""))
                        comment["language"] = language
                        comment["metadata"]["session_id"] = session_id
                        if language == "en":
                            english_comments.append(comment)
                    documents.extend(english_comments)

        seen_ids: set[str] = set()
        unique_documents: list[dict] = []
        for doc in documents:
            if doc["id"] in seen_ids:
                continue
            seen_ids.add(doc["id"])
            unique_documents.append(doc)
        documents = unique_documents

        messages_published = 0
        if send_to_kafka and documents:
            publisher = KafkaJsonPublisher(
                topic=config.kafka_topic,
                bootstrap_servers=config.kafka_bootstrap_servers,
            )
            try:
                messages_published = publisher.publish_batch(documents)
            finally:
                publisher.close()

        if output_jsonl_path is None:
            date_slug = datetime.now(UTC).strftime("%Y-%m-%d")
            filename = f"reddit_{_slugify(config.event_name)}_{date_slug}_{session_id}.jsonl"
            output_path = config.local_backup_dir / filename
        else:
            output_path = Path(output_jsonl_path)
        _write_local_jsonl(documents, output_path)

        return {
            "status": "ok" if documents or posts_scanned else "warning",
            "session_id": session_id,
            "posts_scanned": posts_scanned,
            "comments_collected": len(documents),
            "messages_published": messages_published,
            "kafka_topic": config.kafka_topic,
            "output_jsonl_path": str(output_path),
            "errors": warnings,
            "sample_documents": documents[:3],
        }
    finally:
        if driver is not None:
            driver.quit()
