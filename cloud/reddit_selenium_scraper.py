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
import shutil
import tempfile
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
DEFAULT_DEBUG_DIR = Path("raw") / "youtube" / "debug"


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
    use_chrome_profile: bool = False
    chrome_user_data_dir: str = ""
    chrome_profile_directory: str = "Default"
    local_backup_dir: Path = DEFAULT_LOCAL_BACKUP_DIR
    debug_dir: Path = DEFAULT_DEBUG_DIR
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


def _prepare_temp_chrome_profile(user_data_dir: str, profile_directory: str) -> tuple[str, str, str]:
    source_root = Path(user_data_dir).expanduser()
    if not source_root.exists():
        raise FileNotFoundError(f"Chrome user data directory not found: {source_root}")

    source_profile_dir = source_root / profile_directory
    if not source_profile_dir.exists():
        raise FileNotFoundError(
            f"Chrome profile directory not found: {source_profile_dir}"
        )

    temp_root = Path(tempfile.mkdtemp(prefix="reddit-selenium-profile-"))
    cloned_user_data_dir = temp_root / "user-data"
    cloned_user_data_dir.mkdir(parents=True, exist_ok=True)

    local_state = source_root / "Local State"
    if local_state.exists():
        shutil.copy2(local_state, cloned_user_data_dir / "Local State")

    target_profile_dir = cloned_user_data_dir / profile_directory
    shutil.copytree(source_profile_dir, target_profile_dir, dirs_exist_ok=True)
    return str(cloned_user_data_dir), profile_directory, str(temp_root)


def _build_driver(
    *,
    headless: bool = True,
    page_load_timeout_seconds: int = 30,
    use_chrome_profile: bool = False,
    chrome_user_data_dir: str = "",
    chrome_profile_directory: str = "Default",
) -> tuple[webdriver.Chrome, str | None]:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--remote-debugging-port=0")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1600,2200")
    options.add_argument(
        "--user-agent="
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    temp_profile_root: str | None = None
    if use_chrome_profile and chrome_user_data_dir.strip():
        cloned_user_data_dir, cloned_profile_directory, temp_profile_root = _prepare_temp_chrome_profile(
            chrome_user_data_dir.strip(),
            chrome_profile_directory.strip() or "Default",
        )
        options.add_argument(f"--user-data-dir={cloned_user_data_dir}")
        options.add_argument(f"--profile-directory={cloned_profile_directory}")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(page_load_timeout_seconds)
    return driver, temp_profile_root


def _wait_for_page(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))


def _is_reddit_blocked(page_source: str) -> bool:
    text = (page_source or "").lower()
    return (
        "whoa there, pardner" in text
        or "your request has been blocked" in text
        or "<title>blocked</title>" in text
        or "network policy" in text
        or "blocked by network security" in text
    )


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

    candidate_nodes = soup.select(
        "div.search-result-link, div.thing[data-fullname][data-url], div.thing.link"
    )
    for thing in candidate_nodes:
        post_id = (thing.get("data-fullname") or thing.get("id") or "").strip()
        title_node = thing.select_one("a.search-title, a.title")
        if title_node is None:
            continue

        post_url = (title_node.get("href") or "").strip()
        if not post_url:
            continue
        if post_url.startswith("/"):
            post_url = f"https://old.reddit.com{post_url}"

        comments_link = ""
        comments_anchor = thing.select_one("a.search-comments, a.comments")
        if comments_anchor and comments_anchor.get("href"):
            comments_link = comments_anchor.get("href", "").strip()
            if comments_link.startswith("/"):
                comments_link = f"https://old.reddit.com{comments_link}"

        subreddit_node = thing.select_one(
            "a.search-subreddit-link, a.subreddit, span.search-subreddit-link a"
        )
        subreddit = subreddit_node.get_text(" ", strip=True).removeprefix("r/") if subreddit_node else ""

        if not post_id:
            raw_key = f"{title_node.get_text(' ', strip=True)}::{post_url}"
            post_id = "reddit_post_" + hashlib.md5(raw_key.encode("utf-8")).hexdigest()

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


def _write_debug_snapshot(*, debug_dir: Path, keyword: str, subreddit: str | None, page_source: str) -> str:
    debug_dir.mkdir(parents=True, exist_ok=True)
    subreddit_slug = _slugify(subreddit or "all")
    keyword_slug = _slugify(keyword)
    output_path = debug_dir / f"reddit_search_{subreddit_slug}_{keyword_slug}.html"
    output_path.write_text(page_source or "", encoding="utf-8")
    return str(output_path)


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
    use_chrome_profile: bool | None = None,
    chrome_user_data_dir: str | None = None,
    chrome_profile_directory: str | None = None,
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
        use_chrome_profile=settings.reddit_use_chrome_profile if use_chrome_profile is None else use_chrome_profile,
        chrome_user_data_dir=(chrome_user_data_dir or settings.reddit_chrome_user_data_dir).strip(),
        chrome_profile_directory=(chrome_profile_directory or settings.reddit_chrome_profile_directory).strip() or "Default",
        kafka_bootstrap_servers=(kafka_bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS).strip(),
        kafka_topic=(kafka_topic or KAFKA_RAW_TOPIC).strip() or KAFKA_RAW_TOPIC,
    )

    if config.use_chrome_profile and not config.chrome_user_data_dir:
        raise ValueError("Chrome profile mode requires a Chrome user data directory.")

    if config.use_chrome_profile and config.headless:
        warnings_headless = "Chrome profile mode does not support headless reliably; forcing headless=False."
    else:
        warnings_headless = None

    session_id = hashlib.md5(f"{config.event_name}-{_now_iso()}".encode("utf-8")).hexdigest()
    warnings: list[str] = [config_warning] if config_warning else []
    if warnings_headless:
        warnings.append(warnings_headless)
    documents: list[dict] = []
    posts_scanned = 0

    driver: webdriver.Chrome | None = None
    temp_profile_root: str | None = None
    try:
        driver, temp_profile_root = _build_driver(
            headless=False if config.use_chrome_profile else config.headless,
            page_load_timeout_seconds=config.page_load_timeout_seconds,
            use_chrome_profile=config.use_chrome_profile,
            chrome_user_data_dir=config.chrome_user_data_dir,
            chrome_profile_directory=config.chrome_profile_directory,
        )
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

                page_source = driver.page_source
                if _is_reddit_blocked(page_source):
                    debug_path = _write_debug_snapshot(
                        debug_dir=config.debug_dir,
                        keyword=keyword,
                        subreddit=subreddit,
                        page_source=page_source,
                    )
                    warnings.append(
                        "Reddit blocked the search page with a network policy response. "
                        f"This scraper will not collect posts from this IP/browser session. Debug HTML: {debug_path}"
                    )
                    break

                posts = _extract_post_candidates(page_source)[: config.max_posts]
                if not posts:
                    page_title = ""
                    try:
                        page_title = driver.title.strip()
                    except Exception:
                        page_title = ""
                    debug_path = _write_debug_snapshot(
                        debug_dir=config.debug_dir,
                        keyword=keyword,
                        subreddit=subreddit,
                        page_source=page_source,
                    )
                    warnings.append(
                        f"No posts found for keyword='{keyword}', subreddit='{subreddit}', "
                        f"url='{url}', title='{page_title}', debug_html='{debug_path}'"
                    )
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
        if temp_profile_root:
            shutil.rmtree(temp_profile_root, ignore_errors=True)
