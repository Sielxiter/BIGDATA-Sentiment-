"""Reusable Elasticsearch client helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from elasticsearch import Elasticsearch, helpers
from elasticsearch import ApiError

from config.settings import settings
from storage.index_mapping import get_sentiment_results_index_body


_ELASTICSEARCH_CLIENT: Elasticsearch | None = None


def get_elasticsearch_client() -> Elasticsearch:
    """Return a reusable Elasticsearch client instance."""

    global _ELASTICSEARCH_CLIENT
    if _ELASTICSEARCH_CLIENT is None:
        _ELASTICSEARCH_CLIENT = Elasticsearch(
            settings.elasticsearch_url,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3,
        )
    return _ELASTICSEARCH_CLIENT


def _format_es_error(exc: Exception) -> str:
    """Return a compact, readable Elasticsearch error summary."""

    status = getattr(exc, "status_code", None)
    info = getattr(exc, "info", None)
    if info is None:
        return f"{type(exc).__name__}(status={status!r})"
    return f"{type(exc).__name__}(status={status!r}, info={info!r})"


def create_index_if_not_exists(client: Elasticsearch | None = None) -> bool:
    """Ensure the sentiment_results index exists.

    Returns True when the index is ready for writes, and False only when the
    index could not be verified or created.
    """

    es_client = client or get_elasticsearch_client()
    index_name = settings.elasticsearch_index

    max_attempts = 5
    retry_delay_seconds = 2

    for attempt in range(1, max_attempts + 1):
        try:
            try:
                if es_client.indices.exists(index=index_name):
                    print(f"[elastic_client] Index already exists: {index_name}")
                    return True
            except ApiError as exc:
                # Some WSL/forwarded-connection setups return a generic 400 for the
                # HEAD existence check even though the cluster itself is reachable.
                # In that case, fall through to a create attempt and let the create
                # call provide the definitive answer.
                print(
                    f"[elastic_client] Index existence check failed for {index_name}: "
                    f"{_format_es_error(exc)}"
                )

            try:
                es_client.indices.create(index=index_name, **get_sentiment_results_index_body())
                print(f"[elastic_client] Created index: {index_name}")
                return True
            except ApiError as exc:
                info = getattr(exc, "info", {}) or {}
                error_type = None
                if isinstance(info, dict):
                    error_type = (info.get("error") or {}).get("type")
                if error_type == "resource_already_exists_exception":
                    print(f"[elastic_client] Index already exists: {index_name}")
                    return True
                print(
                    f"[elastic_client] Failed to create index {index_name}: "
                    f"{_format_es_error(exc)}"
                )
                return False
        except Exception as exc:
            if attempt < max_attempts:
                print(
                    f"[elastic_client] Elasticsearch not ready yet "
                    f"(attempt {attempt}/{max_attempts}): {exc}"
                )
                time.sleep(retry_delay_seconds)
                continue
            print(f"[elastic_client] Failed to create index {index_name}: {exc}")
            return False

    return False


def _json_safe_value(value: Any) -> Any:
    """Convert nested values to JSON/Elasticsearch-friendly types."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Mapping):
        return {key: _json_safe_value(inner) for key, inner in value.items()}

    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]

    return value


def _normalize_epoch_seconds_to_iso(value: Any) -> Any:
    """Convert numeric epoch-seconds values to strict ISO timestamps when possible."""

    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    try:
        decimal_value = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return value

    if decimal_value <= 0:
        return value

    try:
        seconds = float(decimal_value)
        return datetime.utcfromtimestamp(seconds).isoformat() + "Z"
    except Exception:
        return value


def _normalize_document(document: dict) -> dict | None:
    """Strip extra fields and enforce the stable Elasticsearch document shape."""

    if not isinstance(document, dict):
        print(f"[elastic_client] Skipping non-dict document: {type(document)!r}")
        return None

    document_id = document.get("id")
    if not document_id:
        print("[elastic_client] Skipping document without an id")
        return None

    metadata = document.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}

    metadata = {
        "author": metadata.get("author", document.get("author")),
        "url": metadata.get("url", document.get("post_url")),
        "likes": metadata.get("likes", document.get("likes")),
        "score": metadata.get("score"),
        "comments_count": metadata.get("comments_count"),
        "comments_count_estimated": metadata.get("comments_count_estimated"),
        "video_id": metadata.get("video_id", document.get("video_id")),
        "video_title": metadata.get("video_title", document.get("video_title")),
        "youtube_type": metadata.get("youtube_type"),
        "keyword": metadata.get("keyword", document.get("keyword")),
        "search_query": metadata.get("search_query", document.get("search_query")),
        "channel_id": metadata.get("channel_id", document.get("channel_id")),
        "channel_title": metadata.get("channel_title", document.get("channel_title")),
        "subreddit": metadata.get("subreddit", document.get("subreddit")),
        "post_id": metadata.get("post_id", document.get("post_id")),
        "post_title": metadata.get("post_title", document.get("post_title")),
        "post_url": metadata.get("post_url", document.get("post_url")),
        "comment_id": metadata.get("comment_id", document.get("comment_id")),
        "scraper": metadata.get("scraper"),
        "scraper_requested_engine": metadata.get("scraper_requested_engine"),
        "sort_mode": metadata.get("sort_mode"),
        "time_filter": metadata.get("time_filter"),
        "search_url": metadata.get("search_url"),
        "post_score": metadata.get("post_score"),
        "post_created_at": _normalize_epoch_seconds_to_iso(metadata.get("post_created_at")),
        "comment_score": metadata.get("comment_score"),
        "video_view_count": metadata.get("video_view_count"),
        "video_like_count": metadata.get("video_like_count"),
        "reply_count": metadata.get("reply_count"),
        "permalink": metadata.get("permalink"),
        "parent_id": metadata.get("parent_id"),
        "session_id": metadata.get("session_id"),
    }

    normalized_document = {
        "id": document_id,
        "event": document.get("event"),
        "event_name": document.get("event_name"),
        "source": document.get("source"),
        "original_text": document.get("original_text"),
        "cleaned_text": document.get("cleaned_text"),
        "keyword": document.get("keyword"),
        "search_query": document.get("search_query"),
        "channel_id": document.get("channel_id"),
        "channel_title": document.get("channel_title"),
        "subreddit": document.get("subreddit"),
        "post_id": document.get("post_id"),
        "post_title": document.get("post_title"),
        "post_url": document.get("post_url"),
        "comment_id": document.get("comment_id"),
        "author": document.get("author"),
        "language": document.get("language"),
        "sentiment": document.get("sentiment"),
        "confidence": document.get("confidence"),
        "model_name": document.get("model_name"),
        "model_version": document.get("model_version"),
        "created_at": document.get("created_at"),
        "collected_at": document.get("collected_at"),
        "processed_at": document.get("processed_at"),
        "video_id": document.get("video_id"),
        "video_title": document.get("video_title"),
        "likes": document.get("likes"),
        "metadata": metadata,
    }

    return _json_safe_value(normalized_document)


def index_document(document: dict) -> bool:
    """Index a single document using the stable id field."""

    try:
        create_index_if_not_exists()
        normalized = _normalize_document(document)
        if normalized is None:
            return False

        get_elasticsearch_client().index(
            index=settings.elasticsearch_index,
            id=normalized["id"],
            document=normalized,
        )
        get_elasticsearch_client().indices.refresh(index=settings.elasticsearch_index)
        print(f"[elastic_client] Indexed document id={normalized['id']}")
        return True
    except Exception as exc:
        print(f"[elastic_client] Failed to index document: {exc}")
        return False


def bulk_index_documents(documents: list[dict]) -> int:
    """Bulk index a batch of documents and return the number indexed."""

    if not documents:
        print("[elastic_client] No documents to index")
        return 0

    try:
        create_index_if_not_exists()
        actions = []
        for document in documents:
            normalized = _normalize_document(document)
            if normalized is None:
                continue
            actions.append(
                {
                    "_op_type": "index",
                    "_index": settings.elasticsearch_index,
                    "_id": normalized["id"],
                    "_source": normalized,
                }
            )

        if not actions:
            print("[elastic_client] No valid documents after normalization")
            return 0

        success_count, errors = helpers.bulk(
            get_elasticsearch_client(),
            actions,
            chunk_size=max(1, settings.elasticsearch_batch_size),
            raise_on_error=False,
            raise_on_exception=False,
        )

        if errors:
            print(f"[elastic_client] Bulk indexing completed with errors: {errors[:3]}")
        get_elasticsearch_client().indices.refresh(index=settings.elasticsearch_index)
        print(f"[elastic_client] Bulk indexed {success_count} documents")
        return int(success_count)
    except Exception as exc:
        print(f"[elastic_client] Bulk indexing failed: {exc}")
        return 0
