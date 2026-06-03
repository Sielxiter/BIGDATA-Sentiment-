"""Reusable Elasticsearch client helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
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
        print(f"[elastic_client] Failed to create index {index_name}: {exc}")
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

    # Spark currently carries a few convenience columns at the top level.
    # Keep the Elasticsearch source stable by reshaping them back into metadata.
    metadata = {
        "author": metadata.get("author"),
        "url": metadata.get("url"),
        "likes": metadata.get("likes", document.get("likes")),
        "comments_count": metadata.get("comments_count"),
        "video_id": metadata.get("video_id", document.get("video_id")),
        "video_title": metadata.get("video_title", document.get("video_title")),
        "youtube_type": metadata.get("youtube_type"),
        "keyword": metadata.get("keyword", document.get("keyword")),
        "subreddit": metadata.get("subreddit"),
        "reddit_type": metadata.get("reddit_type"),
    }

    normalized_document = {
        "id": document_id,
        "event": document.get("event"),
        "source": document.get("source"),
        "original_text": document.get("original_text"),
        "cleaned_text": document.get("cleaned_text"),
        "language": document.get("language"),
        "sentiment": document.get("sentiment"),
        "confidence": document.get("confidence"),
        "model_name": document.get("model_name"),
        "model_version": document.get("model_version"),
        "created_at": document.get("created_at"),
        "processed_at": document.get("processed_at"),
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
        print(f"[elastic_client] Bulk indexed {success_count} documents")
        return int(success_count)
    except Exception as exc:
        print(f"[elastic_client] Bulk indexing failed: {exc}")
        return 0
