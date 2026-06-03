"""Elasticsearch query helpers for the Streamlit dashboard."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from elasticsearch import Elasticsearch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings  # noqa: E402


_ELASTICSEARCH_CLIENT: Elasticsearch | None = None


def get_elasticsearch_client() -> Elasticsearch:
    """Return a cached Elasticsearch client."""

    global _ELASTICSEARCH_CLIENT
    if _ELASTICSEARCH_CLIENT is None:
        _ELASTICSEARCH_CLIENT = Elasticsearch(
            settings.elasticsearch_url,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3,
        )
    return _ELASTICSEARCH_CLIENT


def check_elasticsearch_connection() -> bool:
    """Check whether Elasticsearch is reachable."""

    try:
        return bool(get_elasticsearch_client().ping())
    except Exception as exc:
        print(f"[dashboard.queries] Elasticsearch connection failed: {exc}")
        return False


def _index_exists() -> bool:
    try:
        return get_elasticsearch_client().indices.exists(index=settings.elasticsearch_index)
    except Exception as exc:
        print(f"[dashboard.queries] Could not verify index existence: {exc}")
        return False


def _empty_dataframe(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _build_filters(
    event: str | None = None,
    source: str | None = None,
    model_version: str | None = None,
    sentiment: str | None = None,
) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    if event:
        filters.append({"term": {"event": event}})
    if source:
        filters.append({"term": {"source": source}})
    if model_version:
        filters.append({"term": {"model_version": model_version}})
    if sentiment:
        filters.append({"term": {"sentiment": sentiment}})
    return filters


def _build_query(
    event: str | None = None,
    source: str | None = None,
    model_version: str | None = None,
    sentiment: str | None = None,
) -> dict[str, Any]:
    filters = _build_filters(
        event=event,
        source=source,
        model_version=model_version,
        sentiment=sentiment,
    )
    if filters:
        return {"bool": {"filter": filters}}
    return {"match_all": {}}


def _safe_search(
    *,
    size: int = 10,
    query: dict[str, Any] | None = None,
    aggs: dict[str, Any] | None = None,
    sort: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not _index_exists():
        return {}

    try:
        client = get_elasticsearch_client()
        kwargs: dict[str, Any] = {
            "index": settings.elasticsearch_index,
            "size": size,
            "query": query or {"match_all": {}},
            "track_total_hits": True,
        }
        if aggs:
            kwargs["aggs"] = aggs
        if sort:
            kwargs["sort"] = sort
        return client.search(**kwargs)
    except Exception as exc:
        print(f"[dashboard.queries] Elasticsearch search failed: {exc}")
        return {}


def _flatten_hit(hit: dict[str, Any]) -> dict[str, Any]:
    source = hit.get("_source", {}) or {}
    metadata = source.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}

    return {
        "id": source.get("id") or hit.get("_id"),
        "created_at": source.get("created_at"),
        "processed_at": source.get("processed_at"),
        "event": source.get("event"),
        "source": source.get("source"),
        "sentiment": source.get("sentiment"),
        "confidence": source.get("confidence"),
        "original_text": source.get("original_text"),
        "cleaned_text": source.get("cleaned_text"),
        "language": source.get("language"),
        "model_name": source.get("model_name"),
        "model_version": source.get("model_version"),
        "author": metadata.get("author"),
        "url": metadata.get("url"),
        "likes": metadata.get("likes"),
        "comments_count": metadata.get("comments_count"),
        "video_id": metadata.get("video_id"),
        "video_title": metadata.get("video_title"),
        "youtube_type": metadata.get("youtube_type"),
        "keyword": metadata.get("keyword"),
        "subreddit": metadata.get("subreddit"),
        "reddit_type": metadata.get("reddit_type"),
    }


def _terms_dataframe(response: dict[str, Any], agg_name: str, field_name: str) -> pd.DataFrame:
    buckets = (
        response.get("aggregations", {})
        .get(agg_name, {})
        .get("buckets", [])
    )
    if not buckets:
        return _empty_dataframe([field_name, "count"])

    return pd.DataFrame(
        [
            {
                field_name: bucket.get("key"),
                "count": int(bucket.get("doc_count", 0)),
            }
            for bucket in buckets
        ]
    )


def get_total_documents(
    event: str | None = None,
    source: str | None = None,
    model_version: str | None = None,
    sentiment: str | None = None,
) -> int:
    """Return the document count for the current index and filters."""

    if not _index_exists():
        return 0

    try:
        response = get_elasticsearch_client().count(
            index=settings.elasticsearch_index,
            query=_build_query(
                event=event,
                source=source,
                model_version=model_version,
                sentiment=sentiment,
            ),
        )
        return int(response.get("count", 0))
    except Exception as exc:
        print(f"[dashboard.queries] Count query failed: {exc}")
        return 0


def get_available_events() -> list[str]:
    """Return available event names."""

    response = _safe_search(
        size=0,
        aggs={"events": {"terms": {"field": "event", "size": 100, "order": {"_count": "desc"}}}},
    )
    df = _terms_dataframe(response, "events", "event")
    return [str(value) for value in df.get("event", pd.Series(dtype=str)).dropna().tolist()]


def get_available_sources() -> list[str]:
    """Return available source names."""

    response = _safe_search(
        size=0,
        aggs={"sources": {"terms": {"field": "source", "size": 100, "order": {"_count": "desc"}}}},
    )
    df = _terms_dataframe(response, "sources", "source")
    return [str(value) for value in df.get("source", pd.Series(dtype=str)).dropna().tolist()]


def get_available_sentiments() -> list[str]:
    """Return available sentiment/emotion labels currently in the index."""

    response = _safe_search(
        size=0,
        aggs={"sentiments": {"terms": {"field": "sentiment", "size": 100, "order": {"_count": "desc"}}}},
    )
    df = _terms_dataframe(response, "sentiments", "sentiment")
    return [str(value) for value in df.get("sentiment", pd.Series(dtype=str)).dropna().tolist()]


def get_available_model_versions() -> list[str]:
    """Return available model versions."""

    response = _safe_search(
        size=0,
        aggs={
            "versions": {
                "terms": {
                    "field": "model_version",
                    "size": 100,
                    "order": {"_count": "desc"},
                }
            }
        },
    )
    df = _terms_dataframe(response, "versions", "model_version")
    return [str(value) for value in df.get("model_version", pd.Series(dtype=str)).dropna().tolist()]


def get_sentiment_counts(
    event: str | None = None,
    source: str | None = None,
    model_version: str | None = None,
    sentiment: str | None = None,
) -> pd.DataFrame:
    """Return sentiment counts as a DataFrame."""

    response = _safe_search(
        size=0,
        query=_build_query(
            event=event,
            source=source,
            model_version=model_version,
            sentiment=sentiment,
        ),
        aggs={
            "sentiments": {
                "terms": {
                    "field": "sentiment",
                    "size": 10,
                    "order": {"_count": "desc"},
                }
            }
        },
    )
    df = _terms_dataframe(response, "sentiments", "sentiment")
    if df.empty:
        return _empty_dataframe(["sentiment", "count"])
    return df[["sentiment", "count"]]


def get_average_confidence(
    event: str | None = None,
    source: str | None = None,
    model_version: str | None = None,
    sentiment: str | None = None,
) -> dict[str, Any]:
    """Return the average confidence and document count."""

    response = _safe_search(
        size=0,
        query=_build_query(
            event=event,
            source=source,
            model_version=model_version,
            sentiment=sentiment,
        ),
        aggs={"avg_confidence": {"avg": {"field": "confidence"}}},
    )
    avg_value = (
        response.get("aggregations", {})
        .get("avg_confidence", {})
        .get("value")
    )
    return {
        "average_confidence": float(avg_value or 0.0),
        "document_count": get_total_documents(
            event=event,
            source=source,
            model_version=model_version,
            sentiment=sentiment,
        ),
    }


def get_recent_posts(
    limit: int = 50,
    event: str | None = None,
    sentiment: str | None = None,
    source: str | None = None,
    model_version: str | None = None,
) -> pd.DataFrame:
    """Return the most recent analyzed posts."""

    response = _safe_search(
        size=max(1, int(limit)),
        query=_build_query(
            event=event,
            source=source,
            model_version=model_version,
            sentiment=sentiment,
        ),
        sort=[{"created_at": {"order": "desc"}}],
    )
    hits = response.get("hits", {}).get("hits", [])
    if not hits:
        return _empty_dataframe(
            [
                "id",
                "created_at",
                "processed_at",
                "event",
                "source",
                "sentiment",
                "confidence",
                "original_text",
                "cleaned_text",
                "language",
                "model_name",
                "model_version",
                "author",
                "url",
                "likes",
                "comments_count",
                "video_id",
                "video_title",
                "youtube_type",
                "keyword",
                "subreddit",
                "reddit_type",
            ]
        )

    rows = [_flatten_hit(hit) for hit in hits]
    return pd.DataFrame(rows)


def get_sentiment_timeline(
    event: str | None = None,
    source: str | None = None,
    interval: str = "1h",
    model_version: str | None = None,
    sentiment: str | None = None,
) -> pd.DataFrame:
    """Return sentiment counts over time."""

    response = _safe_search(
        size=0,
        query=_build_query(
            event=event,
            source=source,
            model_version=model_version,
            sentiment=sentiment,
        ),
        aggs={
            "timeline": {
                "date_histogram": {
                    "field": "created_at",
                    "fixed_interval": interval,
                    "min_doc_count": 0,
                },
                "aggs": {
                    "sentiments": {
                        "terms": {
                            "field": "sentiment",
                            "size": 50,
                        }
                    }
                },
            }
        },
    )
    buckets = (
        response.get("aggregations", {})
        .get("timeline", {})
        .get("buckets", [])
    )
    if not buckets:
        return _empty_dataframe(["timestamp", "total"])

    rows = []
    for bucket in buckets:
        row = {
            "timestamp": bucket.get("key_as_string") or bucket.get("key"),
            "total": int(bucket.get("doc_count", 0)),
        }
        sentiment_buckets = bucket.get("sentiments", {}).get("buckets", [])
        for sentiment_bucket in sentiment_buckets:
            label = str(sentiment_bucket.get("key", "unknown"))
            row[label] = int(sentiment_bucket.get("doc_count", 0))
        rows.append(row)
    return pd.DataFrame(rows)


def get_source_distribution(
    event: str | None = None,
    model_version: str | None = None,
    sentiment: str | None = None,
) -> pd.DataFrame:
    """Return the distribution of documents by source."""

    response = _safe_search(
        size=0,
        query=_build_query(event=event, model_version=model_version, sentiment=sentiment),
        aggs={"sources": {"terms": {"field": "source", "size": 100, "order": {"_count": "desc"}}}},
    )
    df = _terms_dataframe(response, "sources", "source")
    if df.empty:
        return _empty_dataframe(["source", "count"])
    return df[["source", "count"]]


def get_language_distribution(
    event: str | None = None,
    source: str | None = None,
    model_version: str | None = None,
    sentiment: str | None = None,
) -> pd.DataFrame:
    """Return the distribution of documents by language."""

    response = _safe_search(
        size=0,
        query=_build_query(
            event=event,
            source=source,
            model_version=model_version,
            sentiment=sentiment,
        ),
        aggs={"languages": {"terms": {"field": "language", "size": 100, "order": {"_count": "desc"}}}},
    )
    df = _terms_dataframe(response, "languages", "language")
    if df.empty:
        return _empty_dataframe(["language", "count"])
    return df[["language", "count"]]


def get_low_confidence_posts(
    threshold: float = 0.55,
    limit: int = 50,
    event: str | None = None,
    source: str | None = None,
    model_version: str | None = None,
    sentiment: str | None = None,
) -> pd.DataFrame:
    """Return low-confidence predictions."""

    if not _index_exists():
        return _empty_dataframe(
            [
                "id",
                "created_at",
                "processed_at",
                "event",
                "source",
                "sentiment",
                "confidence",
                "original_text",
                "cleaned_text",
                "language",
                "model_name",
                "model_version",
                "author",
                "url",
                "likes",
                "comments_count",
                "video_id",
                "video_title",
                "youtube_type",
                "keyword",
                "subreddit",
                "reddit_type",
            ]
        )

    filters = _build_filters(
        event=event,
        source=source,
        model_version=model_version,
        sentiment=sentiment,
    )
    filters.append({"range": {"confidence": {"lt": threshold}}})

    response = _safe_search(
        size=max(1, int(limit)),
        query={"bool": {"filter": filters}},
        sort=[{"confidence": {"order": "asc"}}, {"created_at": {"order": "desc"}}],
    )
    hits = response.get("hits", {}).get("hits", [])
    if not hits:
        return _empty_dataframe(
            [
                "id",
                "created_at",
                "processed_at",
                "event",
                "source",
                "sentiment",
                "confidence",
                "original_text",
                "cleaned_text",
                "language",
                "model_name",
                "model_version",
                "author",
                "url",
                "likes",
                "comments_count",
                "video_id",
                "video_title",
                "youtube_type",
                "keyword",
                "subreddit",
                "reddit_type",
            ]
        )
    return pd.DataFrame([_flatten_hit(hit) for hit in hits])


def get_model_stats(
    event: str | None = None,
    source: str | None = None,
    sentiment: str | None = None,
    model_version: str | None = None,
    low_confidence_threshold: float = 0.55,
) -> dict[str, Any]:
    """Return high-level model monitoring stats."""

    if not _index_exists():
        return {
            "model_name": None,
            "model_version": None,
            "average_confidence": 0.0,
            "document_count": 0,
            "low_confidence_count": 0,
            "version_counts": _empty_dataframe(["model_version", "count"]),
        }

    response = _safe_search(
        size=0,
        query=_build_query(
            event=event,
            source=source,
            model_version=model_version,
            sentiment=sentiment,
        ),
        aggs={
            "model_names": {"terms": {"field": "model_name", "size": 10, "order": {"_count": "desc"}}},
            "model_versions": {"terms": {"field": "model_version", "size": 50, "order": {"_count": "desc"}}},
            "avg_confidence": {"avg": {"field": "confidence"}},
            "low_confidence": {"filter": {"range": {"confidence": {"lt": low_confidence_threshold}}}},
        },
    )

    aggregations = response.get("aggregations", {})
    model_names = aggregations.get("model_names", {}).get("buckets", [])
    model_versions = aggregations.get("model_versions", {}).get("buckets", [])

    version_df = pd.DataFrame(
        [
            {
                "model_version": bucket.get("key"),
                "count": int(bucket.get("doc_count", 0)),
            }
            for bucket in model_versions
        ]
    )
    if version_df.empty:
        version_df = _empty_dataframe(["model_version", "count"])

    return {
        "model_name": model_names[0]["key"] if model_names else None,
        "model_version": model_versions[0]["key"] if model_versions else None,
        "average_confidence": float(aggregations.get("avg_confidence", {}).get("value") or 0.0),
        "document_count": get_total_documents(
            event=event,
            source=source,
            model_version=model_version,
            sentiment=sentiment,
        ),
        "low_confidence_count": int(aggregations.get("low_confidence", {}).get("doc_count", 0)),
        "version_counts": version_df,
    }
