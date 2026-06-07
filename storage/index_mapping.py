"""Elasticsearch index mapping for processed sentiment documents."""

from __future__ import annotations


SENTIMENT_RESULTS_INDEX_MAPPING = {
    "dynamic": False,
    "properties": {
        "id": {"type": "keyword"},
        "event": {"type": "keyword"},
        "event_name": {"type": "keyword"},
        "source": {"type": "keyword"},
        "original_text": {"type": "text"},
        "cleaned_text": {"type": "text"},
        "keyword": {"type": "keyword"},
        "search_query": {"type": "keyword"},
        "channel_id": {"type": "keyword"},
        "channel_title": {"type": "text"},
        "subreddit": {"type": "keyword"},
        "post_id": {"type": "keyword"},
        "post_title": {"type": "text"},
        "post_url": {"type": "keyword"},
        "comment_id": {"type": "keyword"},
        "author": {"type": "keyword"},
        "language": {"type": "keyword"},
        "sentiment": {"type": "keyword"},
        "confidence": {"type": "float"},
        "model_name": {"type": "keyword"},
        "model_version": {"type": "keyword"},
        "created_at": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis",
        },
        "collected_at": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis",
        },
        "processed_at": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis",
        },
        "video_id": {"type": "keyword"},
        "video_title": {"type": "text"},
        "likes": {"type": "integer"},
        "metadata": {
            "type": "object",
            "dynamic": False,
            "properties": {
                "author": {"type": "keyword"},
                "url": {"type": "keyword"},
                "likes": {"type": "integer"},
                "score": {"type": "integer"},
                "comments_count": {"type": "integer"},
                "comments_count_estimated": {"type": "integer"},
                "video_id": {"type": "keyword"},
                "video_title": {"type": "text"},
                "youtube_type": {"type": "keyword"},
                "keyword": {"type": "keyword"},
                "search_query": {"type": "keyword"},
                "channel_id": {"type": "keyword"},
                "channel_title": {"type": "text"},
                "subreddit": {"type": "keyword"},
                "post_id": {"type": "keyword"},
                "post_title": {"type": "text"},
                "post_url": {"type": "keyword"},
                "comment_id": {"type": "keyword"},
                "scraper": {"type": "keyword"},
                "scraper_requested_engine": {"type": "keyword"},
                "sort_mode": {"type": "keyword"},
                "time_filter": {"type": "keyword"},
                "search_url": {"type": "keyword"},
                "post_score": {"type": "integer"},
                "post_created_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                },
                "comment_score": {"type": "integer"},
                "video_view_count": {"type": "integer"},
                "video_like_count": {"type": "integer"},
                "reply_count": {"type": "integer"},
                "permalink": {"type": "keyword"},
                "parent_id": {"type": "keyword"},
                "session_id": {"type": "keyword"},
            },
        },
    },
}


def get_sentiment_results_index_body() -> dict:
    """Return the full index definition used for sentiment_results."""

    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": SENTIMENT_RESULTS_INDEX_MAPPING,
    }
