"""Elasticsearch index mapping for processed sentiment documents."""

from __future__ import annotations


SENTIMENT_RESULTS_INDEX_MAPPING = {
    "dynamic": False,
    "properties": {
        "id": {"type": "keyword"},
        "event": {"type": "keyword"},
        "source": {"type": "keyword"},
        "original_text": {"type": "text"},
        "cleaned_text": {"type": "text"},
        "language": {"type": "keyword"},
        "sentiment": {"type": "keyword"},
        "confidence": {"type": "float"},
        "model_name": {"type": "keyword"},
        "model_version": {"type": "keyword"},
        "created_at": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis",
        },
        "processed_at": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis",
        },
        "metadata": {
            "type": "object",
            "dynamic": False,
            "properties": {
                "author": {"type": "keyword"},
                "url": {"type": "keyword"},
                "likes": {"type": "integer"},
                "comments_count": {"type": "integer"},
                "video_id": {"type": "keyword"},
                "video_title": {"type": "text"},
                "youtube_type": {"type": "keyword"},
                "keyword": {"type": "keyword"},
                "subreddit": {"type": "keyword"},
                "reddit_type": {"type": "keyword"},
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
