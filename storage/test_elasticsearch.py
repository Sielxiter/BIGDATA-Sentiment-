"""Small Elasticsearch smoke test for the sentiment_results index."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings  # noqa: E402
from storage.elastic_client import (  # noqa: E402
    create_index_if_not_exists,
    get_elasticsearch_client,
    index_document,
)


TEST_DOCUMENT = {
    "id": "test-elasticsearch-document",
    "event": "Morocco football",
    "source": "youtube",
    "original_text": "Morocco played very well today!",
    "cleaned_text": "morocco played very well today",
    "language": "en",
    "sentiment": "positive",
    "confidence": 0.94,
    "model_name": "cardiffnlp/twitter-xlm-roberta-base-sentiment",
    "model_version": "v1",
    "created_at": datetime.now(timezone.utc).isoformat(),
    "processed_at": datetime.now(timezone.utc).isoformat(),
    "metadata": {
        "author": "anonymous",
        "url": "https://www.youtube.com/watch?v=VIDEO_ID",
        "likes": 12,
        "comments_count": 0,
        "video_id": "VIDEO_ID",
        "video_title": "Video title",
        "youtube_type": "comment",
        "keyword": "Morocco football",
    },
}


def main() -> int:
    client = get_elasticsearch_client()
    if not create_index_if_not_exists(client):
        print("[test_elasticsearch] Elasticsearch index is not ready")
        return 1

    indexed = index_document(TEST_DOCUMENT)
    if not indexed:
        print("[test_elasticsearch] Failed to index test document")
        return 1

    client.indices.refresh(index=settings.elasticsearch_index)
    count_result = client.count(index=settings.elasticsearch_index)
    search_result = client.search(
        index=settings.elasticsearch_index,
        query={"term": {"id": TEST_DOCUMENT["id"]}},
        size=1,
    )

    print(f"[test_elasticsearch] Index count: {count_result.get('count', 0)}")
    print(
        "[test_elasticsearch] Search hit:\n"
        + json.dumps(search_result.get("hits", {}).get("hits", [{}])[0], indent=2)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
