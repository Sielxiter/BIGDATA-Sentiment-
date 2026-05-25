"""Spark schema definitions for raw social posts."""

from __future__ import annotations

from pyspark.sql.types import LongType, StringType, StructField, StructType


RAW_SOCIAL_POSTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("event", StringType(), True),
        StructField("text", StringType(), True),
        StructField("source", StringType(), True),
        StructField("language", StringType(), True),
        StructField("created_at", StringType(), True),+
        StructField(
            "metadata",
            StructType(
                [
                    StructField("author", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("likes", LongType(), True),
                    StructField("comments_count", LongType(), True),
                    StructField("video_id", StringType(), True),
                    StructField("video_title", StringType(), True),
                    StructField("youtube_type", StringType(), True),
                    StructField("keyword", StringType(), True),
                    StructField("subreddit", StringType(), True),
                    StructField("reddit_type", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

REQUIRED_RAW_POST_FIELDS = ["id", "event", "text", "source", "created_at"]
