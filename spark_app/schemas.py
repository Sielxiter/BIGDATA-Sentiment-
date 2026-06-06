"""Spark schema definitions for raw social posts."""

from __future__ import annotations

from pyspark.sql.types import LongType, StringType, StructField, StructType


RAW_SOCIAL_POSTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("event", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("keyword", StringType(), True),
        StructField("subreddit", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("post_title", StringType(), True),
        StructField("post_url", StringType(), True),
        StructField("comment_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("author", StringType(), True),
        StructField("source", StringType(), True),
        StructField("language", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("collected_at", StringType(), True),
        StructField(
            "metadata",
            StructType(
                [
                    StructField("author", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("likes", LongType(), True),
                    StructField("score", LongType(), True),
                    StructField("comments_count", LongType(), True),
                    StructField("comments_count_estimated", LongType(), True),
                    StructField("video_id", StringType(), True),
                    StructField("video_title", StringType(), True),
                    StructField("youtube_type", StringType(), True),
                    StructField("keyword", StringType(), True),
                    StructField("subreddit", StringType(), True),
                    StructField("post_id", StringType(), True),
                    StructField("post_title", StringType(), True),
                    StructField("post_url", StringType(), True),
                    StructField("comment_id", StringType(), True),
                    StructField("scraper", StringType(), True),
                    StructField("scraper_requested_engine", StringType(), True),
                    StructField("sort_mode", StringType(), True),
                    StructField("time_filter", StringType(), True),
                    StructField("search_url", StringType(), True),
                    StructField("post_score", LongType(), True),
                    StructField("post_created_at", StringType(), True),
                    StructField("comment_score", LongType(), True),
                    StructField("permalink", StringType(), True),
                    StructField("parent_id", StringType(), True),
                    StructField("session_id", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

REQUIRED_RAW_POST_FIELDS = ["id", "source", "text", "collected_at"]
