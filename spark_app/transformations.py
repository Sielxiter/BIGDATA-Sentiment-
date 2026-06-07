"""Spark DataFrame transformations for raw social posts."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from nlp.preprocessing import clean_text
from nlp.sentiment_service import SentimentService
from spark_app.schemas import REQUIRED_RAW_POST_FIELDS, RAW_SOCIAL_POSTS_SCHEMA


SENTIMENT_RESULT_SCHEMA = StructType(
    [
        StructField("label", StringType(), True),
        StructField("score", DoubleType(), True),
        StructField("model_name", StringType(), True),
        StructField("model_version", StringType(), True),
    ]
)

_SENTIMENT_SERVICE = None


def _get_sentiment_service() -> SentimentService:
    global _SENTIMENT_SERVICE
    if _SENTIMENT_SERVICE is None:
        _SENTIMENT_SERVICE = SentimentService()
    return _SENTIMENT_SERVICE


def _predict_sentiment(text: str | None) -> dict:
    """Worker-side helper used by the Spark UDF.

    BERT inference inside Spark is intentionally simple in version 1. Later we
    can replace this with batch inference, Pandas UDFs, or a separate service.
    """

    return _get_sentiment_service().predict(text or "")


def parse_kafka_messages(df: DataFrame) -> DataFrame:
    """Parse Kafka value bytes into structured columns."""

    parsed_df = df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("json_str"),
    )
    parsed_df = parsed_df.withColumn("parsed", F.from_json(F.col("json_str"), RAW_SOCIAL_POSTS_SCHEMA))

    return parsed_df.select(
        "kafka_key",
        F.col("parsed.*"),
    )


def validate_posts(df: DataFrame) -> DataFrame:
    """Filter out rows missing required fields or empty text."""

    condition = F.lit(True)
    for field in REQUIRED_RAW_POST_FIELDS:
        condition = condition & F.col(field).isNotNull() & (F.length(F.trim(F.col(field))) > 0)

    event_condition = (
        (F.col("event_name").isNotNull() & (F.length(F.trim(F.col("event_name"))) > 0))
        | (F.col("event").isNotNull() & (F.length(F.trim(F.col("event"))) > 0))
    )

    return df.filter(condition & event_condition)


def clean_posts(df: DataFrame) -> DataFrame:
    """Add cleaned text and processed timestamp columns."""

    clean_text_udf = F.udf(lambda value: clean_text(value, lowercase=True), StringType())

    return (
        df.withColumn("original_text", F.col("text"))
        .withColumn("cleaned_text", clean_text_udf(F.col("text")))
        .withColumn("event", F.coalesce(F.col("event_name"), F.col("event")))
        .withColumn("event_name", F.coalesce(F.col("event_name"), F.col("event")))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("author", F.coalesce(F.col("author"), F.col("metadata.author")))
        .withColumn("keyword", F.coalesce(F.col("keyword"), F.col("metadata.keyword")))
        .withColumn("search_query", F.coalesce(F.col("search_query"), F.col("metadata.search_query"), F.col("keyword"), F.col("metadata.keyword")))
        .withColumn("channel_id", F.coalesce(F.col("channel_id"), F.col("metadata.channel_id")))
        .withColumn("channel_title", F.coalesce(F.col("channel_title"), F.col("metadata.channel_title")))
        .withColumn("subreddit", F.coalesce(F.col("subreddit"), F.col("metadata.subreddit")))
        .withColumn("post_id", F.coalesce(F.col("post_id"), F.col("metadata.post_id")))
        .withColumn("post_title", F.coalesce(F.col("post_title"), F.col("metadata.post_title")))
        .withColumn("post_url", F.coalesce(F.col("post_url"), F.col("metadata.post_url"), F.col("metadata.url")))
        .withColumn("comment_id", F.coalesce(F.col("comment_id"), F.col("metadata.comment_id")))
        .withColumn("collected_at", F.coalesce(F.col("collected_at"), F.col("created_at")))
        .withColumn("video_id", F.col("metadata.video_id"))
        .withColumn("video_title", F.col("metadata.video_title"))
        .withColumn("likes", F.col("metadata.likes"))
        .select(
            "id",
            "event",
            "event_name",
            "source",
            "original_text",
            "cleaned_text",
            "keyword",
            "search_query",
            "channel_id",
            "channel_title",
            "subreddit",
            "post_id",
            "post_title",
            "post_url",
            "comment_id",
            "author",
            "language",
            "created_at",
            "collected_at",
            "processed_at",
            "video_id",
            "video_title",
            "likes",
            "metadata",
        )
    )


def add_sentiment_predictions(df: DataFrame) -> DataFrame:
    """Add a standardized sentiment result to the cleaned posts."""

    sentiment_udf = F.udf(_predict_sentiment, SENTIMENT_RESULT_SCHEMA)

    return (
        df.withColumn("sentiment_result", sentiment_udf(F.col("cleaned_text")))
        .withColumn("sentiment", F.col("sentiment_result.label"))
        .withColumn("confidence", F.col("sentiment_result.score"))
        .withColumn("model_name", F.col("sentiment_result.model_name"))
        .withColumn("model_version", F.col("sentiment_result.model_version"))
        .select(
            "id",
            "event",
            "event_name",
            "source",
            "original_text",
            "cleaned_text",
            "keyword",
            "search_query",
            "channel_id",
            "channel_title",
            "subreddit",
            "post_id",
            "post_title",
            "post_url",
            "comment_id",
            "author",
            "sentiment",
            "confidence",
            "model_name",
            "model_version",
            "language",
            "created_at",
            "collected_at",
            "processed_at",
            "video_id",
            "video_title",
            "likes",
            "metadata",
        )
    )
