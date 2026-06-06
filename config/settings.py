"""Central configuration loaded from environment variables."""

from __future__ import annotations

from dataclasses import dataclass

import os

from dotenv import load_dotenv


load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "raw_social_posts")
KAFKA_PROCESSED_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC", "processed_sentiments")

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sentiment_results")
ELASTICSEARCH_BATCH_SIZE = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "100"))

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/reddit")
ENABLE_S3_BACKUP = os.getenv("ENABLE_S3_BACKUP", "true").strip().lower() == "true"
COLLECTOR_CONFIG_BUCKET_NAME = os.getenv("COLLECTOR_CONFIG_BUCKET_NAME", S3_BUCKET_NAME)
COLLECTOR_CONFIG_S3_KEY = os.getenv("COLLECTOR_CONFIG_S3_KEY", "config/reddit_collector_runtime.json")

COLLECTOR_SOURCE = os.getenv("COLLECTOR_SOURCE", "reddit")
COLLECTOR_MODE = os.getenv("COLLECTOR_MODE", "local").strip().lower()
COLLECTOR_LAMBDA_FUNCTION_NAME = os.getenv("COLLECTOR_LAMBDA_FUNCTION_NAME", "reddit-collector-to-kafka")
COLLECTOR_LAMBDA_REGION = os.getenv("COLLECTOR_LAMBDA_REGION", AWS_REGION)

DEFAULT_EVENT_NAME = os.getenv("DEFAULT_EVENT_NAME", "Morocco football")
DEFAULT_REDDIT_KEYWORDS = os.getenv("DEFAULT_REDDIT_KEYWORDS", "morocco football,Morocco match,CAN Morocco")
DEFAULT_REDDIT_SUBREDDITS = os.getenv("DEFAULT_REDDIT_SUBREDDITS", "soccer,worldcup")
DEFAULT_REDDIT_SORT_MODE = os.getenv("DEFAULT_REDDIT_SORT_MODE", "relevance")
DEFAULT_REDDIT_TIME_FILTER = os.getenv("DEFAULT_REDDIT_TIME_FILTER", "")
REDDIT_SCRAPER_ENGINE = os.getenv("REDDIT_SCRAPER_ENGINE", "requests").strip().lower()
REDDIT_MAX_POSTS = int(os.getenv("REDDIT_MAX_POSTS", "20"))
REDDIT_MAX_COMMENTS_PER_POST = int(os.getenv("REDDIT_MAX_COMMENTS_PER_POST", "50"))
REDDIT_REQUEST_DELAY_SECONDS = float(os.getenv("REDDIT_REQUEST_DELAY_SECONDS", "2"))
REDDIT_TIMEOUT_SECONDS = float(os.getenv("REDDIT_TIMEOUT_SECONDS", "15"))
REDDIT_HEADLESS = os.getenv("REDDIT_HEADLESS", "true").strip().lower() == "true"
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
)

MODEL_PROVIDER = os.getenv("MODEL_PROVIDER", "huggingface")
MODEL_NAME = os.getenv("MODEL_NAME", "cardiffnlp/twitter-xlm-roberta-base-sentiment")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1")
MODEL_DEVICE = os.getenv("MODEL_DEVICE", "cpu")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "SocialMediaSentimentStreaming")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "checkpoints/spark")
SPARK_OUTPUT_MODE = os.getenv("SPARK_OUTPUT_MODE", "both")
SPARK_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "10"))


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    kafka_raw_topic: str = KAFKA_RAW_TOPIC
    kafka_processed_topic: str = KAFKA_PROCESSED_TOPIC

    elasticsearch_url: str = ELASTICSEARCH_URL
    elasticsearch_index: str = ELASTICSEARCH_INDEX
    elasticsearch_batch_size: int = ELASTICSEARCH_BATCH_SIZE
    aws_region: str = AWS_REGION
    s3_bucket_name: str = S3_BUCKET_NAME
    s3_prefix: str = S3_PREFIX
    enable_s3_backup: bool = ENABLE_S3_BACKUP
    collector_config_bucket_name: str = COLLECTOR_CONFIG_BUCKET_NAME
    collector_config_s3_key: str = COLLECTOR_CONFIG_S3_KEY
    collector_source: str = COLLECTOR_SOURCE
    collector_mode: str = COLLECTOR_MODE
    collector_lambda_function_name: str = COLLECTOR_LAMBDA_FUNCTION_NAME
    collector_lambda_region: str = COLLECTOR_LAMBDA_REGION

    default_event_name: str = DEFAULT_EVENT_NAME
    default_reddit_keywords: str = DEFAULT_REDDIT_KEYWORDS
    default_reddit_subreddits: str = DEFAULT_REDDIT_SUBREDDITS
    default_reddit_sort_mode: str = DEFAULT_REDDIT_SORT_MODE
    default_reddit_time_filter: str = DEFAULT_REDDIT_TIME_FILTER
    reddit_scraper_engine: str = REDDIT_SCRAPER_ENGINE
    reddit_max_posts: int = REDDIT_MAX_POSTS
    reddit_max_comments_per_post: int = REDDIT_MAX_COMMENTS_PER_POST
    reddit_request_delay_seconds: float = REDDIT_REQUEST_DELAY_SECONDS
    reddit_timeout_seconds: float = REDDIT_TIMEOUT_SECONDS
    reddit_headless: bool = REDDIT_HEADLESS
    reddit_user_agent: str = REDDIT_USER_AGENT

    model_provider: str = MODEL_PROVIDER
    model_name: str = MODEL_NAME
    model_version: str = MODEL_VERSION
    model_device: str = MODEL_DEVICE

    spark_app_name: str = SPARK_APP_NAME
    spark_master: str = SPARK_MASTER
    spark_checkpoint_dir: str = SPARK_CHECKPOINT_DIR
    spark_output_mode: str = SPARK_OUTPUT_MODE
    spark_max_offsets_per_trigger: int = SPARK_MAX_OFFSETS_PER_TRIGGER


settings = Settings()
