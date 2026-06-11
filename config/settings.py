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
S3_PREFIX = os.getenv("S3_PREFIX", "raw/youtube")
ENABLE_S3_BACKUP = os.getenv("ENABLE_S3_BACKUP", "true").strip().lower() == "true"
COLLECTOR_CONFIG_BUCKET_NAME = os.getenv("COLLECTOR_CONFIG_BUCKET_NAME", S3_BUCKET_NAME)
COLLECTOR_CONFIG_S3_KEY = os.getenv("COLLECTOR_CONFIG_S3_KEY", "config/youtube_collector_runtime.json")
COLLECTOR_STATE_BUCKET_NAME = os.getenv("COLLECTOR_STATE_BUCKET_NAME", COLLECTOR_CONFIG_BUCKET_NAME)
COLLECTOR_STATE_S3_KEY = os.getenv("COLLECTOR_STATE_S3_KEY", "state/youtube_collector_state.json")
COLLECTOR_STATE_MAX_COMMENT_IDS = int(os.getenv("COLLECTOR_STATE_MAX_COMMENT_IDS", "5000"))

COLLECTOR_SOURCE = os.getenv("COLLECTOR_SOURCE", "youtube")
COLLECTOR_MODE = os.getenv("COLLECTOR_MODE", "local").strip().lower()
COLLECTOR_LAMBDA_FUNCTION_NAME = os.getenv("COLLECTOR_LAMBDA_FUNCTION_NAME", "youtube-collector-to-kafka")
COLLECTOR_LAMBDA_REGION = os.getenv("COLLECTOR_LAMBDA_REGION", AWS_REGION)

DEFAULT_EVENT_NAME = os.getenv("DEFAULT_EVENT_NAME", "Morocco football")
DEFAULT_YOUTUBE_SEARCH_QUERIES = os.getenv(
    "DEFAULT_YOUTUBE_SEARCH_QUERIES",
    "morocco football,Morocco match,CAN Morocco",
)
DEFAULT_YOUTUBE_VIDEO_IDS = os.getenv("DEFAULT_YOUTUBE_VIDEO_IDS", "")
DEFAULT_YOUTUBE_CHANNEL_IDS = os.getenv("DEFAULT_YOUTUBE_CHANNEL_IDS", "")
DEFAULT_YOUTUBE_ORDER = os.getenv("DEFAULT_YOUTUBE_ORDER", "relevance")
DEFAULT_YOUTUBE_REGION_CODE = os.getenv("DEFAULT_YOUTUBE_REGION_CODE", "")
DEFAULT_YOUTUBE_RELEVANCE_LANGUAGE = os.getenv("DEFAULT_YOUTUBE_RELEVANCE_LANGUAGE", "en")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
YOUTUBE_API_BASE_URL = os.getenv("YOUTUBE_API_BASE_URL", "https://www.googleapis.com/youtube/v3")
YOUTUBE_MAX_VIDEOS = int(os.getenv("YOUTUBE_MAX_VIDEOS", "20"))
YOUTUBE_MAX_COMMENTS_PER_VIDEO = int(os.getenv("YOUTUBE_MAX_COMMENTS_PER_VIDEO", "50"))
YOUTUBE_REQUEST_DELAY_SECONDS = float(os.getenv("YOUTUBE_REQUEST_DELAY_SECONDS", "0.25"))
YOUTUBE_API_TIMEOUT_SECONDS = float(os.getenv("YOUTUBE_API_TIMEOUT_SECONDS", "20"))
YOUTUBE_USER_AGENT = os.getenv(
    "YOUTUBE_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
)
YOUTUBE_PUBLISHED_AFTER = os.getenv("YOUTUBE_PUBLISHED_AFTER", "")
YOUTUBE_PUBLISHED_BEFORE = os.getenv("YOUTUBE_PUBLISHED_BEFORE", "")

# Legacy aliases kept for backward compatibility with older imports.
DEFAULT_REDDIT_KEYWORDS = DEFAULT_YOUTUBE_SEARCH_QUERIES
DEFAULT_REDDIT_SUBREDDITS = DEFAULT_YOUTUBE_CHANNEL_IDS
DEFAULT_REDDIT_SORT_MODE = DEFAULT_YOUTUBE_ORDER
DEFAULT_REDDIT_TIME_FILTER = ""
REDDIT_SCRAPER_ENGINE = os.getenv("REDDIT_SCRAPER_ENGINE", "youtube_api").strip().lower()
REDDIT_MAX_POSTS = YOUTUBE_MAX_VIDEOS
REDDIT_MAX_COMMENTS_PER_POST = YOUTUBE_MAX_COMMENTS_PER_VIDEO
REDDIT_REQUEST_DELAY_SECONDS = YOUTUBE_REQUEST_DELAY_SECONDS
REDDIT_TIMEOUT_SECONDS = YOUTUBE_API_TIMEOUT_SECONDS
REDDIT_HEADLESS = False
REDDIT_USER_AGENT = YOUTUBE_USER_AGENT
REDDIT_CHROME_USER_DATA_DIR = os.getenv("REDDIT_CHROME_USER_DATA_DIR", "").strip()
REDDIT_CHROME_PROFILE_DIRECTORY = os.getenv("REDDIT_CHROME_PROFILE_DIRECTORY", "Default").strip()
REDDIT_USE_CHROME_PROFILE = os.getenv("REDDIT_USE_CHROME_PROFILE", "false").strip().lower() == "true"

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
    collector_state_bucket_name: str = COLLECTOR_STATE_BUCKET_NAME
    collector_state_s3_key: str = COLLECTOR_STATE_S3_KEY
    collector_state_max_comment_ids: int = COLLECTOR_STATE_MAX_COMMENT_IDS
    collector_source: str = COLLECTOR_SOURCE
    collector_mode: str = COLLECTOR_MODE
    collector_lambda_function_name: str = COLLECTOR_LAMBDA_FUNCTION_NAME
    collector_lambda_region: str = COLLECTOR_LAMBDA_REGION

    default_event_name: str = DEFAULT_EVENT_NAME
    default_youtube_search_queries: str = DEFAULT_YOUTUBE_SEARCH_QUERIES
    default_youtube_video_ids: str = DEFAULT_YOUTUBE_VIDEO_IDS
    default_youtube_channel_ids: str = DEFAULT_YOUTUBE_CHANNEL_IDS
    default_youtube_order: str = DEFAULT_YOUTUBE_ORDER
    default_youtube_region_code: str = DEFAULT_YOUTUBE_REGION_CODE
    default_youtube_relevance_language: str = DEFAULT_YOUTUBE_RELEVANCE_LANGUAGE
    youtube_api_key: str = YOUTUBE_API_KEY
    youtube_api_base_url: str = YOUTUBE_API_BASE_URL
    youtube_max_videos: int = YOUTUBE_MAX_VIDEOS
    youtube_max_comments_per_video: int = YOUTUBE_MAX_COMMENTS_PER_VIDEO
    youtube_request_delay_seconds: float = YOUTUBE_REQUEST_DELAY_SECONDS
    youtube_api_timeout_seconds: float = YOUTUBE_API_TIMEOUT_SECONDS
    youtube_user_agent: str = YOUTUBE_USER_AGENT
    youtube_published_after: str = YOUTUBE_PUBLISHED_AFTER
    youtube_published_before: str = YOUTUBE_PUBLISHED_BEFORE

    # Legacy aliases retained so older code paths still import cleanly.
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
    reddit_chrome_user_data_dir: str = REDDIT_CHROME_USER_DATA_DIR
    reddit_chrome_profile_directory: str = REDDIT_CHROME_PROFILE_DIRECTORY
    reddit_use_chrome_profile: bool = REDDIT_USE_CHROME_PROFILE

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
