"""Central configuration loaded from environment variables.

This module keeps a single source of truth for Kafka, Elasticsearch, and model
settings so future components can stay modular and avoid hardcoded values.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
import os


load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "raw_social_posts")
KAFKA_PROCESSED_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC", "processed_sentiments")

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sentiment_results")

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "sentiment-bigdata-platform/1.0")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
DEFAULT_EVENT_NAME = os.getenv("DEFAULT_EVENT_NAME", "Morocco football")
DEFAULT_YOUTUBE_KEYWORDS = os.getenv(
    "DEFAULT_YOUTUBE_KEYWORDS",
    "Morocco football,Morocco match,CAN Morocco",
)
DEFAULT_YOUTUBE_MAX_VIDEOS = int(os.getenv("DEFAULT_YOUTUBE_MAX_VIDEOS", "5"))
DEFAULT_YOUTUBE_MAX_COMMENTS = int(os.getenv("DEFAULT_YOUTUBE_MAX_COMMENTS", "50"))
DEFAULT_REDDIT_KEYWORDS = os.getenv(
    "DEFAULT_REDDIT_KEYWORDS",
    "Morocco football,Morocco match,CAN Morocco",
)
DEFAULT_REDDIT_LIMIT = int(os.getenv("DEFAULT_REDDIT_LIMIT", "20"))

MODEL_PROVIDER = os.getenv("MODEL_PROVIDER", "huggingface")
MODEL_NAME = os.getenv("MODEL_NAME", "cardiffnlp/twitter-xlm-roberta-base-sentiment")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1")
MODEL_DEVICE = os.getenv("MODEL_DEVICE", "cpu")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "SocialMediaSentimentStreaming")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "checkpoints/spark")

STREAMLIT_REFRESH_SECONDS = int(os.getenv("STREAMLIT_REFRESH_SECONDS", "5"))
PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    kafka_raw_topic: str = KAFKA_RAW_TOPIC
    kafka_processed_topic: str = KAFKA_PROCESSED_TOPIC

    elasticsearch_url: str = ELASTICSEARCH_URL
    elasticsearch_index: str = ELASTICSEARCH_INDEX

    reddit_client_id: str = REDDIT_CLIENT_ID
    reddit_client_secret: str = REDDIT_CLIENT_SECRET
    reddit_user_agent: str = REDDIT_USER_AGENT
    youtube_api_key: str = YOUTUBE_API_KEY
    default_event_name: str = DEFAULT_EVENT_NAME
    default_youtube_keywords: str = DEFAULT_YOUTUBE_KEYWORDS
    default_youtube_max_videos: int = DEFAULT_YOUTUBE_MAX_VIDEOS
    default_youtube_max_comments: int = DEFAULT_YOUTUBE_MAX_COMMENTS
    default_reddit_keywords: str = DEFAULT_REDDIT_KEYWORDS
    default_reddit_limit: int = DEFAULT_REDDIT_LIMIT

    model_provider: str = MODEL_PROVIDER
    model_name: str = MODEL_NAME
    model_version: str = MODEL_VERSION
    model_device: str = MODEL_DEVICE

    spark_app_name: str = SPARK_APP_NAME
    spark_master: str = SPARK_MASTER
    spark_checkpoint_dir: str = SPARK_CHECKPOINT_DIR

    streamlit_refresh_seconds: int = STREAMLIT_REFRESH_SECONDS
    project_root: Path = PROJECT_ROOT


settings = Settings()
