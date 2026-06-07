"""Persisted runtime configuration for the YouTube collector."""

from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from config.settings import settings


def _resolve_bucket(bucket: str | None = None) -> str:
    return (bucket or settings.collector_config_bucket_name or "").strip()


def _resolve_key(key: str | None = None) -> str:
    return (key or settings.collector_config_s3_key or "config/youtube_collector_runtime.json").strip()


def _resolve_region(region: str | None = None) -> str:
    return (region or settings.aws_region or "eu-north-1").strip()


def load_collector_runtime_config(
    *,
    bucket: str | None = None,
    key: str | None = None,
    region: str | None = None,
) -> tuple[dict[str, Any], str | None]:
    """Load the latest collector configuration from S3.

    Returns a tuple of (config, error_message). Missing config is not treated
    as an error because the app can still fall back to environment defaults.
    """

    resolved_bucket = _resolve_bucket(bucket)
    resolved_key = _resolve_key(key)
    resolved_region = _resolve_region(region)

    if not resolved_bucket:
        return {}, "Collector config bucket name is missing."

    client = boto3.client("s3", region_name=resolved_region)
    try:
        response = client.get_object(Bucket=resolved_bucket, Key=resolved_key)
        payload = response["Body"].read().decode("utf-8")
        return (json.loads(payload) if payload else {}), None
    except ClientError as exc:
        code = (exc.response or {}).get("Error", {}).get("Code", "") if hasattr(exc, "response") else ""
        if code in {"NoSuchKey", "NoSuchBucket", "404", "NotFound"}:
            return {}, None
        return {}, f"Could not load collector config from S3: {code or str(exc)}"
    except (BotoCoreError, NoCredentialsError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        return {}, f"Could not load collector config from S3: {exc}"


def save_collector_runtime_config(
    config: dict[str, Any],
    *,
    bucket: str | None = None,
    key: str | None = None,
    region: str | None = None,
) -> tuple[str | None, str | None]:
    """Persist the collector configuration to S3 as JSON."""

    resolved_bucket = _resolve_bucket(bucket)
    resolved_key = _resolve_key(key)
    resolved_region = _resolve_region(region)

    if not resolved_bucket:
        return None, "Collector config bucket name is missing."
    if not isinstance(config, dict) or not config:
        return None, "No collector configuration provided."

    client = boto3.client("s3", region_name=resolved_region)
    try:
        client.put_object(
            Bucket=resolved_bucket,
            Key=resolved_key,
            Body=json.dumps(config, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        return resolved_key, None
    except (NoCredentialsError, ClientError, BotoCoreError, ValueError, TypeError) as exc:
        return None, str(exc)
