"""Persistent dedup state for the YouTube collector.

State is stored in S3 when a bucket is configured. The structure is keyed by
collector event name so each campaign keeps its own seen-comment history.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from config.settings import settings


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _resolve_bucket(bucket: str | None = None) -> str:
    return (bucket or settings.collector_state_bucket_name or "").strip()


def _resolve_key(key: str | None = None) -> str:
    return (key or settings.collector_state_s3_key or "state/youtube_collector_state.json").strip()


def _resolve_region(region: str | None = None) -> str:
    return (region or settings.aws_region or "eu-north-1").strip()


def _normalize_state(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"version": 1, "events": {}}

    events = payload.get("events")
    if not isinstance(events, dict):
        events = {}

    normalized_events: dict[str, Any] = {}
    for event_name, event_state in events.items():
        if not isinstance(event_state, dict):
            continue
        seen_comment_ids = event_state.get("seen_comment_ids", [])
        if not isinstance(seen_comment_ids, list):
            seen_comment_ids = []
        normalized_events[str(event_name)] = {
            "seen_comment_ids": [str(item).strip() for item in seen_comment_ids if str(item).strip()],
            "updated_at": str(event_state.get("updated_at") or ""),
            "total_seen": int(event_state.get("total_seen", len(seen_comment_ids) or 0)),
        }

    return {
        "version": int(payload.get("version", 1)),
        "events": normalized_events,
    }


def load_collector_state(
    *,
    bucket: str | None = None,
    key: str | None = None,
    region: str | None = None,
) -> tuple[dict[str, Any], str | None]:
    """Load the dedup state from S3.

    Missing configuration is not treated as an error because the collector can
    still run without persistent deduplication.
    """

    resolved_bucket = _resolve_bucket(bucket)
    resolved_key = _resolve_key(key)
    resolved_region = _resolve_region(region)

    if not resolved_bucket:
        return {"version": 1, "events": {}}, None

    client = boto3.client("s3", region_name=resolved_region)
    try:
        response = client.get_object(Bucket=resolved_bucket, Key=resolved_key)
        payload = response["Body"].read().decode("utf-8")
        state = json.loads(payload) if payload else {"version": 1, "events": {}}
        return _normalize_state(state), None
    except ClientError as exc:
        code = (exc.response or {}).get("Error", {}).get("Code", "") if hasattr(exc, "response") else ""
        if code in {"NoSuchKey", "NoSuchBucket", "404", "NotFound"}:
            return {"version": 1, "events": {}}, None
        return {"version": 1, "events": {}}, f"Could not load collector state from S3: {code or str(exc)}"
    except (BotoCoreError, NoCredentialsError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        return {"version": 1, "events": {}}, f"Could not load collector state from S3: {exc}"


def save_collector_state(
    state: dict[str, Any],
    *,
    bucket: str | None = None,
    key: str | None = None,
    region: str | None = None,
) -> tuple[str | None, str | None]:
    """Persist dedup state to S3 as JSON."""

    resolved_bucket = _resolve_bucket(bucket)
    resolved_key = _resolve_key(key)
    resolved_region = _resolve_region(region)

    if not resolved_bucket:
        return None, "Collector state bucket name is missing."

    normalized_state = _normalize_state(state)
    normalized_state["updated_at"] = _now_iso()

    client = boto3.client("s3", region_name=resolved_region)
    try:
        client.put_object(
            Bucket=resolved_bucket,
            Key=resolved_key,
            Body=json.dumps(normalized_state, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        return resolved_key, None
    except (NoCredentialsError, ClientError, BotoCoreError, ValueError, TypeError) as exc:
        return None, str(exc)


def extract_seen_comment_ids(state: dict[str, Any], event_name: str) -> set[str]:
    normalized = _normalize_state(state)
    event_state = normalized.get("events", {}).get(str(event_name), {})
    seen_comment_ids = event_state.get("seen_comment_ids", [])
    if not isinstance(seen_comment_ids, list):
        return set()
    return {str(item).strip() for item in seen_comment_ids if str(item).strip()}


def merge_seen_comment_ids(
    state: dict[str, Any],
    *,
    event_name: str,
    new_comment_ids: list[str] | set[str] | tuple[str, ...],
    max_ids_per_event: int | None = None,
) -> dict[str, Any]:
    normalized = _normalize_state(state)
    events = normalized.setdefault("events", {})
    event_state = events.get(str(event_name), {})
    if not isinstance(event_state, dict):
        event_state = {}

    existing_ids = event_state.get("seen_comment_ids", [])
    if not isinstance(existing_ids, list):
        existing_ids = []

    merged: list[str] = []
    seen: set[str] = set()
    for item in existing_ids:
        text = str(item).strip()
        if text and text not in seen:
            merged.append(text)
            seen.add(text)

    for item in new_comment_ids:
        text = str(item).strip()
        if text and text not in seen:
            merged.append(text)
            seen.add(text)

    if max_ids_per_event is None:
        max_ids_per_event = int(settings.collector_state_max_comment_ids)
    if max_ids_per_event > 0 and len(merged) > max_ids_per_event:
        merged = merged[-max_ids_per_event:]

    events[str(event_name)] = {
        "seen_comment_ids": merged,
        "updated_at": _now_iso(),
        "total_seen": len(merged),
    }
    normalized["events"] = events
    return normalized
