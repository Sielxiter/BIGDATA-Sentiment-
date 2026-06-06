"""S3 backup helpers for Lambda-collected raw comments."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


class S3JsonlBackupClient:
    """Upload a batch of raw comment records to S3 as a JSONL object."""

    def __init__(self, *, bucket: str, prefix: str, region: str) -> None:
        self.bucket = bucket.strip()
        self.prefix = prefix.strip("/") if prefix else "raw/reddit"
        self.region = region
        self._client = boto3.client("s3", region_name=self.region)

    def _build_key(self, event_name: str, session_id: str) -> str:
        safe_event = (event_name or "unknown_event").replace(" ", "_")
        date_value = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return f"{self.prefix}/event={safe_event}/date={date_value}/session_id={session_id}/comments.jsonl"

    def upload_jsonl(self, records: list[dict], event_name: str, session_id: str) -> str:
        if not self.bucket:
            raise ValueError("S3 bucket name is missing.")
        if not records:
            raise ValueError("No records provided for S3 backup.")

        key = self._build_key(event_name=event_name, session_id=session_id)
        body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        return key


def upload_records_to_s3(
    *,
    records: list[dict],
    event_name: str,
    session_id: str,
    bucket: str,
    prefix: str,
    region: str,
) -> tuple[str | None, str | None]:
    """Upload records to S3 and return (s3_key, error_message)."""

    if not bucket:
        return None, "S3_BUCKET_NAME is missing."
    if not records:
        return None, "No records collected for S3 backup."

    client = S3JsonlBackupClient(bucket=bucket, prefix=prefix, region=region)
    try:
        key = client.upload_jsonl(records, event_name=event_name, session_id=session_id)
        return key, None
    except (NoCredentialsError, ClientError, BotoCoreError, ValueError) as exc:
        return None, str(exc)
