"""Optional S3 JSONL backup client for raw collected comments."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

from config.settings import settings

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
except ImportError:  # pragma: no cover - runtime optional dependency
    boto3 = None
    BotoCoreError = ClientError = NoCredentialsError = Exception


class S3JsonlBackupClient:
    """Upload a batch of comment records to S3 as a JSONL object."""

    def __init__(self):
        self.enabled = bool(settings.enable_s3_backup and settings.s3_bucket_name)
        self.bucket = settings.s3_bucket_name
        self.prefix = settings.s3_prefix.strip("/") if settings.s3_prefix else "raw/youtube"
        self.region = settings.aws_region
        self._client = None

        if self.enabled and boto3 is not None:
            self._client = boto3.client("s3", region_name=self.region)

    def _build_key(self, event_name: str, session_id: str) -> str:
        safe_event = (event_name or "unknown_event").replace(" ", "_")
        date_value = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return (
            f"{self.prefix}/event={safe_event}/date={date_value}/"
            f"session_id={session_id}/comments_{uuid4().hex}.jsonl"
        )

    def upload_jsonl(self, records: list[dict], event_name: str, session_id: str) -> str | None:
        if not self.enabled or not records:
            return None
        if self._client is None:
            print("[S3JsonlBackupClient] boto3 unavailable or client not initialized; skipping S3 backup.")
            return None

        key = self._build_key(event_name=event_name, session_id=session_id)
        body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"

        try:
            self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/x-ndjson",
            )
            print(f"[S3JsonlBackupClient] Uploaded {len(records)} records to s3://{self.bucket}/{key}")
            return key
        except (NoCredentialsError, ClientError, BotoCoreError) as exc:
            print(f"[S3JsonlBackupClient] S3 upload failed: {exc}")
            return None

