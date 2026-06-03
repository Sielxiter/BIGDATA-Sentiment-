"""Bridge raw JSONL records from S3 into Kafka.

Main use case:
AWS Lambda writes raw comments to S3, then this local bridge streams those
records into Kafka topic `raw_social_posts`.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Iterable

import boto3

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    AWS_REGION,
    S3_BRIDGE_MAX_FILES_PER_CYCLE,
    S3_BRIDGE_POLL_SECONDS,
    S3_BUCKET_NAME,
    S3_PREFIX,
)
from kafka_app.producer import KafkaPostProducer


STATE_FILE = Path("cloud/.s3_bridge_state.json")


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {"processed": {}}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"processed": {}}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_jsonl_lines(body: str) -> Iterable[dict]:
    for line in body.splitlines():
        row = line.strip()
        if not row:
            continue
        try:
            data = json.loads(row)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            yield data


def _list_candidate_objects(s3_client, bucket: str, prefix: str) -> list[dict]:
    objects: list[dict] = []
    continuation_token: str | None = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        page = s3_client.list_objects_v2(**kwargs)
        objects.extend(page.get("Contents", []))
        if not page.get("IsTruncated"):
            break
        continuation_token = page.get("NextContinuationToken")

    objects.sort(key=lambda x: x.get("LastModified"))
    return objects


def run_once(bucket: str, prefix: str, max_files: int) -> dict:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    producer = KafkaPostProducer()
    state = _load_state()
    processed: dict[str, str] = state.setdefault("processed", {})

    scanned = 0
    sent = 0
    new_files = 0

    try:
        for obj in _list_candidate_objects(s3, bucket, prefix):
            key = obj.get("Key", "")
            etag = str(obj.get("ETag", "")).strip('"')
            if not key or not key.endswith(".jsonl"):
                continue
            scanned += 1
            if processed.get(key) == etag:
                continue

            if new_files >= max_files:
                break

            response = s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read().decode("utf-8", errors="replace")
            count_this_file = 0
            for record in _iter_jsonl_lines(body):
                if record.get("id") and record.get("text") and record.get("source"):
                    producer.send_post(record)
                    sent += 1
                    count_this_file += 1

            processed[key] = etag
            new_files += 1
            print(f"[s3_to_kafka_bridge] Forwarded {count_this_file} record(s) from s3://{bucket}/{key}")
    finally:
        producer.close()
        _save_state(state)

    return {"scanned_objects": scanned, "new_files": new_files, "sent_records": sent}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge S3 JSONL files to Kafka.")
    parser.add_argument("--bucket", default=S3_BUCKET_NAME, help="S3 bucket name.")
    parser.add_argument("--prefix", default=S3_PREFIX, help="S3 prefix where JSONL files are stored.")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=S3_BRIDGE_POLL_SECONDS,
        help="Polling interval in seconds for continuous mode.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=S3_BRIDGE_MAX_FILES_PER_CYCLE,
        help="Max new S3 objects to process per cycle.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.bucket:
        print("[s3_to_kafka_bridge] Missing bucket. Set S3_BUCKET_NAME in .env or pass --bucket.")
        return 1

    if args.once:
        result = run_once(args.bucket, args.prefix, max(1, args.max_files))
        print(f"[s3_to_kafka_bridge] Done: {result}")
        return 0

    poll_seconds = max(1, int(args.poll_seconds))
    while True:
        result = run_once(args.bucket, args.prefix, max(1, args.max_files))
        print(f"[s3_to_kafka_bridge] Cycle done: {result}. Sleeping {poll_seconds}s...")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
