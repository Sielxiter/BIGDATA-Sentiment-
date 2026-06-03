"""Live YouTube collector service with immediate Kafka forwarding."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Callable
from uuid import uuid4

from cloud.s3_client import S3JsonlBackupClient
from collectors.youtube_collector import YouTubeCollector
from kafka_app.producer import KafkaPostProducer


StatusCallback = Callable[[str], None]


class LiveYouTubeCollectorService:
    """Collect comments and forward each record immediately to Kafka."""

    def __init__(
        self,
        event_name: str,
        keywords: list[str],
        max_videos: int,
        max_comments: int,
        poll_interval_seconds: int = 5,
        status_callback: StatusCallback | None = None,
        stop_checker: Callable[[], bool] | None = None,
    ):
        self.event_name = event_name
        self.keywords = keywords
        self.max_videos = max_videos
        self.max_comments = max_comments
        self.poll_interval_seconds = max(1, int(poll_interval_seconds))
        self.status_callback = status_callback
        self.stop_checker = stop_checker or (lambda: False)
        self.session_id = uuid4().hex
        self._seen_ids: set[str] = set()
        self._s3_client = S3JsonlBackupClient()

    def _log(self, message: str) -> None:
        full = f"[LiveYouTubeCollectorService] {message}"
        print(full)
        if self.status_callback:
            self.status_callback(full)

    def _should_stop(self) -> bool:
        return bool(self.stop_checker())

    def _stream_comments_once(self, collector: YouTubeCollector, producer: KafkaPostProducer) -> int:
        sent_count = 0
        backup_batch: list[dict] = []
        seen_videos: set[str] = set()

        for keyword in collector.keywords:
            if self._should_stop():
                self._log("Stop requested. Ending collection loop.")
                break

            videos = collector.search_videos(keyword)
            for video in videos:
                if self._should_stop():
                    self._log("Stop requested. Ending video loop.")
                    break

                video_id = video.get("video_id", "")
                if not video_id or video_id in seen_videos:
                    continue
                seen_videos.add(video_id)

                video_title = video.get("video_title", "")
                comments = collector.collect_comments_from_video(video_id, video_title, keyword)
                for item in comments:
                    if self._should_stop():
                        self._log("Stop requested. Ending comment loop.")
                        break

                    if not item or not item.get("text"):
                        continue
                    if item["id"] in self._seen_ids:
                        continue

                    # Real-time behavior: forward each comment immediately.
                    producer.send_post(item)
                    sent_count += 1
                    self._seen_ids.add(item["id"])
                    backup_batch.append(item)
                    self._log(f"Sent comment id={item['id']} to Kafka.")

                    # Keep S3 backup incremental and non-blocking for flow.
                    if len(backup_batch) >= 50:
                        self._s3_client.upload_jsonl(backup_batch, self.event_name, self.session_id)
                        backup_batch.clear()

        if backup_batch:
            self._s3_client.upload_jsonl(backup_batch, self.event_name, self.session_id)

        return sent_count

    def run_limited(self) -> dict:
        collector = YouTubeCollector(
            event_name=self.event_name,
            keywords=self.keywords,
            max_videos=self.max_videos,
            max_comments=self.max_comments,
        )
        producer = KafkaPostProducer()
        try:
            sent_count = self._stream_comments_once(collector=collector, producer=producer)
            return {"status": "completed", "session_id": self.session_id, "sent_count": sent_count}
        finally:
            producer.close()

    def run_timed(self, duration_minutes: int) -> dict:
        duration_minutes = max(1, int(duration_minutes))
        stop_at = datetime.now(timezone.utc) + timedelta(minutes=duration_minutes)
        total_sent = 0

        collector = YouTubeCollector(
            event_name=self.event_name,
            keywords=self.keywords,
            max_videos=self.max_videos,
            max_comments=self.max_comments,
        )
        producer = KafkaPostProducer()
        try:
            while datetime.now(timezone.utc) < stop_at:
                if self._should_stop():
                    self._log("Stop requested. Ending timed collection.")
                    break

                sent_count = self._stream_comments_once(collector=collector, producer=producer)
                total_sent += sent_count
                self._log(f"Timed cycle complete. Sent {sent_count} comments this cycle.")

                if self._should_stop():
                    break
                time.sleep(self.poll_interval_seconds)
        finally:
            producer.close()

        return {"status": "completed", "session_id": self.session_id, "sent_count": total_sent}

