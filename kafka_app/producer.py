"""Reusable Kafka producer for raw social posts.

This module stays small so collectors can reuse it later without knowing any
Kafka configuration details.
"""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Producer

from config.settings import KAFKA_BOOTSTRAP_SERVERS
from kafka_app.topics import RAW_SOCIAL_POSTS_TOPIC


class KafkaPostProducer:
    """Send normalized post dictionaries to Kafka."""

    def __init__(self) -> None:
        self.topic = RAW_SOCIAL_POSTS_TOPIC
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self._producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        print(f"[KafkaPostProducer] Connected to Kafka at {self.bootstrap_servers}")

    def _delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            print(f"[KafkaPostProducer] Delivery failed for key={msg.key()!r}: {err}")
            return
        print(
            f"[KafkaPostProducer] Delivered to {msg.topic()}[{msg.partition()}] "
            f"offset={msg.offset()} key={msg.key()!r}"
        )

    def send_post(self, post: dict) -> None:
        """Send a single post payload to the raw Kafka topic."""

        try:
            payload = json.dumps(post, ensure_ascii=False).encode("utf-8")
            key = str(post.get("id", "")).encode("utf-8") if post.get("id") else None

            self._producer.produce(
                topic=self.topic,
                key=key,
                value=payload,
                on_delivery=self._delivery_report,
            )
            self._producer.poll(0)
            remaining = self._producer.flush(10)

            if remaining > 0:
                print(f"[KafkaPostProducer] Warning: {remaining} message(s) still pending after flush")
            else:
                print(f"[KafkaPostProducer] Sent post id={post.get('id')!r} to topic={self.topic}")
        except BufferError as exc:
            print(f"[KafkaPostProducer] Kafka buffer is full: {exc}")
            self._producer.flush(10)
        except Exception as exc:  # noqa: BLE001 - keep diagnostics explicit for step 2
            print(f"[KafkaPostProducer] Unexpected error while sending post: {exc}")
            raise

    def close(self) -> None:
        """Flush and release Kafka resources."""

        self._producer.flush(10)
        print("[KafkaPostProducer] Closed producer")
