"""Kafka publishing helpers for Lambda and cloud-side code."""

from __future__ import annotations

import json
import os
from typing import Any

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import NewTopic
    from confluent_kafka.admin import AdminClient
except ImportError as exc:  # pragma: no cover - runtime dependency issue
    AdminClient = None
    Producer = None
    _CONFLUENT_IMPORT_ERROR = exc
else:
    _CONFLUENT_IMPORT_ERROR = None


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def build_kafka_config(*, bootstrap_servers: str | None = None) -> dict[str, Any]:
    """Build a Kafka producer config from environment variables."""

    bootstrap = (bootstrap_servers or _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")).strip()
    if not bootstrap:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is missing.")

    config: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "client.id": _env("KAFKA_CLIENT_ID", "youtube-collector-lambda"),
    }

    security_protocol = _env("KAFKA_SECURITY_PROTOCOL", "")
    if security_protocol:
        config["security.protocol"] = security_protocol

    sasl_mechanism = _env("KAFKA_SASL_MECHANISM", "")
    sasl_username = _env("KAFKA_SASL_USERNAME", "")
    sasl_password = _env("KAFKA_SASL_PASSWORD", "")
    if security_protocol.upper().startswith("SASL"):
        if sasl_mechanism:
            config["sasl.mechanisms"] = sasl_mechanism
        if sasl_username:
            config["sasl.username"] = sasl_username
        if sasl_password:
            config["sasl.password"] = sasl_password

    ssl_ca_location = _env("KAFKA_SSL_CA_LOCATION", "")
    if ssl_ca_location:
        config["ssl.ca.location"] = ssl_ca_location

    return config


class KafkaJsonPublisher:
    """Publish normalized JSON payloads to a Kafka topic."""

    def __init__(self, *, topic: str | None = None, bootstrap_servers: str | None = None) -> None:
        if Producer is None:
            raise RuntimeError(
                "confluent-kafka is not installed in this runtime. "
                "Package it into the Lambda deployment artifact or container image."
            ) from _CONFLUENT_IMPORT_ERROR

        self.topic = (topic or _env("KAFKA_RAW_TOPIC", "raw_social_posts")).strip() or "raw_social_posts"
        self.bootstrap_servers = (bootstrap_servers or _env("KAFKA_BOOTSTRAP_SERVERS", "")).strip()
        self._producer = Producer(build_kafka_config(bootstrap_servers=self.bootstrap_servers))

    def _delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            print(f"[KafkaJsonPublisher] Delivery failed for key={msg.key()!r}: {err}")
            return
        print(
            f"[KafkaJsonPublisher] Delivered to {msg.topic()}[{msg.partition()}] "
            f"offset={msg.offset()} key={msg.key()!r}"
        )

    def publish(self, record: dict) -> None:
        payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
        key = str(record.get("id", "")).encode("utf-8") if record.get("id") else None
        self._producer.produce(
            topic=self.topic,
            key=key,
            value=payload,
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def publish_batch(self, records: list[dict]) -> int:
        published = 0
        for record in records:
            self.publish(record)
            published += 1
        remaining = self._producer.flush(10)
        if remaining > 0:
            print(f"[KafkaJsonPublisher] Warning: {remaining} message(s) still pending after flush")
        return published

    def close(self) -> None:
        self._producer.flush(10)
        print("[KafkaJsonPublisher] Closed producer")


def ensure_kafka_topics(
    topic_names: list[str] | tuple[str, ...],
    *,
    bootstrap_servers: str | None = None,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    """Ensure the requested Kafka topics exist.

    This is used by the local Spark workflow to avoid the common startup
    failure where Spark subscribes to a topic before anything has produced to
    it yet. With Dockerized Kafka, auto-creation is not always enough because
    the consumer can hit the broker before the topic metadata exists.
    """

    if AdminClient is None:
        print(
            "[KafkaJsonPublisher] Kafka admin client is not installed in this runtime. "
            "Skipping topic auto-creation. Create the topics with docker compose or "
            "install confluent-kafka in the active environment if you want Python-based "
            "topic management."
        )
        return

    topics = [str(topic).strip() for topic in topic_names if str(topic).strip()]
    if not topics:
        print("[KafkaJsonPublisher] No Kafka topics requested for creation")
        return

    bootstrap = (bootstrap_servers or _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")).strip()
    if not bootstrap:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is missing.")

    admin_client = AdminClient({"bootstrap.servers": bootstrap})
    new_topics = [
        NewTopic(
            topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        for topic in topics
    ]

    futures = admin_client.create_topics(new_topics, request_timeout=10)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[KafkaJsonPublisher] Created topic: {topic}")
        except Exception as exc:  # pragma: no cover - depends on broker state
            message = str(exc)
            if "TOPIC_ALREADY_EXISTS" in message or "TopicAlreadyExists" in message:
                print(f"[KafkaJsonPublisher] Topic already exists: {topic}")
                continue
            print(f"[KafkaJsonPublisher] Failed to create topic {topic}: {exc}")
