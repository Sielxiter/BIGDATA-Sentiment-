"""Debug consumer for verifying Kafka messages during Step 2."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import KAFKA_BOOTSTRAP_SERVERS  # noqa: E402
from kafka_app.topics import RAW_SOCIAL_POSTS_TOPIC  # noqa: E402


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "sentiment-debug-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([RAW_SOCIAL_POSTS_TOPIC])
    print(
        f"[consumer_debug] Listening on topic={RAW_SOCIAL_POSTS_TOPIC} "
        f"at {KAFKA_BOOTSTRAP_SERVERS}"
    )

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[consumer_debug] Consumer error: {message.error()}")
                continue

            raw_value = message.value().decode("utf-8") if message.value() else ""
            try:
                payload = json.loads(raw_value)
                pretty_value = json.dumps(payload, ensure_ascii=False)
            except json.JSONDecodeError:
                pretty_value = raw_value

            key = message.key().decode("utf-8") if message.key() else None
            print(
                f"[consumer_debug] topic={message.topic()} partition={message.partition()} "
                f"offset={message.offset()} key={key} value={pretty_value}"
            )
    except KeyboardInterrupt:
        print("[consumer_debug] Stopping consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
