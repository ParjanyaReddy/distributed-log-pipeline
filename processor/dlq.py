import base64
import json
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import KafkaError, Producer

from common.config import KAFKA_BROKERS, TOPIC_DLQ
from common.logging import get_logger
from common.metrics import DLQ_TOTAL

logger = get_logger("processor.dlq")


class DeadLetterQueue:
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BROKERS,
        dlq_topic: str = TOPIC_DLQ,
        dry_run: bool = False
    ):
        self.dlq_topic = dlq_topic
        self.dry_run = dry_run
        self.producer: Producer | None = None
        self.in_memory_records: list[dict[str, Any]] = []

        if not self.dry_run:
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 10,
            }
            try:
                self.producer = Producer(conf)
            except Exception as e:
                logger.error(f"Failed to create DLQ producer: {e}")
                raise

    def send_to_dlq(
        self,
        raw_payload: bytes,
        reason: str,
        topic: str,
        partition: int,
        offset: int,
        details: str = ""
    ) -> None:
        """Packages rejected messages with metadata into the dead-letter topic."""
        # Increment metric
        DLQ_TOTAL.labels(reason=reason).inc()

        payload_b64 = base64.b64encode(raw_payload).decode("ascii")
        record = {
            "payload_b64": payload_b64,
            "reason": reason,
            "details": details,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "ts": datetime.now(UTC).isoformat()
        }

        key = f"{topic}-{partition}-{offset}"
        val_bytes = json.dumps(record).encode("utf-8")

        if self.dry_run or self.producer is None:
            self.in_memory_records.append(record)
            return

        def dlq_delivery_callback(err: KafkaError | None, msg: Any) -> None:
            if err is not None:
                logger.error(f"Failed to deliver DLQ message for key {key}: {err}")

        self.producer.produce(
            self.dlq_topic,
            key=key.encode("utf-8"),
            value=val_bytes,
            on_delivery=dlq_delivery_callback
        )
        self.producer.poll(0)

    def flush(self, timeout: float = 5.0) -> int:
        """Flushes buffered DLQ messages to ensure durable acknowledgment."""
        if self.dry_run or self.producer is None:
            return 0
        return self.producer.flush(timeout=timeout)
