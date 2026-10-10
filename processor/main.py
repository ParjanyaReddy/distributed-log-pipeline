import argparse
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable, Any

from confluent_kafka import Consumer, KafkaError, TopicPartition
from prometheus_client import start_http_server

from common.config import (
    KAFKA_BROKERS,
    TOPIC_RAW_LOGS,
    MAX_BATCH_SIZE,
    BATCH_TIMEOUT_SEC
)
from common.logging import get_logger
from common.metrics import (
    EVENTS_CONSUMED,
    EVENTS_WRITTEN,
    PROCESSING_LATENCY_SECONDS
)
from common.models import Event, ValidationFailure
from processor.validate import validate_event_payload
from processor.dlq import DeadLetterQueue

logger = get_logger("processor.main")


class LogProcessor:
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BROKERS,
        group_id: str = "processors",
        input_topic: str = TOPIC_RAW_LOGS,
        max_batch_size: int = MAX_BATCH_SIZE,
        batch_timeout_sec: float = BATCH_TIMEOUT_SEC,
        dlq: Optional[DeadLetterQueue] = None,
        sink_fn: Optional[Callable[[List[Event]], None]] = None,
        dry_run: bool = False
    ):
        self.input_topic = input_topic
        self.max_batch_size = max_batch_size
        self.batch_timeout_sec = batch_timeout_sec
        self.dry_run = dry_run
        self.dlq = dlq or DeadLetterQueue(bootstrap_servers=bootstrap_servers, dry_run=dry_run)
        self.sink_fn = sink_fn or self._default_in_memory_sink

        self.running = True
        self.batch: List[Event] = []
        self.in_memory_sink: List[Event] = []
        self.batch_start_time = time.time()

        # Track the next offset to commit per partition: { (topic, partition): offset + 1 }
        self.pending_offsets: Dict[tuple, int] = {}
        self.committed_offsets: Dict[tuple, int] = {}

        self.consumer: Optional[Consumer] = None
        if not self.dry_run:
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "max.poll.interval.ms": 300000,
            }
            try:
                self.consumer = Consumer(conf)
            except Exception as e:
                logger.error(f"Failed to create Kafka consumer: {e}")
                raise

    def _default_in_memory_sink(self, events: List[Event]) -> None:
        """Default sink in Phase 3 before Phase 4 TimescaleDB integration."""
        self.in_memory_sink.extend(events)
        EVENTS_WRITTEN.inc(len(events))

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(f"Partitions assigned: {[f'{p.topic}-{p.partition}' for p in partitions]}")
        consumer.assign(partitions)

    def on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(f"Partitions revoked: {[f'{p.topic}-{p.partition}' for p in partitions]}")
        # Flush pending work before releasing partition ownership
        self.flush_batch(force=True)
        # Clear partition state for revoked partitions
        revoked_keys = {(p.topic, p.partition) for p in partitions}
        self.pending_offsets = {k: v for k, v in self.pending_offsets.items() if k not in revoked_keys}
        consumer.unassign()

    def track_offset(self, topic: str, partition: int, offset: int) -> None:
        """Tracks the highest offset + 1 for committing."""
        self.pending_offsets[(topic, partition)] = offset + 1

    def flush_batch(self, force: bool = False) -> None:
        """Flushes buffered events strictly according to the processing contract."""
        age = time.time() - self.batch_start_time
        if not force and len(self.batch) < self.max_batch_size and age < self.batch_timeout_sec:
            return

        if not self.batch and not self.pending_offsets:
            self.batch_start_time = time.time()
            return

        # Step 1: Ensure all DLQ messages are acknowledged
        self.dlq.flush(timeout=5.0)

        # Step 2: Write batch to storage sink
        if self.batch:
            self.sink_fn(self.batch)
            self.batch.clear()

        # Step 3: Explicitly commit offsets (including offsets of DLQ messages)
        if self.consumer is not None and self.pending_offsets:
            topic_partitions = [
                TopicPartition(topic, partition, next_offset)
                for (topic, partition), next_offset in self.pending_offsets.items()
            ]
            try:
                self.consumer.commit(offsets=topic_partitions, asynchronous=False)
                self.committed_offsets.update(self.pending_offsets)
                self.pending_offsets.clear()
            except Exception as e:
                logger.error(f"Failed to commit Kafka offsets: {e}")
                raise
        elif self.dry_run:
            self.committed_offsets.update(self.pending_offsets)
            self.pending_offsets.clear()

        self.batch_start_time = time.time()

    def process_message(self, raw_payload: bytes, topic: str, partition: int, offset: int) -> None:
        """Validates and processes a single consumed message."""
        EVENTS_CONSUMED.inc()
        try:
            event = validate_event_payload(raw_payload)
            # Record processing latency from event timestamp
            latency = (datetime.now(timezone.utc) - event.ts).total_seconds()
            if latency >= 0:
                PROCESSING_LATENCY_SECONDS.observe(latency)
            self.batch.append(event)
        except ValidationFailure as vf:
            logger.warning(f"Validation failure at {topic}:{partition}:{offset} - reason: {vf.reason_code.value}")
            self.dlq.send_to_dlq(
                raw_payload=raw_payload,
                reason=vf.reason_code.value,
                topic=topic,
                partition=partition,
                offset=offset,
                details=vf.details
            )

        # R-5: Always advance offset for processed message (whether valid or sent to DLQ)
        self.track_offset(topic, partition, offset)
        self.flush_batch()

    def run(self) -> None:
        if self.consumer is None:
            logger.error("Consumer not initialized. Cannot run.")
            return

        self.consumer.subscribe(
            [self.input_topic],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke
        )
        logger.info(f"Subscribed to {self.input_topic}. Starting consumer loop...")

        while self.running:
            msg = self.consumer.poll(timeout=0.5)
            if msg is None:
                self.flush_batch()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka consumer error: {msg.error()}")
                continue

            self.process_message(
                raw_payload=msg.value(),
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset()
            )

        self.shutdown()

    def shutdown(self) -> None:
        logger.info("Graceful shutdown: flushing final batch and committing offsets...")
        try:
            self.flush_batch(force=True)
        except Exception as e:
            logger.error(f"Error flushing during shutdown: {e}")
        finally:
            if self.consumer is not None:
                self.consumer.close()
                logger.info("Kafka consumer closed.")


def main():
    parser = argparse.ArgumentParser(description="Distributed Log Processor Consumer")
    parser.add_argument("--metrics-port", type=int, default=8000, help="Port for Prometheus metrics HTTP server")
    args = parser.parse_args()

    # Start Prometheus metrics exporter
    try:
        start_http_server(args.metrics_port)
        logger.info(f"Prometheus metrics exposed on port {args.metrics_port}")
    except Exception as e:
        logger.warn(f"Could not start Prometheus HTTP server: {e}")

    processor = LogProcessor()

    def handle_signal(sig, frame):
        logger.info(f"Signal {sig} received. Initiating graceful shutdown...")
        processor.running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    processor.run()


if __name__ == "__main__":
    main()
