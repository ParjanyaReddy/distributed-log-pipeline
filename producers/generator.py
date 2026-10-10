import argparse
import json
import signal
import time
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import KafkaError, Producer

from common.config import (
    KAFKA_BROKERS,
    LABELS_PATH,
    NUM_CLIENTS,
    PRODUCER_RATE,
    RANDOM_SEED,
    TOPIC_RAW_LOGS,
)
from common.logging import get_logger
from producers.anomalies import (
    inject_beaconing,
    inject_brute_force,
    inject_rare_domain,
    inject_volume_spike,
    make_malformed_event,
)
from producers.profiles import SimulatedClient, create_population

logger = get_logger("producer.generator")


class LogGenerator:
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BROKERS,
        topic: str = TOPIC_RAW_LOGS,
        target_rate: float = PRODUCER_RATE,
        num_clients: int = NUM_CLIENTS,
        labels_file: str = LABELS_PATH,
        seed: int = RANDOM_SEED,
        malformed_ratio: float = 0.0,
        anomaly_interval_sec: float = 30.0,
        dry_run: bool = False
    ):
        self.topic = topic
        self.target_rate = target_rate
        self.num_clients = num_clients
        self.labels_file = labels_file
        self.malformed_ratio = malformed_ratio
        self.anomaly_interval_sec = anomaly_interval_sec
        self.dry_run = dry_run
        self.running = True

        # Metrics
        self.sent_count = 0
        self.delivered_count = 0
        self.failure_count = 0

        # Initialize clients
        self.clients: list[SimulatedClient] = create_population(num_clients=num_clients, seed=seed)

        # Setup Kafka Producer if not dry run
        self.producer: Producer | None = None
        if not self.dry_run:
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 20,
                "batch.size": 64 * 1024,
                "compression.type": "snappy",
            }
            try:
                self.producer = Producer(conf)
            except Exception as e:
                logger.error(f"Failed to create Kafka producer: {e}")
                raise

    def delivery_report(self, err: KafkaError | None, msg: Any) -> None:
        if err is not None:
            self.failure_count += 1
            logger.error(f"Message delivery failed: {err}")
        else:
            self.delivered_count += 1

    def send_event(self, key: str, value: Any) -> None:
        raw_val = value if isinstance(value, str) else json.dumps(value)
        self.sent_count += 1

        if self.dry_run or self.producer is None:
            self.delivered_count += 1
            return

        self.producer.produce(
            self.topic,
            key=key.encode("utf-8"),
            value=raw_val.encode("utf-8"),
            on_delivery=self.delivery_report
        )
        # Service callbacks periodically without blocking
        self.producer.poll(0)

    def record_label(self, label: dict[str, Any]) -> None:
        with open(self.labels_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(label) + "\n")

    def run(self, duration_sec: float | None = None) -> None:
        logger.info(f"Starting log generator: target_rate={self.target_rate}/s, clients={self.num_clients}, dry_run={self.dry_run}")
        start_time = time.time()
        last_report_time = start_time
        last_anomaly_time = start_time
        anomaly_index = 0

        client_cycle_idx = 0
        total_clients = len(self.clients)

        interval = 1.0 / self.target_rate if self.target_rate > 0 else 0.001
        next_send_time = time.time()

        while self.running:
            now = time.time()
            if duration_sec is not None and (now - start_time) >= duration_sec:
                break

            # Check if it's time to inject an anomaly
            if self.anomaly_interval_sec > 0 and (now - last_anomaly_time) >= self.anomaly_interval_sec:
                last_anomaly_time = now
                target_client = self.clients[anomaly_index % total_clients]
                current_dt = datetime.now(UTC)

                if anomaly_index % 4 == 0:
                    events, label = inject_brute_force(target_client, current_dt)
                    for ev in events:
                        self.send_event(key=target_client.client_id, value=ev)
                    self.record_label(label)
                    logger.info(f"Injected brute_force anomaly for {target_client.client_id}")
                elif anomaly_index % 4 == 1:
                    ev, label = inject_volume_spike(target_client, current_dt)
                    self.send_event(key=target_client.client_id, value=ev)
                    self.record_label(label)
                    logger.info(f"Injected volume_spike anomaly for {target_client.client_id}")
                elif anomaly_index % 4 == 2:
                    ev, label = inject_rare_domain(target_client, current_dt)
                    self.send_event(key=target_client.client_id, value=ev)
                    self.record_label(label)
                    logger.info(f"Injected rare_domain anomaly for {target_client.client_id}")
                else:
                    events, label = inject_beaconing(target_client, current_dt)
                    for ev in events:
                        self.send_event(key=target_client.client_id, value=ev)
                    self.record_label(label)
                    logger.info(f"Injected beaconing anomaly for {target_client.client_id}")

                anomaly_index += 1

            # Normal event generation
            client = self.clients[client_cycle_idx]
            client_cycle_idx = (client_cycle_idx + 1) % total_clients

            # Check for malformed message injection
            if self.malformed_ratio > 0 and (client.rng.random() < self.malformed_ratio):
                malformed_payload = make_malformed_event("status_999")
                self.send_event(key=client.client_id, value=malformed_payload)
            else:
                event_data = client.generate_event()
                self.send_event(key=client.client_id, value=event_data)

            # Rate limiting / pacing
            next_send_time += interval
            sleep_duration = next_send_time - time.time()
            if sleep_duration > 0:
                time.sleep(sleep_duration)
            elif sleep_duration < -0.1:
                # Catch up without unbounded drift
                next_send_time = time.time()

            # Periodic stats logging
            if now - last_report_time >= 5.0:
                achieved_rate = self.sent_count / (now - start_time)
                logger.info(
                    f"Stats: sent={self.sent_count}, delivered={self.delivered_count}, "
                    f"failed={self.failure_count}, avg_rate={achieved_rate:.1f} ev/s"
                )
                last_report_time = now

        self.shutdown()

    def shutdown(self) -> None:
        logger.info("Flushing producer buffers and shutting down...")
        if self.producer is not None:
            self.producer.flush(timeout=10.0)
        logger.info(
            f"Producer shutdown complete: total_sent={self.sent_count}, "
            f"delivered={self.delivered_count}, failed={self.failure_count}"
        )


def main():
    parser = argparse.ArgumentParser(description="Distributed Log Pipeline Simulated Producer")
    parser.add_argument("--rate", type=float, default=PRODUCER_RATE, help="Target events per second")
    parser.add_argument("--duration", type=float, default=None, help="Run duration in seconds")
    parser.add_argument("--clients", type=int, default=NUM_CLIENTS, help="Number of simulated clients")
    parser.add_argument("--anomaly-interval", type=float, default=30.0, help="Interval between anomalies (s)")
    parser.add_argument("--malformed-ratio", type=float, default=0.0, help="Ratio of malformed events (0.0 to 1.0)")
    parser.add_argument("--dry-run", action="store_true", help="Generate events without sending to Kafka")
    args = parser.parse_args()

    generator = LogGenerator(
        target_rate=args.rate,
        num_clients=args.clients,
        anomaly_interval_sec=args.anomaly_interval,
        malformed_ratio=args.malformed_ratio,
        dry_run=args.dry_run
    )

    def handle_signal(sig, frame):
        logger.info(f"Signal {sig} received, stopping generator gracefully...")
        generator.running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    generator.run(duration_sec=args.duration)


if __name__ == "__main__":
    main()
