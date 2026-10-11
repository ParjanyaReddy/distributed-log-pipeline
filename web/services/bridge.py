"""Pipeline Bridge Service.

Encapsulates stream simulation, log buffering, anomaly triggers, and
partition metrics aggregation away from HTTP controllers and UI rendering.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

from common.logging import get_logger
from processor.detectors.engine import AnomalyEngine
from processor.dlq import DeadLetterQueue
from processor.enrich import enrich_event
from processor.sink import DatabaseSink
from processor.validate import validate_event_payload
from producers.anomalies import (
    inject_beaconing,
    inject_brute_force,
    inject_rare_domain,
    inject_volume_spike,
)
from producers.profiles import SimulatedClient, create_population

logger = get_logger("web.services.bridge")

MAX_LOG_HISTORY = 100
MAX_ALERT_HISTORY = 50
DEFAULT_PARTITION_COUNT = 6


class WebPipelineBridge:
    """Thread-safe stateful business logic bridge between the processing engine and web telemetry."""

    def __init__(self, num_clients: int = 50, seed: int = 42) -> None:
        self.lock = threading.Lock()
        self.clients: list[SimulatedClient] = create_population(num_clients=num_clients, seed=seed)
        self.engine = AnomalyEngine()
        self.sink = DatabaseSink(dry_run=True)
        self.dlq = DeadLetterQueue(dry_run=True)

        self.logs_buffer: list[dict[str, Any]] = []
        self.alerts_buffer: list[dict[str, Any]] = []

        self.streaming_active = True
        self.total_consumed = 0
        self.partition_counts = [0] * DEFAULT_PARTITION_COUNT
        self.last_rate_time = time.time()
        self.recent_events_count = 0
        self.current_rate = 0.0

        # Background simulator thread
        self.worker_thread = threading.Thread(target=self._background_traffic_loop, daemon=True)
        self.worker_thread.start()
        logger.info("WebPipelineBridge initialized and background generator started.")

    def process_raw_event(self, raw_payload: bytes, partition: int = 0) -> None:
        """Processes a single raw event: validation, enrichment, anomaly detection, buffering, sink write."""
        with self.lock:
            self.total_consumed += 1
            self.recent_events_count += 1
            shard_idx = partition % DEFAULT_PARTITION_COUNT
            self.partition_counts[shard_idx] += 1

            try:
                event = validate_event_payload(raw_payload)
                event = enrich_event(event)

                # Anomaly detection evaluation
                alerts = self.engine.evaluate_event(event)
                for alert in alerts:
                    alert_copy = dict(alert)
                    alert_copy["ts"] = alert["ts"].isoformat() if isinstance(alert["ts"], datetime) else alert["ts"]
                    self.alerts_buffer.insert(0, alert_copy)
                    if len(self.alerts_buffer) > MAX_ALERT_HISTORY:
                        self.alerts_buffer.pop()

                # Add to logs ring buffer
                ev_dict = event.model_dump(mode="json")
                ev_dict["ts"] = event.ts.isoformat()
                ev_dict["partition"] = shard_idx
                self.logs_buffer.insert(0, ev_dict)
                if len(self.logs_buffer) > MAX_LOG_HISTORY:
                    self.logs_buffer.pop()

                # Write batch to sink
                self.sink.write_batch([event], alerts=alerts)

            except Exception as exc:
                self.dlq.send_to_dlq(
                    raw_payload=raw_payload,
                    reason="validation_error",
                    topic="raw-logs",
                    partition=shard_idx,
                    offset=self.total_consumed,
                    details=str(exc),
                )

    def trigger_anomaly(self, anomaly_type: str) -> dict[str, Any]:
        """Injects a specific MITRE ATT&CK attack scenario into the stream pipeline."""
        if not self.clients:
            return {"status": "error", "message": "No simulated clients available"}

        target_client = self.clients[int(time.time()) % len(self.clients)]
        now = datetime.now(timezone.utc)
        shard = hash(target_client.client_id) % DEFAULT_PARTITION_COUNT

        try:
            if anomaly_type == "brute_force":
                events, label = inject_brute_force(target_client, now, num_failures=8)
                for ev in events:
                    self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=shard)
                return {
                    "status": "triggered",
                    "type": "brute_force",
                    "client_id": target_client.client_id,
                    "label": label,
                }

            if anomaly_type == "volume_spike":
                ev, label = inject_volume_spike(target_client, now, bytes_out=85_000_000)
                self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=shard)
                return {
                    "status": "triggered",
                    "type": "volume_spike",
                    "client_id": target_client.client_id,
                    "label": label,
                }

            if anomaly_type == "rare_domain":
                ev, label = inject_rare_domain(target_client, now)
                self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=shard)
                return {
                    "status": "triggered",
                    "type": "rare_domain",
                    "client_id": target_client.client_id,
                    "label": label,
                }

            if anomaly_type == "beaconing":
                events, label = inject_beaconing(target_client, now, count=8, interval_seconds=3.0)
                for ev in events:
                    self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=shard)
                return {
                    "status": "triggered",
                    "type": "beaconing",
                    "client_id": target_client.client_id,
                    "label": label,
                }

            return {"status": "error", "message": f"Unknown anomaly type: {anomaly_type}"}

        except Exception as exc:
            logger.error(f"Failed to inject anomaly '{anomaly_type}': {exc}", exc_info=True)
            return {"status": "error", "message": f"Failed to inject anomaly: {exc}"}

    def toggle_streaming(self) -> bool:
        """Toggles real-time streaming traffic on/off."""
        with self.lock:
            self.streaming_active = not self.streaming_active
            return self.streaming_active

    def get_stats_snapshot(self) -> dict[str, Any]:
        """Produces a thread-safe snapshot of ingestion metrics, cluster health, and partition distribution."""
        with self.lock:
            total_ev = max(self.total_consumed, 1)
            partition_details = [
                {
                    "partition_id": i,
                    "leader": f"broker-{(i % 3) + 1}",
                    "isr": "3/3",
                    "high_watermark": self.partition_counts[i],
                    "count": self.partition_counts[i],
                    "share_pct": round((self.partition_counts[i] / total_ev) * 100, 1),
                    "status": "HEALTHY",
                }
                for i in range(DEFAULT_PARTITION_COUNT)
            ]
            return {
                "rate_ops": self.current_rate,
                "total_consumed": self.total_consumed,
                "total_stored": len(self.sink.in_memory_events),
                "total_dlq": len(self.dlq.in_memory_records),
                "total_alerts": len(self.alerts_buffer),
                "partition_counts": list(self.partition_counts),
                "partition_details": partition_details,
                "streaming_active": self.streaming_active,
                "cluster_info": {
                    "cluster_id": "kraft-cluster-01",
                    "brokers_online": 3,
                    "consumer_group": "cg-log-processors",
                    "consumer_state": "STABLE",
                    "p95_latency_ms": 4.2 if self.current_rate > 0 else 0.0,
                    "consumer_lag": 0,
                },
            }

    def get_recent_logs(self, limit: int = 50) -> list[dict[str, Any]]:
        """Retrieves recent logs snapshot from the in-memory ring buffer."""
        with self.lock:
            return list(self.logs_buffer[:limit])

    def get_recent_alerts(self, limit: int = 30) -> list[dict[str, Any]]:
        """Retrieves recent anomaly alerts snapshot from the in-memory ring buffer."""
        with self.lock:
            return list(self.alerts_buffer[:limit])

    def _background_traffic_loop(self) -> None:
        """Internal generator loop that produces steady synthetic traffic."""
        idx = 0
        while True:
            try:
                if self.streaming_active and self.clients:
                    client = self.clients[idx % len(self.clients)]
                    idx += 1
                    ev = client.generate_event()
                    partition = hash(client.client_id) % DEFAULT_PARTITION_COUNT
                    self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=partition)

                now = time.time()
                if now - self.last_rate_time >= 1.0:
                    elapsed = now - self.last_rate_time
                    self.current_rate = round(self.recent_events_count / elapsed, 1)
                    self.recent_events_count = 0
                    self.last_rate_time = now

                time.sleep(0.08)  # ~12 events per second for steady live animation
            except Exception as exc:
                logger.error(f"Error in background traffic loop: {exc}", exc_info=True)
                time.sleep(1.0)
