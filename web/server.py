import http.server
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import urlparse

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
from producers.profiles import create_population

logger = get_logger("web.server")

# In-memory buffer for live console display
MAX_LOG_HISTORY = 100
MAX_ALERT_HISTORY = 50


class WebPipelineBridge:
    def __init__(self):
        self.lock = threading.Lock()
        self.clients = create_population(num_clients=50, seed=42)
        self.engine = AnomalyEngine()
        self.sink = DatabaseSink(dry_run=True)
        self.dlq = DeadLetterQueue(dry_run=True)

        self.logs_buffer: List[Dict[str, Any]] = []
        self.alerts_buffer: List[Dict[str, Any]] = []

        self.streaming_active = True
        self.total_consumed = 0
        self.partition_counts = [0] * 6
        self.last_rate_time = time.time()
        self.recent_events_count = 0
        self.current_rate = 0.0

        # Background simulator thread
        self.worker_thread = threading.Thread(target=self._background_traffic_loop, daemon=True)
        self.worker_thread.start()

    def process_raw_event(self, raw_payload: bytes, partition: int = 0) -> None:
        with self.lock:
            self.total_consumed += 1
            self.recent_events_count += 1
            self.partition_counts[partition % 6] += 1

            try:
                event = validate_event_payload(raw_payload)
                event = enrich_event(event)

                # Anomaly detection
                alerts = self.engine.evaluate_event(event)
                for alert in alerts:
                    alert_copy = dict(alert)
                    alert_copy["ts"] = alert["ts"].isoformat() if isinstance(alert["ts"], datetime) else alert["ts"]
                    self.alerts_buffer.insert(0, alert_copy)
                    if len(self.alerts_buffer) > MAX_ALERT_HISTORY:
                        self.alerts_buffer.pop()

                # Add to logs buffer
                ev_dict = event.model_dump(mode="json")
                ev_dict["ts"] = event.ts.isoformat()
                ev_dict["partition"] = partition % 6
                self.logs_buffer.insert(0, ev_dict)
                if len(self.logs_buffer) > MAX_LOG_HISTORY:
                    self.logs_buffer.pop()

                # Batch write to sink
                self.sink.write_batch([event], alerts=alerts)

            except Exception as e:
                self.dlq.send_to_dlq(
                    raw_payload=raw_payload,
                    reason="validation_error",
                    topic="raw-logs",
                    partition=partition % 6,
                    offset=self.total_consumed,
                    details=str(e),
                )

    def trigger_anomaly(self, anomaly_type: str) -> Dict[str, Any]:
        target_client = self.clients[int(time.time()) % len(self.clients)]
        now = datetime.now(timezone.utc)

        if anomaly_type == "brute_force":
            events, label = inject_brute_force(target_client, now, num_failures=8)
            for ev in events:
                self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=hash(target_client.client_id) % 6)
            return {"status": "triggered", "type": "brute_force", "client_id": target_client.client_id, "label": label}

        elif anomaly_type == "volume_spike":
            ev, label = inject_volume_spike(target_client, now, bytes_out=85_000_000)
            self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=hash(target_client.client_id) % 6)
            return {"status": "triggered", "type": "volume_spike", "client_id": target_client.client_id, "label": label}

        elif anomaly_type == "rare_domain":
            ev, label = inject_rare_domain(target_client, now)
            self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=hash(target_client.client_id) % 6)
            return {"status": "triggered", "type": "rare_domain", "client_id": target_client.client_id, "label": label}

        elif anomaly_type == "beaconing":
            events, label = inject_beaconing(target_client, now, count=8, interval_seconds=3.0)
            for ev in events:
                self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=hash(target_client.client_id) % 6)
            return {"status": "triggered", "type": "beaconing", "client_id": target_client.client_id, "label": label}

        return {"status": "error", "message": f"Unknown anomaly type: {anomaly_type}"}

    def _background_traffic_loop(self) -> None:
        idx = 0
        while True:
            if self.streaming_active:
                client = self.clients[idx % len(self.clients)]
                idx += 1
                ev = client.generate_event()
                partition = hash(client.client_id) % 6
                self.process_raw_event(json.dumps(ev).encode("utf-8"), partition=partition)

            # Update rate meter every second
            now = time.time()
            if now - self.last_rate_time >= 1.0:
                elapsed = now - self.last_rate_time
                self.current_rate = round(self.recent_events_count / elapsed, 1)
                self.recent_events_count = 0
                self.last_rate_time = now

            time.sleep(0.08)  # ~12 events per second for steady live animation


bridge = WebPipelineBridge()


class PipelineRequestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        super().__init__(*args, directory=static_dir, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/stats":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            with bridge.lock:
                data = {
                    "rate_ops": bridge.current_rate,
                    "total_consumed": bridge.total_consumed,
                    "total_stored": len(bridge.sink.in_memory_events),
                    "total_dlq": len(bridge.dlq.in_memory_records),
                    "total_alerts": len(bridge.alerts_buffer),
                    "partition_counts": bridge.partition_counts,
                    "streaming_active": bridge.streaming_active,
                }
            self.wfile.write(json.dumps(data).encode("utf-8"))

        elif parsed.path == "/api/logs":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            with bridge.lock:
                logs = list(bridge.logs_buffer[:50])
            self.wfile.write(json.dumps(logs, default=str).encode("utf-8"))

        elif parsed.path == "/api/alerts":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            with bridge.lock:
                alerts = list(bridge.alerts_buffer[:30])
            self.wfile.write(json.dumps(alerts, default=str).encode("utf-8"))

        else:
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        try:
            req_data = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            req_data = {}

        if parsed.path == "/api/simulate/attack":
            anomaly_type = req_data.get("type", "brute_force")
            result = bridge.trigger_anomaly(anomaly_type)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))

        elif parsed.path == "/api/traffic/toggle":
            with bridge.lock:
                bridge.streaming_active = not bridge.streaming_active
                status = bridge.streaming_active
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"streaming_active": status}).encode("utf-8"))

        else:
            self.send_response(404)
            self.end_headers()


def start_server(port: int = 8080):
    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), PipelineRequestHandler)
    logger.info(f"Pipeline SOC Console running live at http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Web server stopping...")
    finally:
        server.server_close()


if __name__ == "__main__":
    start_server(8080)
