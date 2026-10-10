from collections import deque
from datetime import UTC, datetime, timedelta
from typing import Any

from common.models import Event
from processor.detectors.base import BaseDetector


class BruteForceDetector(BaseDetector):
    def __init__(
        self,
        failure_threshold: int = 5,
        window_seconds: float = 60.0,
        allowed_lateness_sec: float = 30.0
    ):
        super().__init__(name="brute_force", allowed_lateness_sec=allowed_lateness_sec)
        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds

        # In-memory state: { client_id: deque([(ts, event_id, auth_result)]) }
        self.client_auth_history: dict[str, deque[tuple[datetime, str, str]]] = {}
        # Track alerted windows to avoid duplicate alerts within the same window bucket
        self.alerted_buckets: set[str] = set()

    def process_event(self, event: Event) -> list[dict[str, Any]]:
        if self.is_late(event.ts):
            return []

        if event.auth_result not in ("failure", "success"):
            return []

        client_id = event.client_id
        if client_id not in self.client_auth_history:
            self.client_auth_history[client_id] = deque()

        history = self.client_auth_history[client_id]
        history.append((event.ts, str(event.event_id), event.auth_result))

        # Evict records outside the sliding window based on event time
        cutoff = event.ts - timedelta(seconds=self.window_seconds)
        while history and history[0][0] < cutoff:
            history.popleft()

        failures = [item for item in history if item[2] == "failure"]
        failure_count = len(failures)

        alerts = []
        if failure_count >= self.failure_threshold:
            # Deterministic window bucket (rounded down to window_seconds)
            bucket_timestamp = int(event.ts.timestamp() // self.window_seconds) * int(self.window_seconds)
            bucket_iso = datetime.fromtimestamp(bucket_timestamp, tz=UTC).isoformat()
            followed_by_success = (event.auth_result == "success")
            severity = "critical" if followed_by_success else "high"
            dedupe_suffix = "_compromise" if followed_by_success else ""
            dedupe_key = f"brute_force{dedupe_suffix}|{client_id}|{bucket_iso}"

            if dedupe_key not in self.alerted_buckets:
                self.alerted_buckets.add(dedupe_key)
                alert = {
                    "ts": event.ts,
                    "type": "brute_force",
                    "severity": severity,
                    "client_id": client_id,
                    "dedupe_key": dedupe_key,
                    "details": {
                        "failures": failure_count,
                        "window_s": int(self.window_seconds),
                        "threshold": self.failure_threshold,
                        "followed_by_success": followed_by_success
                    }
                }
                alerts.append(alert)

        return alerts

    def get_state(self) -> dict[str, Any]:
        return {
            client_id: [(ts.isoformat(), eid, auth) for ts, eid, auth in queue]
            for client_id, queue in self.client_auth_history.items()
        }

    def set_state(self, state: dict[str, Any]) -> None:
        self.client_auth_history.clear()
        for client_id, records in state.items():
            self.client_auth_history[client_id] = deque(
                (datetime.fromisoformat(ts), eid, auth)
                for ts, eid, auth in records
            )
