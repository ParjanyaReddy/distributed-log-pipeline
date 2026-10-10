import math
from datetime import datetime, timezone
from typing import List, Dict, Any, Deque
from collections import deque

from common.models import Event
from processor.detectors.base import BaseDetector


class BeaconingDetector(BaseDetector):
    def __init__(
        self,
        min_samples: int = 6,
        cv_threshold: float = 0.15,
        min_interval_sec: float = 0.5,
        max_interval_sec: float = 300.0,
        bucket_seconds: float = 120.0,
        allowed_lateness_sec: float = 30.0
    ):
        super().__init__(name="beaconing", allowed_lateness_sec=allowed_lateness_sec)
        self.min_samples = min_samples
        self.cv_threshold = cv_threshold
        self.min_interval_sec = min_interval_sec
        self.max_interval_sec = max_interval_sec
        self.bucket_seconds = bucket_seconds

        # Map of (client_id, domain) -> deque of timestamps
        self.history: Dict[tuple[str, str], Deque[datetime]] = {}
        self.alerted_buckets: set[str] = set()

    def process_event(self, event: Event) -> List[Dict[str, Any]]:
        if self.is_late(event.ts):
            return []

        # C2 Beaconing monitors automated callbacks, not authentication bursts
        if event.category == "authentication" or event.action == "blocked":
            return []

        pair_key = (event.client_id, event.domain)
        if pair_key not in self.history:
            self.history[pair_key] = deque(maxlen=self.min_samples + 2)

        ts_deque = self.history[pair_key]
        ts_deque.append(event.ts)

        alerts = []
        if len(ts_deque) >= self.min_samples:
            # Calculate consecutive inter-arrival intervals
            intervals = [
                (ts_deque[i] - ts_deque[i - 1]).total_seconds()
                for i in range(1, len(ts_deque))
            ]

            # Inter-arrival intervals must be positive
            if all(it >= self.min_interval_sec for it in intervals):
                mean_interval = sum(intervals) / len(intervals)

                if self.min_interval_sec <= mean_interval <= self.max_interval_sec:
                    variance = sum((it - mean_interval) ** 2 for it in intervals) / len(intervals)
                    std_dev = math.sqrt(variance)
                    cv = std_dev / mean_interval if mean_interval > 0 else 1.0

                    if cv <= self.cv_threshold:
                        bucket_timestamp = int(event.ts.timestamp() // self.bucket_seconds) * int(self.bucket_seconds)
                        bucket_iso = datetime.fromtimestamp(bucket_timestamp, tz=timezone.utc).isoformat()
                        dedupe_key = f"beaconing|{event.client_id}|{event.domain}|{bucket_iso}"

                        if dedupe_key not in self.alerted_buckets:
                            self.alerted_buckets.add(dedupe_key)
                            alerts.append({
                                "ts": event.ts,
                                "type": "beaconing",
                                "severity": "high",
                                "client_id": event.client_id,
                                "dedupe_key": dedupe_key,
                                "details": {
                                    "domain": event.domain,
                                    "mean_interval_s": round(mean_interval, 2),
                                    "std_dev_s": round(std_dev, 2),
                                    "coefficient_of_variation": round(cv, 4),
                                    "cv_threshold": self.cv_threshold,
                                    "sample_count": len(intervals)
                                }
                            })

        return alerts

    def get_state(self) -> Dict[str, Any]:
        return {
            f"{cid}::{domain}": [ts.isoformat() for ts in dq]
            for (cid, domain), dq in self.history.items()
        }

    def set_state(self, state_dict: Dict[str, Any]) -> None:
        self.history.clear()
        for composite_key, ts_list in state_dict.items():
            cid, domain = composite_key.split("::", 1)
            self.history[(cid, domain)] = deque(
                [datetime.fromisoformat(ts) for ts in ts_list],
                maxlen=self.min_samples + 2
            )
