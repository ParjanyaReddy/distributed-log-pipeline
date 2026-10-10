import math
from datetime import UTC, datetime
from typing import Any

from common.models import Event
from processor.detectors.base import BaseDetector


class ClientEwmaState:
    def __init__(self, alpha: float = 0.1, z_thresh: float = 3.0, warmup: int = 30):
        self.alpha = alpha
        self.z_thresh = z_thresh
        self.warmup = warmup
        self.mean = 0.0
        self.var = 0.0
        self.n = 0

    def evaluate_and_update(self, x: float) -> tuple[bool, float]:
        """Evaluates z-score against PREVIOUS mean and variance, then updates state (Rule R-10)."""
        is_anomaly = False
        z_score = 0.0

        if self.n == 0:
            self.mean = x
            self.var = 0.0
            self.n = 1
            return False, 0.0

        # Step 1: Compute z-score using previous distribution parameters
        std = math.sqrt(max(0.0, self.var))
        if self.n >= self.warmup and std > 1e-3:
            z_score = (x - self.mean) / std
            # We specifically look for high-side data volume spikes (exfiltration)
            if z_score > self.z_thresh:
                is_anomaly = True

        # Step 2: Update mean and variance using Welford's EWMA algorithm
        self.n += 1
        diff = x - self.mean
        incr = self.alpha * diff
        self.mean += incr
        self.var = (1.0 - self.alpha) * (self.var + diff * incr)

        return is_anomaly, z_score


class VolumeSpikeDetector(BaseDetector):
    def __init__(
        self,
        alpha: float = 0.1,
        z_thresh: float = 3.0,
        warmup: int = 30,
        bucket_seconds: float = 60.0,
        allowed_lateness_sec: float = 30.0
    ):
        super().__init__(name="volume_spike", allowed_lateness_sec=allowed_lateness_sec)
        self.alpha = alpha
        self.z_thresh = z_thresh
        self.warmup = warmup
        self.bucket_seconds = bucket_seconds

        # Per-client state: { client_id: ClientEwmaState }
        self.client_states: dict[str, ClientEwmaState] = {}
        self.alerted_buckets: set[str] = set()

    def process_event(self, event: Event) -> list[dict[str, Any]]:
        if self.is_late(event.ts):
            return []

        client_id = event.client_id
        if client_id not in self.client_states:
            self.client_states[client_id] = ClientEwmaState(
                alpha=self.alpha,
                z_thresh=self.z_thresh,
                warmup=self.warmup
            )

        state = self.client_states[client_id]
        is_spike, z_score = state.evaluate_and_update(float(event.bytes_out))

        alerts = []
        if is_spike:
            bucket_timestamp = int(event.ts.timestamp() // self.bucket_seconds) * int(self.bucket_seconds)
            bucket_iso = datetime.fromtimestamp(bucket_timestamp, tz=UTC).isoformat()
            dedupe_key = f"volume_spike|{client_id}|{bucket_iso}"

            if dedupe_key not in self.alerted_buckets:
                self.alerted_buckets.add(dedupe_key)
                severity = "critical" if z_score >= 5.0 else "high"
                alerts.append({
                    "ts": event.ts,
                    "type": "volume_spike",
                    "severity": severity,
                    "client_id": client_id,
                    "dedupe_key": dedupe_key,
                    "details": {
                        "bytes_out": event.bytes_out,
                        "mean": round(state.mean, 2),
                        "std": round(math.sqrt(max(0.0, state.var)), 2),
                        "z_score": round(z_score, 2),
                        "threshold": self.z_thresh
                    }
                })

        return alerts

    def get_state(self) -> dict[str, Any]:
        return {
            client_id: {
                "mean": state.mean,
                "var": state.var,
                "n": state.n,
                "alpha": state.alpha,
                "z_thresh": state.z_thresh,
                "warmup": state.warmup
            }
            for client_id, state in self.client_states.items()
        }

    def set_state(self, state_dict: dict[str, Any]) -> None:
        self.client_states.clear()
        for client_id, s in state_dict.items():
            state = ClientEwmaState(s["alpha"], s["z_thresh"], s["warmup"])
            state.mean = s["mean"]
            state.var = s["var"]
            state.n = s["n"]
            self.client_states[client_id] = state
