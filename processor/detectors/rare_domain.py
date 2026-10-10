from datetime import datetime, timezone
from typing import List, Dict, Any, Set

from common.models import Event
from processor.detectors.base import BaseDetector

SUSPICIOUS_CATEGORIES = {"suspicious", "c2_callback", "unapproved_storage", "unknown"}


class RareDomainDetector(BaseDetector):
    def __init__(
        self,
        min_bytes_out: int = 50_000,
        bucket_seconds: float = 300.0,
        allowed_lateness_sec: float = 30.0
    ):
        super().__init__(name="rare_domain", allowed_lateness_sec=allowed_lateness_sec)
        self.min_bytes_out = min_bytes_out
        self.bucket_seconds = bucket_seconds

        # Per-client seen domains: { client_id: set(domains) }
        self.client_domains: Dict[str, Set[str]] = {}
        self.alerted_buckets: Set[str] = set()

    def process_event(self, event: Event) -> List[Dict[str, Any]]:
        if self.is_late(event.ts):
            return []

        client_id = event.client_id
        if client_id not in self.client_domains:
            self.client_domains[client_id] = set()

        seen_set = self.client_domains[client_id]
        is_first_seen = event.domain not in seen_set
        seen_set.add(event.domain)

        alerts = []
        is_suspicious_category = event.category in SUSPICIOUS_CATEGORIES
        elevated_bytes = event.bytes_out >= self.min_bytes_out

        if (is_first_seen or is_suspicious_category) and elevated_bytes:
            bucket_timestamp = int(event.ts.timestamp() // self.bucket_seconds) * int(self.bucket_seconds)
            bucket_iso = datetime.fromtimestamp(bucket_timestamp, tz=timezone.utc).isoformat()
            dedupe_key = f"rare_domain|{client_id}|{event.domain}|{bucket_iso}"

            if dedupe_key not in self.alerted_buckets:
                self.alerted_buckets.add(dedupe_key)
                severity = "critical" if is_suspicious_category and event.bytes_out >= 100_000 else "high"
                alerts.append({
                    "ts": event.ts,
                    "type": "rare_domain",
                    "severity": severity,
                    "client_id": client_id,
                    "dedupe_key": dedupe_key,
                    "details": {
                        "domain": event.domain,
                        "category": event.category,
                        "bytes_out": event.bytes_out,
                        "first_seen": is_first_seen,
                        "threshold_bytes": self.min_bytes_out
                    }
                })

        return alerts

    def get_state(self) -> Dict[str, Any]:
        return {
            client_id: list(domains)
            for client_id, domains in self.client_domains.items()
        }

    def set_state(self, state_dict: Dict[str, Any]) -> None:
        self.client_domains.clear()
        for client_id, domains in state_dict.items():
            self.client_domains[client_id] = set(domains)
