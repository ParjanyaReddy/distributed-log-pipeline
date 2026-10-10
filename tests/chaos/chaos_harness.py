from typing import List, Dict, Any, Set
from uuid import uuid4
from datetime import datetime, timezone
import json

from common.models import Event
from processor.main import LogProcessor
from processor.dlq import DeadLetterQueue
from processor.sink import DatabaseSink


class ChaosTestHarness:
    """Orchestrates chaos simulation runs and asserts zero data loss invariants."""
    def __init__(self, num_partitions: int = 6):
        self.num_partitions = num_partitions
        self.produced_events: List[Dict[str, Any]] = []
        self.produced_malformed: List[bytes] = []

    def make_event(self, client_id: str, i: int, domain: str = "service.corp") -> bytes:
        data = {
            "event_id": str(uuid4()),
            "ts": datetime.now(timezone.utc).isoformat(),
            "client_id": client_id,
            "src_ip": "10.0.1.20",
            "method": "GET",
            "url": f"https://{domain}/item/{i}",
            "domain": domain,
            "status": 200,
            "bytes_out": 200,
            "bytes_in": 1200,
            "user_agent": "Mozilla/5.0",
            "action": "allowed",
            "category": "productivity",
            "auth_result": "none"
        }
        self.produced_events.append(data)
        return json.dumps(data).encode("utf-8")

    def make_malformed(self, reason: str = "bad_json") -> bytes:
        if reason == "bad_json":
            raw = b"{invalid:json"
        elif reason == "bad_uuid":
            raw = json.dumps({
                "event_id": "not-a-uuid",
                "ts": datetime.now(timezone.utc).isoformat(),
                "client_id": "client-bad",
                "src_ip": "10.0.0.1",
                "method": "GET",
                "url": "https://bad.corp",
                "domain": "bad.corp",
                "status": 200,
                "bytes_out": 100,
                "bytes_in": 100,
                "user_agent": "test",
                "action": "allowed",
                "category": "test",
                "auth_result": "none"
            }).encode("utf-8")
        else:
            raw = b"completely corrupted byte stream"
        self.produced_malformed.append(raw)
        return raw

    def assert_zero_data_loss(self, sink: DatabaseSink, dlq: DeadLetterQueue) -> None:
        """Core Chaos Invariant: produced == stored_distinct + dlq, with 0 duplicates."""
        stored_tuples = sink.in_memory_events
        dlq_records = dlq.in_memory_records

        total_produced = len(self.produced_events) + len(self.produced_malformed)
        stored_count = len(stored_tuples)
        dlq_count = len(dlq_records)

        # Check for zero duplicate rows in the stored dataset
        stored_event_ids: Set[str] = {ev[0] for ev in stored_tuples}
        assert stored_count == len(stored_event_ids), (
            f"Duplicate records found in database! Total stored: {stored_count}, Distinct IDs: {len(stored_event_ids)}"
        )

        # Assert total balance: produced == stored + dlq
        assert total_produced == stored_count + dlq_count, (
            f"Data loss detected! Produced: {total_produced}, Stored: {stored_count}, DLQ: {dlq_count}"
        )
        assert stored_count == len(self.produced_events)
        assert dlq_count == len(self.produced_malformed)
