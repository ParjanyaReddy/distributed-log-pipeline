import json
import pytest
from datetime import datetime, timezone
from uuid import uuid4
from processor.main import LogProcessor
from processor.dlq import DeadLetterQueue
from producers.anomalies import make_malformed_event


def make_valid_raw_event(i: int) -> bytes:
    data = {
        "event_id": str(uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "client_id": f"client-{(i % 50) + 1:04d}",
        "src_ip": f"10.0.0.{(i % 200) + 1}",
        "method": "GET",
        "url": f"https://example.com/item/{i}",
        "domain": "example.com",
        "status": 200,
        "bytes_out": 250,
        "bytes_in": 1200,
        "user_agent": "Mozilla/5.0",
        "action": "allowed",
        "category": "testing",
        "auth_result": "none"
    }
    return json.dumps(data).encode("utf-8")


def test_consumer_gate_10k_valid_and_100_malformed():
    """Phase 3 Gate test: Process 10,000 valid + 100 malformed events.
    Assert 10,000 in sink, 100 in DLQ, and committed offsets match end offsets.
    """
    dlq = DeadLetterQueue(dry_run=True)
    processor = LogProcessor(
        max_batch_size=500,
        batch_timeout_sec=0.5,
        dlq=dlq,
        dry_run=True
    )

    valid_count = 10000
    malformed_count = 100
    total_messages = valid_count + malformed_count

    # Interleave valid and malformed messages across 6 partitions
    current_offset_per_partition = {p: 0 for p in range(6)}

    malformed_interval = total_messages // malformed_count
    malformed_sent = 0

    for i in range(total_messages):
        partition = i % 6
        offset = current_offset_per_partition[partition]
        current_offset_per_partition[partition] += 1

        if (i % malformed_interval == 0) and (malformed_sent < malformed_count):
            malformed_payload = make_malformed_event("invalid_json").encode("utf-8")
            processor.process_message(
                raw_payload=malformed_payload,
                topic="raw-logs",
                partition=partition,
                offset=offset
            )
            malformed_sent += 1
        else:
            valid_payload = make_valid_raw_event(i)
            processor.process_message(
                raw_payload=valid_payload,
                topic="raw-logs",
                partition=partition,
                offset=offset
            )

    # Trigger graceful flush on shutdown
    processor.shutdown()

    # Verify sink and DLQ counts
    assert len(processor.sink.in_memory_events) == valid_count
    assert len(dlq.in_memory_records) == malformed_count

    # Verify that all consumed offsets across all 6 partitions are committed
    for p in range(6):
        expected_next_offset = current_offset_per_partition[p]
        actual_committed = processor.committed_offsets.get(("raw-logs", p))
        assert actual_committed == expected_next_offset

    # Ensure no uncommitted messages remain
    assert len(processor.pending_offsets) == 0
    assert len(processor.batch) == 0
