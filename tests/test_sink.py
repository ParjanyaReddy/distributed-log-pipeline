from datetime import UTC, datetime
from uuid import uuid4

from common.models import Event
from processor.sink import DatabaseSink


def make_test_event(i: int = 1) -> Event:
    return Event(
        event_id=uuid4(),
        ts=datetime.now(UTC),
        client_id=f"client-{i:04d}",
        src_ip="10.0.0.1",
        method="GET",
        url="https://example.com/test",
        domain="example.com",
        status=200,
        bytes_out=150,
        bytes_in=1200,
        user_agent="test-agent",
        action="allowed",
        category="testing",
        auth_result="none"
    )


def test_sink_page_size_cap():
    # Rule R-8: page_size cannot exceed 1000
    sink = DatabaseSink(page_size=5000, dry_run=True)
    assert sink.page_size == 1000


def test_sink_idempotency_replay():
    """Phase 4 Gate: Replaying identical batch twice results in no duplicate rows."""
    sink = DatabaseSink(dry_run=True)
    events = [make_test_event(i) for i in range(100)]
    alerts = [
        {
            "ts": datetime.now(UTC),
            "type": "brute_force",
            "severity": "high",
            "client_id": "client-0001",
            "dedupe_key": "brute_force|client-0001|2026-10-10T12:00:00Z",
            "details": {"failures": 10}
        }
    ]

    # First write
    sink.write_batch(events=events, alerts=alerts)
    initial_event_count = len(sink.in_memory_events)
    initial_alert_count = len(sink.in_memory_alerts)
    assert initial_event_count == 100
    assert initial_alert_count == 1

    # Second write (replay of identical batch from offset replay)
    sink.write_batch(events=events, alerts=alerts)
    assert len(sink.in_memory_events) == initial_event_count
    assert len(sink.in_memory_alerts) == initial_alert_count


def test_sink_crash_injection_recovery():
    """Phase 4 Gate: Simulate crash between DB commit and offset commit.
    On restart and replay, stored count remains strictly identical with 0 data loss.
    """
    sink = DatabaseSink(dry_run=True)
    batch_1 = [make_test_event(i) for i in range(50)]
    batch_2 = [make_test_event(i + 100) for i in range(50)]

    # Worker 1 writes batch 1 and succeeds
    sink.write_batch(events=batch_1)
    assert len(sink.in_memory_events) == 50

    # Worker 1 writes batch 2 to DB, but "crashes" before committing offset
    sink.write_batch(events=batch_2)
    assert len(sink.in_memory_events) == 100

    # Worker 2 restarts from uncommitted offset (re-processing batch 2)
    sink.write_batch(events=batch_2)

    # Idempotent write prevents duplicates
    assert len(sink.in_memory_events) == 100
    # Distinct event IDs match total unique events
    unique_ids = set(ev[0] for ev in sink.in_memory_events)
    assert len(unique_ids) == 100
