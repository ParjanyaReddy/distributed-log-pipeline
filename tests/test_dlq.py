import base64

from processor.dlq import DeadLetterQueue


def test_dlq_recording_and_encoding():
    dlq = DeadLetterQueue(dry_run=True)
    raw_payload = b'{"bad": "json"}'
    dlq.send_to_dlq(
        raw_payload=raw_payload,
        reason="invalid_json",
        topic="raw-logs",
        partition=2,
        offset=42,
        details="malformed payload"
    )

    assert len(dlq.in_memory_records) == 1
    record = dlq.in_memory_records[0]

    assert record["reason"] == "invalid_json"
    assert record["topic"] == "raw-logs"
    assert record["partition"] == 2
    assert record["offset"] == 42
    # Verify base64 decode recovers the original bytes
    decoded = base64.b64decode(record["payload_b64"])
    assert decoded == raw_payload
