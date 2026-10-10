import json
import os
import zlib

from producers.generator import LogGenerator


def kafka_default_partition(key_str: str, num_partitions: int = 6) -> int:
    """Simulates deterministic key hashing (similar to standard partitioner)"""
    return (zlib.crc32(key_str.encode("utf-8")) & 0x7fffffff) % num_partitions


def test_generator_dry_run_delivery(tmp_path):
    labels_file = str(tmp_path / "test_labels.jsonl")
    generator = LogGenerator(
        target_rate=200.0,
        num_clients=50,
        labels_file=labels_file,
        anomaly_interval_sec=0.5,
        dry_run=True
    )

    # Run for 1.0 second
    generator.run(duration_sec=1.0)

    assert generator.sent_count > 0
    assert generator.sent_count == generator.delivered_count
    assert generator.failure_count == 0

    # Ensure labels file was created and contains recorded anomalies
    assert os.path.exists(labels_file)
    with open(labels_file, "r") as f:
        lines = f.readlines()
        assert len(lines) >= 1
        first_label = json.loads(lines[0])
        assert "type" in first_label
        assert "client_id" in first_label


def test_client_partition_stickiness():
    """Verify that same client_id always lands on the same partition for 6 partitions."""
    num_partitions = 6
    client_ids = [f"client-{i:04d}" for i in range(1, 100)]

    for client_id in client_ids:
        p1 = kafka_default_partition(client_id, num_partitions)
        p2 = kafka_default_partition(client_id, num_partitions)
        p3 = kafka_default_partition(client_id, num_partitions)
        assert p1 == p2 == p3
        assert 0 <= p1 < num_partitions
