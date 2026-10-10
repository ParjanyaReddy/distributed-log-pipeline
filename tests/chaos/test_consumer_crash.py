import pytest
from tests.chaos.chaos_harness import ChaosTestHarness
from processor.main import LogProcessor
from processor.dlq import DeadLetterQueue
from processor.sink import DatabaseSink


def test_chaos_scenario_1_consumer_crash_mid_run():
    """Chaos Experiment 1: Consumer crashes mid-run during an active batch.
    Second consumer takes over assigned partitions, replays uncommitted offsets,
    and proves: produced == stored_distinct + dlq, with 0 data loss and 0 duplicate rows.
    """
    for iteration in range(3):  # Rule: passes 3 times in a row
        harness = ChaosTestHarness()
        shared_sink = DatabaseSink(dry_run=True)
        shared_dlq = DeadLetterQueue(dry_run=True)

        # Consumer 1 starts processing
        worker_1 = LogProcessor(
            max_batch_size=100,
            batch_timeout_sec=1.0,
            dlq=shared_dlq,
            sink=shared_sink,
            dry_run=True
        )

        # Produce 300 messages across partitions
        partition_offsets = {p: 0 for p in range(6)}
        messages = []
        for i in range(300):
            p = i % 6
            off = partition_offsets[p]
            partition_offsets[p] += 1
            payload = harness.make_event(f"client-{(i % 20):04d}", i)
            messages.append((payload, "raw-logs", p, off))

        # Worker 1 processes 150 messages, flushes, and then crashes abruptly on message 220
        # (simulating a SIGKILL without graceful shutdown flush)
        for i in range(220):
            payload, topic, p, off = messages[i]
            worker_1.process_message(payload, topic, p, off)

        # Simulate Worker 1 abrupt death (dropping in-memory uncommitted batch)
        # Worker 1 committed whatever batches completed (up to last flush)
        last_committed = dict(worker_1.committed_offsets)
        worker_1.running = False

        # Worker 2 takes over and starts from the last committed offset of each partition
        worker_2 = LogProcessor(
            max_batch_size=100,
            batch_timeout_sec=1.0,
            dlq=shared_dlq,
            sink=shared_sink,
            dry_run=True
        )

        for payload, topic, p, off in messages:
            committed_off = last_committed.get((topic, p), 0)
            if off >= committed_off:
                # Worker 2 replays from uncommitted offset
                worker_2.process_message(payload, topic, p, off)

        # Worker 2 shuts down cleanly
        worker_2.shutdown()

        # Invariant Verification
        harness.assert_zero_data_loss(shared_sink, shared_dlq)
