import time

from processor.dlq import DeadLetterQueue
from processor.main import LogProcessor
from processor.sink import DatabaseSink
from tests.chaos.chaos_harness import ChaosTestHarness


def test_chaos_scenario_5_traffic_burst_and_recovery():
    """Chaos Experiment 5: 10x traffic burst simulation.
    Injects 5,000 messages in a rapid burst. Asserts backpressure handles the spike,
    buffers drain smoothly, and produced == stored_distinct + dlq.
    """
    for iteration in range(3):
        harness = ChaosTestHarness()
        sink = DatabaseSink(dry_run=True)
        dlq = DeadLetterQueue(dry_run=True)

        processor = LogProcessor(
            max_batch_size=500,
            batch_timeout_sec=0.5,
            dlq=dlq,
            sink=sink,
            dry_run=True
        )

        burst_count = 5000
        start_time = time.time()

        for i in range(burst_count):
            p = i % 6
            payload = harness.make_event(f"client-{(i % 50):04d}", i)
            processor.process_message(payload, "raw-logs", p, i)

        processor.shutdown()
        drain_time = time.time() - start_time

        # Invariant Verification
        harness.assert_zero_data_loss(sink, dlq)
        assert len(sink.in_memory_events) == burst_count
        assert drain_time < 5.0  # Fast drain under batching
