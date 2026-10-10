from processor.dlq import DeadLetterQueue
from processor.main import LogProcessor
from processor.sink import DatabaseSink
from tests.chaos.chaos_harness import ChaosTestHarness


def test_chaos_scenario_3_broker_restart():
    """Chaos Experiment 3: Kafka broker restart / temporary disconnect.
    Simulates consumer pausing during broker unavailability and resuming
    cleanly upon broker reconnect without duplicating or losing data.
    """
    for iteration in range(3):
        harness = ChaosTestHarness()
        sink = DatabaseSink(dry_run=True)
        dlq = DeadLetterQueue(dry_run=True)

        processor = LogProcessor(
            max_batch_size=50,
            batch_timeout_sec=0.5,
            dlq=dlq,
            sink=sink,
            dry_run=True
        )

        messages = [
            (harness.make_event(f"client-{(i % 10):04d}", i), "raw-logs", i % 6, i)
            for i in range(200)
        ]

        # Process first half before "broker restart"
        for payload, topic, p, off in messages[:100]:
            processor.process_message(payload, topic, p, off)
        processor.flush_batch(force=True)

        # Broker goes down and restarts: offsets replay from last commit
        last_committed = dict(processor.committed_offsets)

        # Resume processing remaining messages after broker restart
        for payload, topic, p, off in messages[80:]:  # Overlap 20 messages to simulate replay
            committed_off = last_committed.get((topic, p), 0)
            if off >= committed_off:
                processor.process_message(payload, topic, p, off)

        processor.shutdown()

        # Invariant Verification: exactly 200 stored, 0 duplicates
        harness.assert_zero_data_loss(sink, dlq)
