from processor.dlq import DeadLetterQueue
from processor.main import LogProcessor
from processor.sink import DatabaseSink
from tests.chaos.chaos_harness import ChaosTestHarness


def test_chaos_scenario_4_malformed_flood():
    """Chaos Experiment 4: Sudden flood of malformed / poison messages.
    Asserts pipeline does not crash, 100% of malformed messages route to DLQ,
    valid messages are saved to database, and produced == stored_distinct + dlq.
    """
    for iteration in range(3):
        harness = ChaosTestHarness()
        sink = DatabaseSink(dry_run=True)
        dlq = DeadLetterQueue(dry_run=True)

        processor = LogProcessor(
            max_batch_size=100,
            batch_timeout_sec=0.5,
            dlq=dlq,
            sink=sink,
            dry_run=True
        )

        # 1000 malformed messages interleaved with 500 valid messages
        messages = []
        for i in range(1500):
            p = i % 6
            if i % 3 != 0:
                # Malformed payload
                reason = "bad_json" if i % 2 == 0 else "bad_uuid"
                payload = harness.make_malformed(reason)
            else:
                # Valid payload
                payload = harness.make_event("client-flood", i)
            messages.append((payload, "raw-logs", p, i))

        # Stream all messages through processor
        for payload, topic, p, off in messages:
            processor.process_message(payload, topic, p, off)

        processor.shutdown()

        # Invariant Verification
        harness.assert_zero_data_loss(sink, dlq)
        assert len(dlq.in_memory_records) == 1000
        assert len(sink.in_memory_events) == 500
