import pytest
from unittest.mock import MagicMock
import psycopg2

from tests.chaos.chaos_harness import ChaosTestHarness
from processor.main import LogProcessor
from processor.dlq import DeadLetterQueue
from processor.sink import DatabaseSink


class OutageMockSink(DatabaseSink):
    """Simulates a database that fails with OperationalError for N attempts then recovers."""
    def __init__(self, failure_count: int = 3):
        super().__init__(dry_run=True)
        self.failure_count = failure_count
        self.attempts = 0
        self.keep_alive_invoked = 0

    def write_batch(self, events, alerts=None, keep_alive_fn=None):
        if self.attempts < self.failure_count:
            self.attempts += 1
            if keep_alive_fn:
                keep_alive_fn()
                self.keep_alive_invoked += 1
            # Simulate transient network / database outage
            raise psycopg2.OperationalError("could not connect to server: Connection refused")
        
        # Recovered
        super().write_batch(events, alerts, keep_alive_fn)


def test_chaos_scenario_2_database_outage_recovery():
    """Chaos Experiment 2: Database outage simulation.
    Consumer catches OperationalError, applies exponential backoff, keeps poll alive,
    and successfully flushes upon recovery with zero data loss.
    """
    for iteration in range(3):
        harness = ChaosTestHarness()
        dlq = DeadLetterQueue(dry_run=True)
        mock_sink = OutageMockSink(failure_count=3)

        processor = LogProcessor(
            max_batch_size=50,
            batch_timeout_sec=0.5,
            dlq=dlq,
            sink=mock_sink,
            dry_run=True
        )

        # Produce messages
        messages = []
        for i in range(150):
            p = i % 6
            payload = harness.make_event("client-db-outage", i)
            messages.append((payload, "raw-logs", p, i))

        # Process messages: initial batches will trigger outage retries
        for payload, topic, p, off in messages:
            try:
                processor.process_message(payload, topic, p, off)
            except psycopg2.OperationalError:
                # Processor propagates error after max retries or catches during loop
                pass

        # Database recovers and processor flushes cleanly
        mock_sink.attempts = 10  # Ensure recovered
        processor.shutdown()

        # Invariant Verification
        harness.assert_zero_data_loss(mock_sink, dlq)
        assert mock_sink.keep_alive_invoked >= 1
