import time
from uuid import uuid4
from datetime import datetime, timezone
from common.models import Event
from processor.sink import DatabaseSink


def generate_batch(count: int) -> list[Event]:
    now = datetime.now(timezone.utc)
    return [
        Event(
            event_id=uuid4(),
            ts=now,
            client_id=f"client-{(i % 50):04d}",
            src_ip="10.0.0.1",
            method="GET",
            url="https://benchmark.corp/item",
            domain="benchmark.corp",
            status=200,
            bytes_out=200,
            bytes_in=1500,
            user_agent="bench",
            action="allowed",
            category="bench",
            auth_result="none"
        )
        for i in range(count)
    ]


def run_benchmark():
    total_events = 5000
    batch_sizes = [1, 500, 1000]
    results = {}

    print(f"Benchmarking Database Sink with {total_events} total events...")
    for size in batch_sizes:
        sink = DatabaseSink(page_size=size, dry_run=True)
        batches = [generate_batch(size) for _ in range(total_events // size)]

        start_time = time.time()
        for b in batches:
            sink.write_batch(events=b)
        duration = max(0.0001, time.time() - start_time)
        throughput = total_events / duration

        results[size] = {
            "duration_sec": duration,
            "throughput_ev_per_sec": throughput
        }
        print(f"Batch size {size:>4}: {duration:.4f}s ({throughput:,.1f} events/sec)")

    return results


if __name__ == "__main__":
    run_benchmark()
