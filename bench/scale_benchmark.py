import time
from tests.chaos.chaos_harness import ChaosTestHarness
from processor.main import LogProcessor
from processor.dlq import DeadLetterQueue
from processor.sink import DatabaseSink


def benchmark_worker_scaling():
    """Evaluates throughput scaling across 1, 2, 3, and 6 workers on 6 partitions."""
    harness = ChaosTestHarness(num_partitions=6)
    total_events = 12000
    messages = [
        (harness.make_event(f"client-{(i % 100):04d}", i), "raw-logs", i % 6, i)
        for i in range(total_events)
    ]

    worker_counts = [1, 2, 3, 6]
    results = {}

    print("\n--- Worker Scaling Benchmark (6 Partitions) ---")
    print(f"{'Workers':<10} {'Assigned Partitions':<22} {'Time (s)':<12} {'Throughput (ev/s)':<18} {'Scaling Efficiency'}")
    print("-" * 75)

    baseline_throughput = 0.0

    for w_count in worker_counts:
        sink = DatabaseSink(dry_run=True)
        dlq = DeadLetterQueue(dry_run=True)

        workers = [
            LogProcessor(
                max_batch_size=500,
                batch_timeout_sec=0.5,
                dlq=dlq,
                sink=sink,
                dry_run=True
            )
            for _ in range(w_count)
        ]

        # In Kafka consumer group, partitions are evenly divided across workers
        # e.g., 6 partitions / 2 workers = 3 partitions per worker
        partitions_per_worker = 6 // w_count
        worker_partition_map = {}
        for w_idx in range(w_count):
            for p in range(w_idx * partitions_per_worker, (w_idx + 1) * partitions_per_worker):
                worker_partition_map[p] = workers[w_idx]

        start_time = time.time()
        for payload, topic, p, off in messages:
            target_worker = worker_partition_map[p]
            target_worker.process_message(payload, topic, p, off)

        for w in workers:
            w.shutdown()

        elapsed = max(0.001, time.time() - start_time)
        throughput = total_events / elapsed

        if w_count == 1:
            baseline_throughput = throughput
            efficiency = "100% (Baseline)"
        else:
            speedup = throughput / baseline_throughput
            efficiency = f"{speedup:.2f}x ({round(speedup / w_count * 100, 1)}%)"

        parts_desc = f"{6 // w_count} part/worker"
        results[w_count] = {
            "time_s": round(elapsed, 4),
            "throughput": round(throughput, 1),
            "efficiency": efficiency
        }

        print(f"{w_count:<10} {parts_desc:<22} {elapsed:<12.4f} {throughput:<18,.1f} {efficiency}")

    print("\n* Note: Any workers beyond 6 will sit idle due to the 6-partition ceiling.")
    return results


if __name__ == "__main__":
    benchmark_worker_scaling()
