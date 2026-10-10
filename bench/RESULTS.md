# Benchmark Results

## Batch Size vs Insertion Throughput (Phase 4 Evaluation)

Evaluated effect of batching on ingestion sink throughput across 5,000 events:

| Batch Size | Execution Time (s) | Throughput (events/sec) | Speedup vs Batch Size 1 |
|---|---|---|---|
| **1** (Single insert) | 0.8105s | 6,169 ev/s | 1.0x (Baseline) |
| **500** | 0.0080s | 627,833 ev/s | ~101.8x |
| **1,000** (Max page) | 0.0078s | 642,018 ev/s | ~104.1x |

### Observations & Trade-offs
- Batching amortizes SQL parsing, statement dispatch, and transaction overhead.
- Increasing batch size from 1 to 500 yields a ~100x throughput boost.
- Increasing from 500 to 1,000 yields incremental throughput gains while capping bind parameters well below the PostgreSQL statement limit of 65,535 (13 columns * 1,000 rows = 13,000 parameters).
