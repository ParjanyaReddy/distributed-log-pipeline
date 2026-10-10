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

---

## Streaming Anomaly Detection Quality (Phase 5 Evaluation)

Evaluated detection performance across 4 detectors against labeled synthetic traffic (35 warmup events/client, 5 injected attack bursts per anomaly type):

| Detector | Precision | Recall | F1-Score | Mean Time to Detect (MTTD) | Notes / Thresholds |
|---|---|---|---|---|---|
| **`brute_force`** | **100.0%** | **100.0%** | **100.0%** | **8.00s** | Threshold: $\ge 5$ failures in 60s window; elevates to critical on compromise. |
| **`volume_spike`** | **100.0%** | **100.0%** | **100.0%** | **0.00s** | EWMA z-score: $z > 3.0$, warmup $N \ge 30$, $\alpha = 0.1$, pre-update evaluation. |
| **`rare_domain`** | **100.0%** | **100.0%** | **100.0%** | **0.00s** | First-seen or low-reputation category with $\text{bytes\_out} \ge 50\text{ KB}$. |
| **`beaconing`** | **100.0%** | **100.0%** | **100.0%** | **20.00s** | Inter-arrival coefficient of variation $CV \le 0.15$ with sample count $\ge 6$. |

### Detection Methodologies
- **Event-Time Watermarking:** All windows and moving baselines operate strictly on event time (`ts`), preventing falsification from consumer lags or network pauses. Late events (> 30s) are dropped from window states and logged via metrics.
- **Deduplication:** Every alert carries a deterministic `dedupe_key` (`type|client_id|bucket_iso`) preventing duplicate alerts upon Kafka offset replays.
- **Zero Ingestion Impact:** Detector errors are caught in an isolated try/except block, recording a Prometheus metric (`detector_errors_total`) without blocking message persistence.

---

## Fault Tolerance & Chaos Testing (Phase 7 Evaluation)

All 5 automated chaos scenarios assert the core distributed invariant:
$$\text{produced} == \text{stored\_distinct} + \text{dlq}$$
with **zero data loss** and **zero duplicate rows** in the database.

| Scenario | Injected Fault | System Behavior | Data Loss | Duplicates | Result |
|---|---|---|---|---|---|
| **1. Consumer Crash** | Worker abruptly killed mid-run (SIGKILL) during uncommitted batch. | Remaining consumer takes over assigned partitions, replays from last committed offset, `ON CONFLICT DO NOTHING` ignores replayed inserts. | **0 rows** | **0 rows** | **PASS** (3/3 runs) |
| **2. Database Outage** | Database throws `OperationalError` during batch write. | Worker applies exponential backoff with jitter, maintains Kafka heartbeat via keep-alive poll, flushes cleanly upon recovery. | **0 rows** | **0 rows** | **PASS** (3/3 runs) |
| **3. Broker Restart** | Kafka broker disconnects mid-flight. | Worker pauses polling, resumes upon broker reconnect, replays from durably committed offsets. | **0 rows** | **0 rows** | **PASS** (3/3 runs) |
| **4. Malformed Flood** | Flood of 1,000 corrupt/non-schema payloads (bad JSON, invalid UUIDs, status 999). | 100% of invalid records routed to `raw-logs.dlq` with base64 payload and reason codes. Zero crashes on valid logs. | **0 rows** | **0 rows** | **PASS** (3/3 runs) |
| **5. 10x Traffic Burst** | Sudden spike of 5,000 events arriving simultaneously. | Natural consumer batching absorbs burst without memory blowout; buffers drain in < 1.5 seconds. | **0 rows** | **0 rows** | **PASS** (3/3 runs) |
