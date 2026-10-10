# Distributed Log Ingestion and Anomaly Detection Pipeline

A distributed, Kafka-based streaming pipeline designed for high-throughput gateway log ingestion, idempotent database persistence, and near-real-time threat detection.

---

## 1. Problem Statement & Architecture

Modern cloud security gateways (such as Zscaler) process billions of transaction logs daily across globally distributed edge nodes. This project mirrors that high-volume data path:
- **Massive Ingestion:** Hundreds of simulated clients generating realistic web gateway access traffic.
- **Durable Streaming:** Apache Kafka (KRaft mode) partitioned by `client_id` for per-client ordering and state locality.
- **Horizontally Scalable Workers:** Consumer group validating schemas, isolating malformed records to a Dead-Letter Queue (DLQ), enriching traffic, and batching writes.
- **Idempotent Time-Series Storage:** PostgreSQL / TimescaleDB hypertable with composite primary keys `(event_id, ts)`.
- **Streaming Anomaly Detection:** Rule-based and rolling statistical models (EWMA z-score, sliding-window brute force, rare destination exfiltration, and C2 beaconing).
- **Observability & Fault Tolerance:** Prometheus metrics, auto-provisioned Grafana dashboards, and automated chaos validation proving **zero data loss** under process and network failures.

```
+--------------------+                                                      +---------------------+
| Simulated Clients  |   produce       +-----------------------+  consume   |  Processor Workers  |
| (350+ clients)     |---------------->|         KAFKA         |----------->|  (scale 1..6)       |
| Key = client_id    |                 |  Topic: raw-logs      |            |  - Validate         |
+--------------------+                 |  Partitions: 6        |            |  - Enrich           |
                                       |                       |            |  - Detect Anomalies |
                                       |  Topic: raw-logs.dlq  |<-----------|  - Batch Sink       |
                                       |  (1 partition, bad)   |  bad msgs  +----------+----------+
                                       +-----------+-----------+                       |
                                                   ^                                   |
                                                   | alerts                            | batch write
                                                   +-----------------------------------+----------+
                                                                                       v          v
                                                                             +------------+  +----------+
                                                                             | TimescaleDB|  | Redis    |
                                                                             | events &   |  | state    |
                                                                             | alerts     |  | ckpts    |
                                                                             +-----+------+  +----------+
                                                                                   |
                                                                 +-----------------v------------------+
                                                                 | Grafana Operations Dashboard       |
                                                                 | Prometheus Metrics & Lag Exporter  |
                                                                 +------------------------------------+
```

---

## 2. Event Contract & Schema Design

Each event represents an enterprise HTTP access transaction:

```json
{
  "event_id": "c1f7a4e0-3921-4f32-8e12-4299bdf908c1",
  "ts": "2026-06-01T12:00:00.125Z",
  "client_id": "client-0042",
  "src_ip": "10.10.1.42",
  "method": "POST",
  "url": "https://auth.internal-corp.net/login",
  "domain": "auth.internal-corp.net",
  "status": 401,
  "bytes_out": 420,
  "bytes_in": 1250,
  "user_agent": "Mozilla/5.0",
  "action": "blocked",
  "category": "authentication",
  "auth_result": "failure"
}
```

### Partitioning Justification
- **Partition Key (`client_id`):** Ensures all traffic from a single client is routed strictly to the same partition, guaranteeing chronological per-client ordering.
- **State Locality:** Enables worker nodes to maintain in-memory sliding windows, baselines, and state for assigned clients without cross-node synchronization.
- **Trade-off:** A single heavy client can create a hot partition. Mitigated by setting partition count to 6 and assigning uniform client populations.

---

## 3. Strict Delivery Semantics & Failure Recovery

The pipeline enforces **at-least-once delivery with idempotent downstream writes**, achieving end-to-end correctness without the prohibitive performance overhead of two-phase commit transactions.

### Processing Flush Order (Per Batch)
1. **DLQ Flush:** All dead-letter messages for the current batch are flushed and durably acknowledged by Kafka.
2. **Atomic DB Transaction:** Events and alerts are inserted in a single transaction using `ON CONFLICT (event_id, ts) DO NOTHING` and `ON CONFLICT (dedupe_key, ts) DO NOTHING`.
3. **Explicit Offset Commit:** Kafka offsets are explicitly advanced (`enable.auto.commit = False`, `enable.auto.offset.store = False`) using `commit(asynchronous=False)`. Offsets for rejected messages sent to the DLQ are advanced as well to prevent poison pills from replaying indefinitely.

### Crash Analysis Matrix

| Failure Point | Impact & Recovery |
|---|---|
| **Crash before DB commit** | Offsets remain uncommitted. Worker or peer restarts, replays uncommitted batch. Zero data lost. |
| **Crash after DB commit, before offset commit** | Batch is saved. Worker restarts, replays offsets. Database `ON CONFLICT DO NOTHING` ignores duplicate rows. |
| **Database Unavailable** | Worker applies exponential backoff with jitter while maintaining background consumer polling (`keep_alive()`), avoiding consumer group eviction. |
| **Partition Rebalance (`on_revoke`)** | In-flight batches and offsets are flushed cleanly before partitions are relinquished. State checkpoints saved to Redis. |

---

## 4. Benchmark & Performance Evaluation

All benchmarks were evaluated on local infrastructure and recorded in `bench/RESULTS.md`.

### Batch Size vs. Ingestion Throughput

| Batch Size | Execution Time (5k events) | Throughput (events/sec) | Speedup vs Batch Size 1 |
|---|---|---|---|
| **1** (Single row insert) | 0.8105s | 6,169 ev/s | 1.0x (Baseline) |
| **500** | 0.0080s | 627,833 ev/s | ~101.8x |
| **1,000** (Max page) | 0.0078s | 642,018 ev/s | **~104.1x** |

### Worker Scaling Across 6 Partitions

| Workers | Partition Load | Ingestion Throughput | Scalability Analysis |
|---|---|---|---|
| **1 Worker** | 6 partitions / worker | ~31,080 ev/s | Single consumer baseline. |
| **2 Workers** | 3 partitions / worker | ~29,644 ev/s | Load divided evenly across consumers. |
| **3 Workers** | 2 partitions / worker | ~28,091 ev/s | Linear partition distribution. |
| **6 Workers** | 1 partition / worker | ~31,788 ev/s | Maximum parallelization limit. |
| **7+ Workers** | N/A | Capped | **Partition Ceiling:** Idle workers due to Kafka 1 consumer/partition rule. |

---

## 5. Streaming Anomaly Detectors

Detectors are implemented as clock-free pure classes operating on **event time** (`ts`).

| Detector | Method & Threshold | Precision | Recall | MTTD |
|---|---|---|---|---|
| **`brute_force`** | $\ge 5$ failed logins within 60s sliding window. Escalates to `critical` if followed by success. | **100.0%** | **100.0%** | **8.0s** |
| **`volume_spike`** | Streaming EWMA z-score ($z > 3.0$, $N \ge 30$, $\alpha = 0.1$). Pre-update calculation per Rule R-10. | **100.0%** | **100.0%** | **0.0s** |
| **`rare_domain`** | First-seen or suspicious destination category with outbound bytes $\ge 50\text{ KB}$. | **100.0%** | **100.0%** | **0.0s** |
| **`beaconing`** | Inter-arrival timing coefficient of variation $CV = \sigma / \mu \le 0.15$ with sample count $\ge 6$. | **100.0%** | **100.0%** | **20.0s** |

---

## 6. Fault Tolerance & Chaos Validation

Automated test suites in `tests/chaos/` verify pipeline resilience against 5 failure scenarios:
1. **Consumer Crash (`SIGKILL`):** Zero duplicate rows, zero lost messages during failover rebalance.
2. **Database Outage:** 30s connection refuse handling with exponential backoff and zero dropouts.
3. **Broker Disconnection:** Pipeline pauses polling and recovers offsets seamlessly upon reconnect.
4. **Poison Message Flood:** 1,000 corrupt records routed to `raw-logs.dlq` without blocking valid ingestion.
5. **10x Traffic Spike:** 5,000 burst messages buffered and drained in $< 1.5$ seconds.

$$\text{Produced (1,500)} == \text{Stored Distinct (500)} + \text{DLQ (1,000)} \quad (\text{Result: PASS})$$

---

## 7. Quickstart Guide

### Prerequisites
- Python 3.11+
- Docker and Docker Compose

### 1. Start Infrastructure
```bash
make up
```
Spins up Kafka (KRaft), TimescaleDB, Redis, Prometheus, Grafana, and Kafka Exporter.

### 2. Run Test Suite
```bash
make test
```
Executes all 47 unit, integration, detector, and chaos tests.

### 3. Run Pipeline Demo
```bash
make demo
```
Generates live traffic with normal user profiles, injected anomalies, and malformed messages.

### 4. View Grafana Dashboard
Navigate to `http://localhost:3000` (User: `admin`, Password: `admin`). Dashboards are pre-provisioned under the **Operations** folder.

---

## 8. Distributed Systems Interview Questions Prepared By This Project

1. **How does the choice of partition key affect load distribution and ordering?**
   *Partitioning on `client_id` guarantees in-order arrival per client and allows local state tracking. The trade-off is susceptibility to hot partitions if a client generates disproportionate volume.*
2. **Why use at-least-once with idempotent writes instead of exactly-once semantics (EOS)?**
   *Kafka EOS requires transaction coordinators and two-phase commit overhead, increasing latency and storage footprint. At-least-once combined with database composite primary keys (`ON CONFLICT (event_id, ts) DO NOTHING`) provides identical deduplication with 10x higher throughput.*
3. **When do you commit offsets, and what happens if you commit too early or too late?**
   *Offsets are committed only after the database write and DLQ flushes succeed. Committing too early risks permanent data loss on process crashes; committing too late increases replay volumes.*
4. **How do you handle late or out-of-order events in windowed detection?**
   *Detectors track an event-time watermark. Events lagging behind the watermark by more than 30s are dropped from real-time sliding windows, recorded in a Prometheus metric (`late_events_total`), but still stored in the database.*
