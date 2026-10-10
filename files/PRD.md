# PRD.md: Distributed Log Ingestion & Anomaly Detection Pipeline

## 1. What we are building
A Kafka-based streaming pipeline. Simulated clients emit web-gateway-style log events. A scalable consumer group validates, enriches, stores, and analyzes them. Anomalies (brute force, volume spikes, rare/suspicious destinations, beaconing) are flagged in near real time and visualized in Grafana.

## 2. Target users
| Persona | Need |
|---|---|
| Portfolio reviewer / interviewer | Evidence of distributed-systems thinking: partitioning, delivery guarantees, fault tolerance, measured results |
| Security analyst (simulated) | Queryable event store, alerts with severity, low false-positive rate |
| Platform engineer (simulated) | Throughput, lag, DLQ rate, latency dashboards |

## 3. Goals (measurable)
| ID | Goal | Acceptance criterion |
|---|---|---|
| G1 | Simulate hundreds of clients at configurable rates | 300+ clients, rate set via env/CLI, achieved rate reported |
| G2 | Scalable ingestion | Consumer group scales 1 to 6 workers; throughput table in bench/RESULTS.md |
| G3 | Validate/normalize/enrich; bad records to DLQ | 100% of malformed test messages land in `raw-logs.dlq` with payload, reason, source offset |
| G4 | Queryable storage | Events + alerts in Postgres/Timescale; idempotent inserts |
| G5 | Near-real-time detection | 4 detectors; labeled-anomaly eval reports precision, recall, mean time to detect |
| G6 | Observability | Grafana shows throughput, per-partition lag, DLQ rate, alerts by type, p95 latency |
| G7 | Fault tolerance | Chaos scenarios pass with produced count == distinct stored count (+ DLQ count) |

## 4. Non-goals
- Writing our own broker.
- End-to-end exactly-once. We do at-least-once plus idempotent writes and explain why.
- Real GeoIP/threat-intel feeds (use a static local lookup).

## 5. Features
**Must have**
1. Docker Compose environment (`make up`): Kafka (KRaft), Postgres/Timescale, Redis, Prometheus, Grafana, Kafka exporter.
2. Producer/generator with behavior profiles and labeled injected anomalies.
3. Consumer group: validate, enrich, batch idempotent sink, DLQ, graceful shutdown, rebalance handling.
4. Detectors: brute_force, volume_spike (EWMA z-score), rare_domain, beaconing.
5. Prometheus metrics + Grafana dashboard.
6. Test suite: unit, integration, idempotency, fault-tolerance, detection-quality.
7. Chaos experiments (5) and benchmarks documented in README.

**Nice to have (stretch, only after all must-haves pass)**
Schema Registry/Avro, ClickHouse sink, Isolation Forest, 3-broker cluster, Kafka transactions write-up, KEDA autoscaling, Slack/webhook alert routing.

## 6. Event contract (v1)
Fields: `event_id` (UUID, idempotency key), `ts` (ISO-8601 UTC, event time), `client_id`, `src_ip`, `method`, `url`, `domain`, `status`, `bytes_out`, `bytes_in`, `user_agent`, `action` (allowed|blocked), `category`, `auth_result` (success|failure|none).
Validation limits live in `Rules.md`.

## 7. Success definition ("done")
- All must-have features implemented and every Phase gate in `Phases.md` is checked.
- Zero data loss and zero duplicate rows in all chaos runs.
- Detector precision and recall reported with thresholds and dataset described.
- README checklist (spec section 10) complete.

## 8. Assumptions (change here first if wrong)
- Python 3.11+, single developer, single laptop with Docker.
- Start with 1 broker; 3-broker cluster only for chaos scenario 3.
- TimescaleDB chosen over plain Postgres (simpler partition/retention management).
