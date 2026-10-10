# Comprehensive Implementation Plan: Distributed Log Ingestion Pipeline

This document expands on the `Phases.md` structure to outline the precise implementation steps, file modifications, and tasks required to build the pipeline. We will execute this plan sequentially, ensuring each Phase Gate is passed before moving to the next.

## Phase 0: Skeleton & Contracts
**Objective:** Set up the repository layout, dependencies, data models, and database schema.

1. **Repository & Dependencies Initialization:**
   - Create the directory structure defined in `Architecture.md`.
   - Create `requirements.txt` with pinned versions for `confluent-kafka`, `pydantic`, `psycopg2-binary`, `redis`, `prometheus_client`, `numpy`, `pytest`, `testcontainers`, etc.
   - Initialize `Makefile` with targets: `up`, `down`, `test`, `lint`, `logs`.
2. **Configuration & Common Utilities:**
   - Create `common/config.py` to manage all environment variables (`LP_KAFKA_BROKERS`, `LP_DB_URI`, etc.).
   - Create `common/logging.py` to enforce JSON line logging format.
3. **Data Models:**
   - Create `common/models.py` using `pydantic` v2. Define the `Event` schema based on the event contract.
   - Include validation rules (e.g., rejecting naive timestamps, ensuring valid IPs/status codes) as defined in `Rules.md`.
4. **Database Schema:**
   - Create `db/schema.sql` implementing the TimescaleDB hypertables for `events` and `alerts`, using the composite primary keys `(event_id, ts)` and `(dedupe_key, ts)`.
5. **Phase 0 Gate:**
   - Run `make lint` and `make test`. Ensure schema applies cleanly. Update `Memory.md`.

## Phase 1: Infrastructure
**Objective:** Bring up the backend services locally using Docker Compose.

1. **Docker Compose Setup:**
   - Create `docker-compose.yml` containing:
     - `kafka` (KRaft mode, 1 broker).
     - `timescaledb` (Postgres image with Timescale extension).
     - `redis` (for state check-pointing).
     - `prometheus` and `grafana` (with pre-provisioned datasource).
     - `kafka-exporter` for monitoring lag.
2. **Topic Initialization:**
   - Create a script or container command to idempotently create `raw-logs` (6 partitions), `raw-logs.dlq` (1 partition), and `alerts` (3 partitions).
3. **Phase 1 Gate:**
   - `make up` successfully spins up all containers healthily. Kafka topics exist correctly.

## Phase 2: Producer
**Objective:** Build the load generator with behavior profiles and anomaly injection.

1. **Profiles Setup:**
   - Create `producers/profiles.py` to define client behaviors (office worker, developer, heavy downloader) and seed the RNG.
2. **Event Generator:**
   - Create `producers/generator.py` to orchestrate 300+ simulated clients.
   - Implement rate controlling (events/sec) and idempotency (`enable.idempotence=True`, `acks=all`).
3. **Anomaly Injector:**
   - Integrate labeled anomaly injection in the generator, outputting ground-truth to `labels.jsonl` for later evaluation.
4. **Phase 2 Gate:**
   - Run a unit test on profiles. Run a 60s load test at 1000 ev/s and verify delivery counts match.

## Phase 3 & 4: Consumer Core & Storage Sink
**Objective:** Ingest events, validate, enrich, manage DLQ, and perform batched, idempotent DB writes.

1. **Consumer Loop & Rebalance:**
   - Create `processor/main.py`. Implement the Kafka consumer loop, manual offset tracking (`enable.auto.commit=False`), and `SIGTERM` graceful shutdown.
   - Implement `on_assign` and `on_revoke` callbacks for state management.
2. **Validation & Enrichment:**
   - Create `processor/validate.py` to leverage the Pydantic models.
   - Create `processor/enrich.py` to add static domain categories and IP data.
   - Create `processor/dlq.py` to handle routing invalid events to `raw-logs.dlq`.
3. **Idempotent Storage Sink:**
   - Create `processor/sink.py`. Implement batched `execute_values` insertions (<= 1000 rows) with `ON CONFLICT DO NOTHING`.
   - Ensure the strict processing contract (DLQ ack -> DB commit -> Alert publish -> Offset commit) is followed.
4. **Phase 3/4 Gate:**
   - Integration test: Produce valid and malformed messages. Ensure exact counts in TimescaleDB and the DLQ. Run an idempotency test (replaying offsets).

## Phase 5: Anomaly Detectors
**Objective:** Implement rule-based and statistical streaming anomaly detection.

1. **Base Framework & State:**
   - Create `processor/detectors/base.py` for standard detector interfaces.
   - Create `processor/state.py` to manage loading/saving per-client state to Redis on partition assignment/revocation.
2. **Detectors Implementation:**
   - `brute_force.py`: Sliding window of failed logins.
   - `volume_spike.py`: EWMA z-score on `bytes_out` with warmup guards.
   - `rare_domain.py`: New or low-reputation destinations.
   - `beaconing.py`: Inter-arrival time coefficient of variation.
3. **Phase 5 Gate:**
   - Unit test each detector heavily (warmup, zero-variance, out-of-order). Validate against the `labels.jsonl` from the producer.

## Phase 6: Observability
**Objective:** Expose pipeline health and performance metrics visually.

1. **Prometheus Metrics:**
   - Update `common/metrics.py` to expose counts, latency histograms, DLQ rates, and detector errors.
2. **Grafana Dashboards:**
   - Create `dashboards/main.json`. Map out throughput, lag, latency, alerts, and DLQ errors matching the `Design.md` layout.
   - Auto-provision the dashboard in `docker-compose.yml`.
3. **Phase 6 Gate:**
   - Run load and verify the dashboard populates correctly with lag metrics.

## Phase 7 & 8: Chaos & Benchmarks
**Objective:** Validate system resilience and document performance.

1. **Chaos Scripts:**
   - Write automated tests in `tests/chaos/` to kill consumers, stop Postgres, restart Kafka, flood malformed data, and spike traffic.
2. **Benchmarking & CI:**
   - Run scaling tests (1 vs 3 vs 6 workers) and record results in `bench/RESULTS.md`.
   - Setup GitHub Actions CI for linting and testing.
3. **Final Gate:**
   - All chaos tests pass with zero data loss. README is updated. Pipeline is "done".
