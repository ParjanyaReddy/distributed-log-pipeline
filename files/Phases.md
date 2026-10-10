# Phases.md: Build plan with verification gates

Rule: do not start phase N+1 until every box in phase N is checked. Each gate is a runnable command or a measurable result.

## Phase 0: Skeleton & contracts (Day 1-2)
- [ ] Repo layout per `Architecture.md`; `requirements.txt` pinned; `ruff`, `mypy`, `pytest` configured
- [ ] `common/models.py`: Event model + reason codes; `common/config.py`
- [ ] `db/schema.sql` (corrected composite PK) reviewed
- [ ] `Makefile` targets: `up down test lint logs`
**Gate:** `make lint test` passes on empty tests; schema applies cleanly on a throwaway Timescale container.

## Phase 1: Infrastructure (Week 1)
- [ ] Compose: kafka (KRaft), timescaledb, redis, prometheus, grafana, kafka-exporter, healthchecks on all
- [ ] Topic-init job creates 3 topics idempotently (`--if-not-exists`)
- [ ] Schema auto-applied on DB start
- [ ] Listeners work from both containers and host
**Gate:** `make up` from clean state reaches all-healthy; `kafka-topics --describe` shows 6/1/3 partitions; `make down -v && make up` works twice.

## Phase 2: Producer (Week 1-2)
- [ ] Profiles (office worker, developer, heavy downloader), 300+ clients, seeded RNG
- [ ] Rate control (events/sec) + achieved-rate report
- [ ] Idempotent producer, delivery callback, `flush()` on exit
- [ ] Labeled anomaly injector writes a ground-truth file (`labels.jsonl`: type, client, start, end)
- [ ] Optional malformed-message mode (for DLQ tests)
**Gate:** unit tests for profiles; run 60s at 1000 ev/s: delivered == sent, 0 failures; same client always lands on same partition (test).

## Phase 3: Consumer core (Week 2)
- [ ] Validate -> DLQ path with fixed reason codes
- [ ] Consumer loop with manual offset tracking per partition
- [ ] Graceful shutdown + rebalance callbacks
- [ ] Prometheus metrics endpoint
- [ ] (Sink stubbed with in-memory list at this step)
**Gate:** integration test: produce 10k valid + 100 malformed -> 10k in sink, 100 in DLQ, committed offsets == end offsets; SIGTERM leaves no uncommitted processed messages.

## Phase 4: Storage sink (Week 2-3)
- [ ] Batched `execute_values` insert with `ON CONFLICT DO NOTHING`, batch <= 1000 rows per statement
- [ ] Flush ordering exactly per `Architecture.md` section 6
- [ ] Retry/backoff on DB errors while staying alive in group (R-12)
- [ ] Enrichment (domain -> category via static map, IP parse)
**Gate:** idempotency test (replay twice, same count); crash-injection test (kill between DB commit and offset commit; restart; count correct); `bench`: batch size 1 vs 500 vs 1000 recorded.

## Phase 5: Detectors (Week 3-4)
Order: brute_force -> volume_spike -> rare_domain -> beaconing. One detector per sub-phase, each with its own gate.
- [ ] Shared base interface + per-detector error isolation
- [ ] Event-time windows + allowed lateness; late-event metric
- [ ] `dedupe_key` for every alert; alerts written in same txn as events
- [ ] Redis checkpoint/restore on rebalance
- [ ] Config thresholds in env/config, documented in README
**Gate (each):** unit tests incl. warmup/zero-variance/out-of-order/replay; run on labeled traffic and record precision/recall/MTTD in `bench/RESULTS.md`.

## Phase 6: Observability (Week 4)
- [ ] Metrics: `events_consumed_total`, `events_written_total`, `dlq_total{reason}`, `batch_write_seconds`, `processing_latency_seconds`, `alerts_total{type}`, `detector_errors_total`, `late_events_total`
- [ ] Grafana dashboard JSON provisioned automatically (no manual clicking)
**Gate:** dashboard shows live data after `make up && make demo`; lag per partition visible.

## Phase 7: Chaos experiments (Week 4)
Automated scripts in `tests/chaos/`, each asserting `produced == stored_distinct + dlq`:
- [ ] 1. Kill one consumer mid-run
- [ ] 2. Stop Postgres 30s
- [ ] 3. Restart Kafka (3-broker variant optional)
- [ ] 4. Malformed message flood
- [ ] 5. 10x burst, then measure recovery time
**Gate:** all 5 pass 3 times in a row; screenshots + results in README.

## Phase 8: Benchmarks, docs, CI (Week 5)
- [ ] Throughput 1/2/3/6 workers; note the 6-partition ceiling
- [ ] Batch-size effect; hardware/config documented
- [ ] Detection quality table
- [ ] README per spec checklist (section 10), `make up` + `make demo`
- [ ] CI (GitHub Actions): lint, unit, integration
**Gate:** fresh clone -> `make up && make demo` works; CI green.

## Bug-prevention checkpoints (run at every gate)
1. `make lint test` green
2. `docker compose down -v && up` from clean state works
3. `SELECT count(*), count(DISTINCT event_id) FROM events` equal
4. Committed offsets <= high watermark and lag returns to ~0 after load stops
5. No unexpected ERROR logs; DLQ count equals the injected malformed count
