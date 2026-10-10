# Memory.md: Living project state (update after EVERY task)

> Paste this file at the start of each new AI chat, along with `Rules.md`. Don't paste the whole codebase.

## Current status
- **Phase:** 5 (Anomaly detectors)
- **Current task:** Implement base detector interface, state management in Redis, and brute force detector (Phase 5a).
- **Current working files:** processor/detectors/base.py, processor/detectors/brute_force.py, processor/state.py
- **Blocked on:** nothing

## Decisions log (don't re-debate without cause)
| Date | Decision | Reason |
|---|---|---|
| start | Python 3.11 + confluent-kafka | Fast to build, reliable client |
| start | TimescaleDB | Simpler partitioning/retention |
| start | PK = (event_id, ts) | Hypertables require partition column in unique keys |
| start | At-least-once + idempotent sink | Exactly-once is a non-goal |
| start | Alerts deduped via deterministic `dedupe_key` | Replays must not duplicate alerts |
| start | Z-score uses previous mean/std | Avoid understated z from post-update mean |
| phase-2 | Deterministic seeded PRNG per client | Reproducible evaluation & load generation |
| phase-3 | Explicit manual offset commit per partition | Strict at-least-once: offsets advance only after flush & DLQ ack |
| phase-4 | Single DB transaction for events & alerts | Atomicity; ON CONFLICT DO NOTHING handles replay idempotency |

## Completed items (chronological)
- [x] Phase 0: Project skeleton, contracts, models, database schema, configs
- [x] Phase 1: Docker Compose infrastructure, Kafka topic definitions, Prometheus/Grafana configs
- [x] Phase 2: User behavior profiles (office worker, developer, downloader), rate-controlled generator, labeled anomaly injectors, ground-truth labels.jsonl writer
- [x] Phase 3: Schema validation, dead-letter queue routing with base64 payloads, manual offset management, rebalance callbacks, Prometheus metrics
- [x] Phase 4: Static enrichment, batched idempotent DatabaseSink (page_size <= 1000), DB retry with backoff, idempotency & crash replay tests, batch benchmark evaluation

## Gates passed
- [x] Phase 0  - [x] Phase 1  - [x] Phase 2  - [x] Phase 3  - [x] Phase 4
- [ ] Phase 5a brute_force  - [ ] 5b volume_spike  - [ ] 5c rare_domain  - [ ] 5d beaconing
- [ ] Phase 6  - [ ] Phase 7  - [ ] Phase 8

## Known issues / tech debt
- (none)

## Config snapshot (fill as built)
- Batch size: 1000   Batch max age: 1.0s   Allowed lateness: 30s
- Thresholds: brute_force N=? window=?  |  z=3.0 warmup=30  |  beacon CV<? min samples=?

## Last verified results
- Phase 4: All 33 tests passed. Idempotency replay test verified 0 duplicate rows on replay. Benchmarked batch size 1 (6,169 ev/s) vs 500 (627,833 ev/s) vs 1,000 (642,018 ev/s).

## Next 3 tasks
1. Phase 5a: Implement detector base class and sliding window brute force detector (`processor/detectors/brute_force.py`).
2. Phase 5: Per-client state checkpoints in Redis with assign/revoke rebalance handlers (`processor/state.py`).
3. Phase 5b: Implement EWMA z-score volume spike detector (`processor/detectors/volume_spike.py`).

