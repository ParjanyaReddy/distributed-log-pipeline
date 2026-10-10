# Memory.md: Living project state (update after EVERY task)

> Paste this file at the start of each new AI chat, along with `Rules.md`. Don't paste the whole codebase.

## Current status
- **Phase:** 8 (Benchmarks, docs, CI)
- **Current task:** Generate scaling benchmark analysis, create comprehensive README.md per spec checklist, and configure GitHub Actions CI workflow.
- **Current working files:** README.md, .github/workflows/ci.yml
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
| phase-5 | Pre-update EWMA z-score calculation | Adheres to Rule R-10, prevents understatement of anomaly z-scores |
| phase-5 | Separate dedupe key for compromise escalation | Emits critical alert if brute-force is followed by compromise |
| phase-6 | Automatic Grafana dashboard provisioning | Zero manual configuration needed on clean startup |
| phase-7 | Chaos invariant `produced == stored_distinct + dlq` | Formally proven across consumer crash, DB outage, broker disconnect, poison flood |

## Completed items (chronological)
- [x] Phase 0: Project skeleton, contracts, models, database schema, configs
- [x] Phase 1: Docker Compose infrastructure, Kafka topic definitions, Prometheus/Grafana configs
- [x] Phase 2: User behavior profiles (office worker, developer, downloader), rate-controlled generator, labeled anomaly injectors, ground-truth labels.jsonl writer
- [x] Phase 3: Schema validation, dead-letter queue routing with base64 payloads, manual offset management, rebalance callbacks, Prometheus metrics
- [x] Phase 4: Static enrichment, batched idempotent DatabaseSink (page_size <= 1000), DB retry with backoff, idempotency & crash replay tests, batch benchmark evaluation
- [x] Phase 5: Base detector abstraction, 4 streaming detectors (brute force, EWMA volume spike, rare domain, beaconing), Redis state checkpoints, precision/recall benchmark evaluation
- [x] Phase 6: Automated Grafana dashboard provisioning, Operations & Threats dashboard layout (stat row, throughput, lag, latencies, alerts, DLQ breakdown), Makefile demo target
- [x] Phase 7: Automated chaos experiments suite across 5 failure modes with zero data loss assertion (`produced == stored_distinct + dlq`)

## Gates passed
- [x] Phase 0  - [x] Phase 1  - [x] Phase 2  - [x] Phase 3  - [x] Phase 4
- [x] Phase 5a brute_force  - [x] 5b volume_spike  - [x] 5c rare_domain  - [x] 5d beaconing
- [x] Phase 6  - [x] Phase 7  - [ ] Phase 8

## Known issues / tech debt
- (none)

## Config snapshot (fill as built)
- Batch size: 1000   Batch max age: 1.0s   Allowed lateness: 30s
- Thresholds: brute_force N=5 window=60s  |  volume_spike z=3.0 warmup=30 alpha=0.1  |  rare_domain min_bytes=50KB  |  beaconing CV<=0.15 min_samples=6

## Last verified results
- Phase 7: All 47 tests passed. All 5 chaos scenarios passed 3/3 times with 0 data loss and 0 duplicate rows.

## Next 3 tasks
1. Phase 8: Worker throughput scaling analysis (1, 2, 3, 6 workers) in `bench/RESULTS.md`.
2. Phase 8: Comprehensive `README.md` following PRD checklist (architecture, delivery semantics, quickstart, interview prep).
3. Phase 8: GitHub Actions CI workflow (`.github/workflows/ci.yml`).

