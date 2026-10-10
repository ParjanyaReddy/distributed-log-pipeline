# Memory.md: Living project state (update after EVERY task)

> Paste this file at the start of each new AI chat, along with `Rules.md`. Don't paste the whole codebase.

## Current status
- **Phase:** 4 (Storage sink)
- **Current task:** Implement batched idempotent PostgreSQL/TimescaleDB writer (`processor/sink.py`), static enrichment (`processor/enrich.py`), and DB retry backoff.
- **Current working files:** processor/sink.py, processor/enrich.py
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

## Completed items (chronological)
- [x] Phase 0: Project skeleton, contracts, models, database schema, configs
- [x] Phase 1: Docker Compose infrastructure, Kafka topic definitions, Prometheus/Grafana configs
- [x] Phase 2: User behavior profiles (office worker, developer, downloader), rate-controlled generator, labeled anomaly injectors, ground-truth labels.jsonl writer
- [x] Phase 3: Schema validation, dead-letter queue routing with base64 payloads, manual offset management, rebalance callbacks, Prometheus metrics

## Gates passed
- [x] Phase 0  - [x] Phase 1  - [x] Phase 2  - [x] Phase 3  - [ ] Phase 4
- [ ] Phase 5a brute_force  - [ ] 5b volume_spike  - [ ] 5c rare_domain  - [ ] 5d beaconing
- [ ] Phase 6  - [ ] Phase 7  - [ ] Phase 8

## Known issues / tech debt
- (none)

## Config snapshot (fill as built)
- Batch size: 1000   Batch max age: 1.0s   Allowed lateness: 30s
- Thresholds: brute_force N=? window=?  |  z=3.0 warmup=30  |  beacon CV<? min samples=?

## Last verified results
- Phase 3: All 27 tests passed. Validated 10k valid + 100 malformed stream gate: exact 10k in sink, 100 in DLQ, all offsets committed, 0 uncommitted on shutdown.

## Next 3 tasks
1. Phase 4: Implement enrichment module for domain categories and IP parsing (`processor/enrich.py`).
2. Phase 4: Implement batched idempotent PostgreSQL/TimescaleDB sink with `execute_values` (`processor/sink.py`).
3. Phase 4: Integration and idempotency tests with database replay verification.

