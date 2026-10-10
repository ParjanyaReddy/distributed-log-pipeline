# Memory.md: Living project state (update after EVERY task)

> Paste this file at the start of each new AI chat, along with `Rules.md`. Don't paste the whole codebase.

## Current status
- **Phase:** 2 (Producer)
- **Current task:** Build the load generator with behavior profiles and anomaly injection.
- **Current working files:** producers/profiles.py, producers/generator.py
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

## Completed items (chronological)
- [ ] (none)

## Gates passed
- [x] Phase 0  - [x] Phase 1  - [ ] Phase 2  - [ ] Phase 3  - [ ] Phase 4
- [ ] Phase 5a brute_force  - [ ] 5b volume_spike  - [ ] 5c rare_domain  - [ ] 5d beaconing
- [ ] Phase 6  - [ ] Phase 7  - [ ] Phase 8

## Known issues / tech debt
- (none)

## Config snapshot (fill as built)
- Batch size: ?   Batch max age: ?   Allowed lateness: 30s
- Thresholds: brute_force N=? window=?  |  z=3.0 warmup=30  |  beacon CV<? min samples=?

## Last verified results
- Throughput: ?   p95 latency: ?   Precision/recall: ?

## Next 3 tasks
1. ...
2. ...
3. ...
