# Rules.md: Constraints for the AI assistant (and for me)

## 1. Use / Avoid
| Use | Avoid |
|---|---|
| `confluent-kafka`, `pydantic` v2, `psycopg2-binary`, `redis-py`, `prometheus_client`, `numpy`, `pytest`, `testcontainers` | `kafka-python`, ORMs (SQLAlchemy/Django), pandas in the hot path, global mutable state, `print` for logging |
| Pinned versions in `requirements.txt` | Unpinned or "latest" images/packages |
| Env-var config via `common/config.py` | Hardcoded hosts, ports, thresholds |
| `logging` (JSON lines) | Swallowing exceptions with bare `except:` or `pass` |
| Event time (`ts`) for windows | `time.time()`/`datetime.now()` inside detectors |

## 2. Non-negotiable correctness rules
- **R-1 Offsets:** `enable.auto.commit=False` and `enable.auto.offset.store=False`. Commit explicit offsets only after DB commit and DLQ ack.
- **R-2 Idempotency:** every insert uses `ON CONFLICT (event_id, ts) DO NOTHING`. Alerts use deterministic `dedupe_key`.
- **R-3 Primary key includes `ts`** (hypertable constraint). Never "fix" this by dropping partitioning.
- **R-4 DLQ:** include original payload (base64), reason, topic, partition, offset. DLQ messages must be flushed before the offset commit.
- **R-5 Commit offsets for DLQ'd messages too**, or the poison message replays forever.
- **R-6 Rebalance:** implement `on_revoke` (flush, commit, drop state) and `on_assign` (load state). Never leave a batch holding messages from revoked partitions.
- **R-7 Shutdown:** SIGTERM/SIGINT set a flag, loop exits, flush, commit, `consumer.close()`.
- **R-8 Batch size cap:** psycopg2 `execute_values` with `page_size` <= 1000. Postgres allows at most 65,535 bind params per statement (the spec's 5,000 rows x 13 cols = 65,000 is too close).
- **R-9 Detectors:** pure functions/classes with no I/O. Input is an event, output is a list of alerts. Injectable clock-free and unit-testable.
- **R-10 EWMA z-score:** compute z against the **previous** mean/std, then update. (The spec's sample updates the mean first, which understates z.) Guard: warmup, `std > epsilon`, and decide explicitly whether low-side anomalies matter.
- **R-11 Validation:** reject, don't coerce silently: non-UUID `event_id`, unparsable/naive `ts`, `ts` more than 24h in the future, invalid IP, `status` outside 100-599, negative bytes, unknown `action`/`auth_result`, missing required fields, payload > 64 KB, non-JSON/non-UTF-8.
- **R-12 DB outage:** retry with exponential backoff + jitter, but keep calling `poll()` (or `pause()` partitions and keep polling) so the member isn't kicked. Set `max.poll.interval.ms` deliberately. Resume on recovery.
- **R-13 Producer:** `enable.idempotence=True`, `acks=all`, check delivery callbacks, `flush()` on exit, count delivery failures.
- **R-14 Timestamps:** store UTC `TIMESTAMPTZ`. Reject naive timestamps.
- **R-15 Metrics:** labels must be low-cardinality (never `client_id` or `event_id` as a label).

## 3. Error handling model
| Error class | Action |
|---|---|
| Malformed/invalid message | DLQ + `dlq_total{reason}` + continue |
| Transient infra (DB, Redis, Kafka) | Retry with backoff; Redis failure degrades detectors (log + metric), never blocks ingestion |
| Programming error in detector | Catch per-detector, log with offset, increment `detector_errors_total`, continue. Events must still be stored. |
| Unrecoverable (config missing) | Fail fast at startup with a clear message |
Return types: validation returns `Event` or raises `ValidationFailure(reason_code)`, using a fixed set of reason codes.

## 4. AI boundaries
1. Work on **one phase and one task** at a time, as listed in `Memory.md`.
2. Do **not** refactor or rename existing, passing code unless I explicitly ask.
3. Do **not** add dependencies, services, or files outside `Architecture.md` without asking.
4. If a requirement conflicts with these docs, **stop and ask**. Don't guess.
5. Never mark something done without showing the test that proves it.
6. No placeholder/stub implementations that look finished; mark with `TODO(phase-X)` and tell me.
7. Update `Memory.md` at the end of every task.
8. State assumptions explicitly before writing code.

## 5. Definition of Done (per task)
- [ ] Code + unit tests written; `make test` passes
- [ ] Types/linters pass (`ruff`, `mypy` on touched modules)
- [ ] Failure path tested (not only the happy path)
- [ ] Metrics/logging added where the Architecture requires
- [ ] `Memory.md` updated

## 6. Testing rules
- Every detector: warmup, zero variance, boundary threshold, out-of-order events, replay of the same events (alert count unchanged).
- Idempotency test: process same offsets twice; row count unchanged.
- Chaos assertion: `produced == stored_distinct + dlq` for every run.
- Tests must be deterministic: seed RNGs, no wall-clock sleeps for logic (use bounded polling).
- Don't mock Kafka/Postgres in integration tests; use Testcontainers/Compose.

## 7. Commit/Git
Small commits, one per task, message `phase-N: <task>`. Tag each phase gate (`phase-1-done`, etc.).
