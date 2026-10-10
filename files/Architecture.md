# Architecture.md

## 1. Data flow
```
generator (N clients, key=client_id) -> Kafka raw-logs (6 partitions)
   -> processor workers (group "processors")
        validate --bad--> raw-logs.dlq
        enrich -> detectors -> batch
        flush: DLQ flush -> DB txn (events + alerts) -> publish alerts -> commit offsets
   -> TimescaleDB (events, alerts) ; Redis (detector checkpoints)
   -> Prometheus -> Grafana
```

## 2. Tech stack (decisions)
| Layer | Choice |
|---|---|
| Bus | Apache Kafka, KRaft mode, 1 broker (3 for chaos #3) |
| Client lib | `confluent-kafka` |
| Validation | `pydantic` v2 models (JSON Schema exportable) |
| Storage | TimescaleDB (Postgres image), `psycopg2-binary` + `execute_values` |
| State | Redis (detector checkpoints) |
| Detection | pure Python + `numpy`; scikit-learn only as stretch |
| Metrics | `prometheus_client`, Kafka exporter, Prometheus, Grafana |
| Tests | `pytest`, `testcontainers`, custom load generator |
Pin every dependency version in `requirements.txt` when first installed (no unpinned installs).

## 3. Topics
| Topic | Partitions | Key | Notes |
|---|---|---|---|
| raw-logs | 6 | client_id | per-client ordering; hot-partition trade-off documented |
| raw-logs.dlq | 1 | source topic-partition-offset | value = JSON {payload_b64, reason, topic, partition, offset, ts} |
| alerts | 3 | client_id | consumers must tolerate duplicates (dedupe_key) |

## 4. Folder structure
```
log-pipeline/
├── docs/                 # these 6 .md files (or in root)
├── docker-compose.yml
├── common/               # config.py, models.py (pydantic), metrics.py, logging.py
├── producers/            # generator.py, profiles.py
├── processor/
│   ├── main.py           # consumer loop, signals, rebalance callbacks
│   ├── validate.py  enrich.py  dlq.py  sink.py  state.py
│   └── detectors/        # base.py brute_force.py volume_spike.py rare_domain.py beaconing.py
├── db/schema.sql
├── dashboards/  bench/  tests/{unit,integration,chaos}/
├── Makefile  requirements.txt  README.md
```

## 5. Database schema (corrected from spec)
**Spec bug:** `event_id UUID PRIMARY KEY` fails on a Timescale hypertable or partitioned table. Unique constraints must include the partition column (`ts`). Use a composite key; idempotency still works because `ts` is the event's own timestamp, identical on replay.
```sql
CREATE TABLE events (
  event_id UUID NOT NULL, ts TIMESTAMPTZ NOT NULL, client_id TEXT NOT NULL,
  src_ip INET, method TEXT, domain TEXT, url TEXT, status INT,
  bytes_out BIGINT, bytes_in BIGINT, action TEXT, category TEXT, auth_result TEXT,
  PRIMARY KEY (event_id, ts)
);
SELECT create_hypertable('events','ts');
CREATE INDEX ON events (client_id, ts DESC);
CREATE INDEX ON events (domain, ts DESC);

CREATE TABLE alerts (
  id BIGSERIAL, ts TIMESTAMPTZ NOT NULL, type TEXT NOT NULL, severity TEXT NOT NULL,
  client_id TEXT, dedupe_key TEXT NOT NULL, details JSONB,
  PRIMARY KEY (dedupe_key, ts)
);
```
(`ts DESC` single index is redundant on a hypertable; Timescale creates one by default.)
Insert: `INSERT ... ON CONFLICT (event_id, ts) DO NOTHING`.
Alert `dedupe_key` = `type|client_id|window_start`. It must be deterministic so replays collapse.

## 6. Processing contract (the heart of correctness)
Per flush (batch >= N rows or age >= T seconds):
1. `dlq_producer.flush()`. All DLQ messages for this batch must be acknowledged before step 4.
2. One DB transaction: insert events + alerts (both `ON CONFLICT DO NOTHING`); commit txn.
3. Publish alerts to `alerts` topic and flush.
4. Commit Kafka offsets **explicitly**: for each partition, `last_processed_offset + 1`, including offsets of messages sent to the DLQ. `enable.auto.commit=False`, `enable.auto.offset.store=False`.
5. Clear batch; checkpoint detector state to Redis (after the commit).

Never commit an offset for a message not yet durably in DB or DLQ.

## 7. Crash analysis (goes in README)
| Crash point | Result |
|---|---|
| Before DB commit | No offset commit, replay, no loss |
| After DB commit, before offset commit | Replay, duplicate inserts ignored by PK |
| After alert publish, before offset commit | Replay, alert re-published; dedupe_key makes consumers idempotent |
| DLQ produce fails/unacked | Do not commit; retry or crash |
| Rebalance mid-batch | `on_revoke` flushes + commits before partitions are released |
| DB down | Retry with backoff; keep polling alive (see Rules R-12) |

## 8. Detector state design
- State is per `client_id`, in memory, valid because partition key = client_id.
- On `on_assign`: load checkpoints from Redis for assigned partitions' clients. On `on_revoke`: flush then drop in-memory state for those partitions.
- Replays may double-feed detectors. Outputs stay correct because alerts are deduplicated and detection is time-windowed on **event time**.
- Windows use event time (`ts`), with an allowed lateness (default 30s); older events are counted in a metric and skipped by detectors (still stored).

## 9. Ports/config
Kafka internal `kafka:9092` (containers) and external `localhost:9094` (host tools). Set `KAFKA_ADVERTISED_LISTENERS` accordingly. All settings via env vars read in `common/config.py`, with no hardcoded hosts.
