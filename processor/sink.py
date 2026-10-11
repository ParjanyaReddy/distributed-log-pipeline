import random
import time
from collections.abc import Callable
from typing import Any

import psycopg2
from psycopg2.extras import Json, execute_values

from common.config import DB_URI, MAX_BATCH_SIZE
from common.logging import get_logger
from common.metrics import BATCH_WRITE_SECONDS, EVENTS_WRITTEN
from common.models import Event

logger = get_logger("processor.sink")

EVENTS_INSERT_SQL = """
INSERT INTO events (
    event_id, ts, client_id, src_ip, method, domain, url,
    status, bytes_out, bytes_in, action, category, auth_result
) VALUES %s
ON CONFLICT (event_id, ts) DO NOTHING
"""

ALERTS_INSERT_SQL = """
INSERT INTO alerts (
    ts, type, severity, client_id, dedupe_key, details
) VALUES %s
ON CONFLICT (dedupe_key, ts) DO NOTHING
"""


class DatabaseSink:
    def __init__(
        self,
        db_uri: str = DB_URI,
        page_size: int = min(MAX_BATCH_SIZE, 1000),
        max_retries: int = 5,
        dry_run: bool = False
    ):
        self.db_uri = db_uri
        self.page_size = min(page_size, 1000)  # Rule R-8: page_size <= 1000
        self.max_retries = max_retries
        self.dry_run = dry_run
        self.conn: Any | None = None

        # For dry_run testing
        self.in_memory_events: list[tuple] = []
        self.in_memory_alerts: list[tuple] = []

    def get_connection(self) -> Any:
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.db_uri)
            self.conn.autocommit = False
        return self.conn

    def close(self) -> None:
        if self.conn is not None and not self.conn.closed:
            self.conn.close()

    def write_batch(
        self,
        events: list[Event],
        alerts: list[dict[str, Any]] | None = None,
        keep_alive_fn: Callable[[], None] | None = None
    ) -> None:
        """Writes events and alerts in a single database transaction with retry backoff."""
        if not events and not alerts:
            return

        if self.dry_run:
            self._write_dry_run(events, alerts)
            return

        # Prepare tuples for events
        event_tuples = [
            (
                str(e.event_id),
                e.ts,
                e.client_id,
                e.src_ip,
                e.method,
                e.domain,
                e.url,
                e.status,
                e.bytes_out,
                e.bytes_in,
                e.action,
                e.category,
                e.auth_result
            )
            for e in events
        ]

        # Prepare tuples for alerts
        alert_tuples = []
        if alerts:
            alert_tuples = [
                (
                    a["ts"],
                    a["type"],
                    a["severity"],
                    a.get("client_id"),
                    a["dedupe_key"],
                    Json(a.get("details", {}))
                )
                for a in alerts
            ]

        attempt = 0
        backoff_sec = 0.5

        while True:
            try:
                with BATCH_WRITE_SECONDS.time():
                    conn = self.get_connection()
                    with conn.cursor() as cur:
                        if event_tuples:
                            execute_values(
                                cur,
                                EVENTS_INSERT_SQL,
                                event_tuples,
                                page_size=self.page_size
                            )
                        if alert_tuples:
                            execute_values(
                                cur,
                                ALERTS_INSERT_SQL,
                                alert_tuples,
                                page_size=self.page_size
                            )
                    conn.commit()

                EVENTS_WRITTEN.inc(len(events))
                return

            except (psycopg2.OperationalError, psycopg2.DatabaseError) as err:
                attempt += 1
                if self.conn and not self.conn.closed:
                    try:
                        self.conn.rollback()
                    except Exception as rollback_err:
                        logger.warning(f"Connection rollback failed during retry: {rollback_err}")
                    self.conn.close()
                self.conn = None

                logger.error(f"Database write attempt {attempt} failed: {err}")
                if self.max_retries and attempt >= self.max_retries:
                    raise

                # Exponential backoff with jitter (Rule R-12)
                sleep_duration = min(backoff_sec * (2 ** (attempt - 1)) + random.uniform(0, 0.5), 30.0)
                logger.info(f"Retrying database write in {sleep_duration:.2f}s...")
                
                # Keep consumer alive during backoff
                sleep_end = time.time() + sleep_duration
                while time.time() < sleep_end:
                    if keep_alive_fn:
                        keep_alive_fn()
                    time.sleep(min(0.2, sleep_end - time.time()))

    def _write_dry_run(
        self,
        events: list[Event],
        alerts: list[dict[str, Any]] | None = None
    ) -> None:
        """Simulates ON CONFLICT DO NOTHING in-memory for testing."""
        # Simulated composite primary key for events: (event_id, ts)
        existing_event_keys = {(ev[0], ev[1]) for ev in self.in_memory_events}
        for e in events:
            key = (str(e.event_id), e.ts)
            if key not in existing_event_keys:
                self.in_memory_events.append((str(e.event_id), e.ts, e.client_id, e))
                existing_event_keys.add(key)

        # Simulated composite primary key for alerts: (dedupe_key, ts)
        if alerts:
            existing_alert_keys = {(al[0], al[1]) for al in self.in_memory_alerts}
            for a in alerts:
                key = (a["dedupe_key"], a["ts"])
                if key not in existing_alert_keys:
                    self.in_memory_alerts.append((a["dedupe_key"], a["ts"], a))
                    existing_alert_keys.add(key)

        EVENTS_WRITTEN.inc(len(events))
