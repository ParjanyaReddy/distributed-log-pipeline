import json
from typing import Any

import redis

from common.config import REDIS_URI
from common.logging import get_logger

logger = get_logger("processor.state")


class StateManager:
    """Handles per-client detector state checkpoints in Redis with graceful in-memory fallback."""
    def __init__(self, redis_uri: str = REDIS_URI, dry_run: bool = False):
        self.dry_run = dry_run
        self.client: redis.Redis | None = None
        self.in_memory_store: dict[str, str] = {}

        if not self.dry_run:
            try:
                self.client = redis.from_url(redis_uri, decode_responses=True, socket_timeout=2.0)
                self.client.ping()
                logger.info("Connected to Redis for detector state checkpoints.")
            except Exception as e:
                logger.warning(f"Redis unavailable, falling back to local memory checkpoints: {e}")
                self.client = None

    def save_checkpoint(self, detector_name: str, client_id: str, state_data: dict[str, Any]) -> None:
        key = f"lp:detector:{detector_name}:{client_id}"
        json_val = json.dumps(state_data)

        if self.dry_run or self.client is None:
            self.in_memory_store[key] = json_val
            return

        try:
            # Set with 24h TTL to prevent stale state accumulation
            self.client.setex(name=key, time=86400, value=json_val)
        except Exception as e:
            logger.warning(f"Failed to checkpoint state to Redis for {key}: {e}")
            self.in_memory_store[key] = json_val

    def load_checkpoint(self, detector_name: str, client_id: str) -> dict[str, Any] | None:
        key = f"lp:detector:{detector_name}:{client_id}"

        if self.dry_run or self.client is None:
            val = self.in_memory_store.get(key)
            return json.loads(val) if val else None

        try:
            val = self.client.get(key)
            return json.loads(val) if val else None
        except Exception as e:
            logger.warning(f"Failed to load state from Redis for {key}: {e}")
            val = self.in_memory_store.get(key)
            return json.loads(val) if val else None

    def delete_checkpoint(self, detector_name: str, client_id: str) -> None:
        key = f"lp:detector:{detector_name}:{client_id}"
        self.in_memory_store.pop(key, None)
        if self.client is not None:
            try:
                self.client.delete(key)
            except Exception:
                pass
