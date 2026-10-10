from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from common.logging import get_logger
from common.metrics import LATE_EVENTS
from common.models import Event

logger = get_logger("processor.detectors.base")


class BaseDetector(ABC):
    def __init__(self, name: str, allowed_lateness_sec: float = 30.0):
        self.name = name
        self.allowed_lateness_sec = allowed_lateness_sec
        self.watermark_ts: datetime | None = None

    def is_late(self, event_ts: datetime) -> bool:
        """Checks if event timestamp is too far behind the observed watermark."""
        if self.watermark_ts is None:
            self.watermark_ts = event_ts
            return False

        if event_ts > self.watermark_ts:
            self.watermark_ts = event_ts
            return False

        lateness = (self.watermark_ts - event_ts).total_seconds()
        if lateness > self.allowed_lateness_sec:
            LATE_EVENTS.inc()
            return True
        return False

    @abstractmethod
    def process_event(self, event: Event) -> list[dict[str, Any]]:
        """Evaluates an event using event-time logic. Returns a list of alert dicts."""

    @abstractmethod
    def get_state(self) -> dict[str, Any]:
        """Serializes internal state for Redis checkpointing."""

    @abstractmethod
    def set_state(self, state: dict[str, Any]) -> None:
        """Restores internal state from a Redis checkpoint."""
