from typing import Any

from common.logging import get_logger
from common.metrics import ALERTS_TOTAL, DETECTOR_ERRORS
from common.models import Event
from processor.detectors.base import BaseDetector
from processor.detectors.beaconing import BeaconingDetector
from processor.detectors.brute_force import BruteForceDetector
from processor.detectors.rare_domain import RareDomainDetector
from processor.detectors.volume_spike import VolumeSpikeDetector
from processor.state import StateManager

logger = get_logger("processor.detectors.engine")


class AnomalyEngine:
    """Orchestrates all anomaly detectors with strict per-detector error isolation."""
    def __init__(self, state_manager: StateManager | None = None):
        self.state_manager = state_manager or StateManager(dry_run=True)
        self.detectors: list[BaseDetector] = [
            BruteForceDetector(),
            VolumeSpikeDetector(),
            RareDomainDetector(),
            BeaconingDetector()
        ]

    def evaluate_event(self, event: Event) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []

        for detector in self.detectors:
            try:
                detector_alerts = detector.process_event(event)
                for alert in detector_alerts:
                    # Increment metric
                    ALERTS_TOTAL.labels(
                        type=alert["type"],
                        severity=alert["severity"]
                    ).inc()
                    alerts.append(alert)
            except Exception as e:
                # Per-detector error isolation (Rules.md Error Handling Model)
                DETECTOR_ERRORS.labels(detector=detector.name).inc()
                logger.error(
                    f"Detector {detector.name} threw error on event {event.event_id}: {e}",
                    exc_info=True
                )

        return alerts

    def checkpoint_state(self, client_id: str) -> None:
        """Saves state across all detectors for a client to Redis."""
        for detector in self.detectors:
            state = detector.get_state()
            if client_id in state:
                self.state_manager.save_checkpoint(detector.name, client_id, state[client_id])

    def restore_state(self, client_id: str) -> None:
        """Restores state across all detectors for a client from Redis."""
        for detector in self.detectors:
            saved = self.state_manager.load_checkpoint(detector.name, client_id)
            if saved:
                detector.set_state({client_id: saved})
