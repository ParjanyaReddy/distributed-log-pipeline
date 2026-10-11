"""Scalable API Router Implementation.

Manages route registration, path matching, query parameter parsing,
and request dispatching for both versioned and legacy API endpoints.
"""

from __future__ import annotations

from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from web.api.base import APIResponse, error_response
from web.api.handlers import (
    handle_get_alerts,
    handle_get_health,
    handle_get_logs,
    handle_get_stats,
    handle_post_simulate_attack,
    handle_post_toggle_traffic,
)
from web.services.bridge import WebPipelineBridge


class APIRouter:
    """Lightweight, scalable REST API router."""

    def __init__(self, bridge: WebPipelineBridge) -> None:
        self.bridge = bridge
        self._get_routes: dict[str, Callable[[WebPipelineBridge, dict[str, Any]], APIResponse]] = {}
        self._post_routes: dict[str, Callable[[WebPipelineBridge, bytes], APIResponse]] = {}

    def get(self, path: str, handler: Callable[[WebPipelineBridge, dict[str, Any]], APIResponse]) -> None:
        """Registers a GET endpoint."""
        self._get_routes[path] = handler

    def post(self, path: str, handler: Callable[[WebPipelineBridge, bytes], APIResponse]) -> None:
        """Registers a POST endpoint."""
        self._post_routes[path] = handler

    def dispatch_get(self, raw_path: str) -> APIResponse | None:
        """Dispatches a GET request if route exists; returns None if route is not an API path."""
        parsed = urlparse(raw_path)
        path = parsed.path.rstrip("/")
        if not path:
            path = "/"

        if path in self._get_routes:
            query_params = parse_qs(parsed.query)
            return self._get_routes[path](self.bridge, query_params)

        # If it looks like an API route but is not registered
        if path.startswith("/api/"):
            if path in self._post_routes:
                return error_response(f"Method GET not allowed for {path}", status_code=405, code="METHOD_NOT_ALLOWED")
            return error_response(f"Route not found: {path}", status_code=404, code="NOT_FOUND")

        return None

    def dispatch_post(self, raw_path: str, body: bytes) -> APIResponse | None:
        """Dispatches a POST request if route exists; returns None if not an API path."""
        parsed = urlparse(raw_path)
        path = parsed.path.rstrip("/")
        if not path:
            path = "/"

        if path in self._post_routes:
            return self._post_routes[path](self.bridge, body)

        if path.startswith("/api/"):
            if path in self._get_routes:
                return error_response(f"Method POST not allowed for {path}", status_code=405, code="METHOD_NOT_ALLOWED")
            return error_response(f"Route not found: {path}", status_code=404, code="NOT_FOUND")

        return None


def create_default_router(bridge: WebPipelineBridge) -> APIRouter:
    """Factory creating and registering all standard API routes (versioned v1 and backwards compatible)."""
    router = APIRouter(bridge)

    # Health & Metrics
    router.get("/api/v1/health", handle_get_health)
    router.get("/api/health", handle_get_health)

    # Telemetry and Statistics
    router.get("/api/v1/stats", handle_get_stats)
    router.get("/api/stats", handle_get_stats)

    # Log Ring Buffer
    router.get("/api/v1/logs", handle_get_logs)
    router.get("/api/logs", handle_get_logs)

    # Anomaly Alerts
    router.get("/api/v1/alerts", handle_get_alerts)
    router.get("/api/alerts", handle_get_alerts)

    # Adversary Simulation
    router.post("/api/v1/simulate/attack", handle_post_simulate_attack)
    router.post("/api/simulate/attack", handle_post_simulate_attack)
    router.post("/api/v1/attacks/simulate", handle_post_simulate_attack)

    # Stream Traffic Control
    router.post("/api/v1/traffic/toggle", handle_post_toggle_traffic)
    router.post("/api/traffic/toggle", handle_post_toggle_traffic)
    router.post("/api/v1/stream/toggle", handle_post_toggle_traffic)

    return router
