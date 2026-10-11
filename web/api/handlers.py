"""Controller action handlers for all pipeline telemetry and simulation endpoints."""

from __future__ import annotations

import json
import time
from typing import Any

from common.config import ENVIRONMENT
from common.logging import get_logger
from web.api.base import APIResponse, error_response, json_response
from web.services.bridge import WebPipelineBridge

logger = get_logger("web.api.handlers")
_START_TIME = time.time()


def handle_get_stats(bridge: WebPipelineBridge, query_params: dict[str, Any]) -> APIResponse:
    """GET /api/v1/stats and /api/stats: Return cluster and ingestion metrics."""
    try:
        data = bridge.get_stats_snapshot()
        return json_response(data)
    except Exception as exc:
        logger.error(f"Error fetching stats: {exc}", exc_info=True)
        return error_response("Failed to retrieve pipeline statistics", status_code=500, code="INTERNAL_ERROR")


def handle_get_logs(bridge: WebPipelineBridge, query_params: dict[str, Any]) -> APIResponse:
    """GET /api/v1/logs and /api/logs: Return buffered recent logs with optional limit."""
    try:
        limit = 50
        if "limit" in query_params:
            try:
                limit = max(1, min(int(query_params["limit"][0]), 200))
            except (ValueError, IndexError):
                pass
        logs = bridge.get_recent_logs(limit=limit)
        return json_response(logs)
    except Exception as exc:
        logger.error(f"Error fetching logs: {exc}", exc_info=True)
        return error_response("Failed to retrieve log stream", status_code=500, code="INTERNAL_ERROR")


def handle_get_alerts(bridge: WebPipelineBridge, query_params: dict[str, Any]) -> APIResponse:
    """GET /api/v1/alerts and /api/alerts: Return buffered security alerts."""
    try:
        limit = 30
        if "limit" in query_params:
            try:
                limit = max(1, min(int(query_params["limit"][0]), 100))
            except (ValueError, IndexError):
                pass
        alerts = bridge.get_recent_alerts(limit=limit)
        return json_response(alerts)
    except Exception as exc:
        logger.error(f"Error fetching alerts: {exc}", exc_info=True)
        return error_response("Failed to retrieve alert stream", status_code=500, code="INTERNAL_ERROR")


def handle_post_simulate_attack(bridge: WebPipelineBridge, body: bytes) -> APIResponse:
    """POST /api/v1/simulate/attack and /api/simulate/attack: Inject synthetic adversary scenario."""
    try:
        req_data = json.loads(body.decode("utf-8")) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return error_response("Invalid JSON payload", status_code=400, code="INVALID_JSON", details=str(exc))

    attack_type = req_data.get("type", "brute_force") if isinstance(req_data, dict) else "brute_force"
    valid_attacks = {"brute_force", "volume_spike", "rare_domain", "beaconing"}

    if attack_type not in valid_attacks:
        return error_response(
            f"Invalid attack type '{attack_type}'. Must be one of: {sorted(list(valid_attacks))}",
            status_code=422,
            code="UNPROCESSABLE_ENTITY",
        )

    try:
        result = bridge.trigger_anomaly(attack_type)
        return json_response(result)
    except Exception as exc:
        logger.error(f"Error triggering attack simulation: {exc}", exc_info=True)
        return error_response("Attack simulation failed", status_code=500, code="SIMULATION_ERROR")


def handle_post_toggle_traffic(bridge: WebPipelineBridge, body: bytes) -> APIResponse:
    """POST /api/v1/traffic/toggle and /api/traffic/toggle: Pause or resume stream generation."""
    try:
        status = bridge.toggle_streaming()
        return json_response({
            "streaming_active": status,
            "status": "active" if status else "paused",
        })
    except Exception as exc:
        logger.error(f"Error toggling streaming traffic: {exc}", exc_info=True)
        return error_response("Failed to toggle stream state", status_code=500, code="INTERNAL_ERROR")


def handle_get_health(bridge: WebPipelineBridge, query_params: dict[str, Any]) -> APIResponse:
    """GET /api/v1/health: Liveness and service readiness probe."""
    uptime = round(time.time() - _START_TIME, 2)
    return json_response({
        "status": "healthy",
        "service": "apex-stream-soc",
        "version": "2.4.0",
        "uptime_seconds": uptime,
        "environment": ENVIRONMENT,
        "streaming_active": bridge.streaming_active,
    })
