from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from producers.profiles import SimulatedClient


def inject_brute_force(
    client: SimulatedClient,
    start_ts: datetime,
    num_failures: int = 12,
    interval_seconds: float = 2.0
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Injects a brute-force authentication sequence followed by a success."""
    events = []
    current_ts = start_ts
    domain = "auth.internal-corp.net"

    for i in range(num_failures):
        event = {
            "event_id": str(uuid4()),
            "ts": current_ts.isoformat(),
            "client_id": client.client_id,
            "src_ip": client.src_ip,
            "method": "POST",
            "url": f"https://{domain}/login",
            "domain": domain,
            "status": 401,
            "bytes_out": 420,
            "bytes_in": 1250,
            "user_agent": "Python-requests/2.31.0",
            "action": "blocked",
            "category": "authentication",
            "auth_result": "failure"
        }
        events.append(event)
        current_ts += timedelta(seconds=interval_seconds)

    # Follow with a success indicating potential credential compromise
    success_event = {
        "event_id": str(uuid4()),
        "ts": current_ts.isoformat(),
        "client_id": client.client_id,
        "src_ip": client.src_ip,
        "method": "POST",
        "url": f"https://{domain}/login",
        "domain": domain,
        "status": 200,
        "bytes_out": 450,
        "bytes_in": 3400,
        "user_agent": "Python-requests/2.31.0",
        "action": "allowed",
        "category": "authentication",
        "auth_result": "success"
    }
    events.append(success_event)

    label = {
        "type": "brute_force",
        "client_id": client.client_id,
        "start_ts": start_ts.isoformat(),
        "end_ts": current_ts.isoformat(),
        "details": {
            "failures": num_failures,
            "domain": domain,
            "followed_by_success": True
        }
    }
    return events, label


def inject_volume_spike(
    client: SimulatedClient,
    ts: datetime,
    bytes_out: int = 75_000_000
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Injects an enormous outbound data transfer (data exfiltration)."""
    domain = "mega-cloud-drop.io"
    event = {
        "event_id": str(uuid4()),
        "ts": ts.isoformat(),
        "client_id": client.client_id,
        "src_ip": client.src_ip,
        "method": "POST",
        "url": f"https://{domain}/upload/archive.tar.gz",
        "domain": domain,
        "status": 200,
        "bytes_out": bytes_out,
        "bytes_in": 250,
        "user_agent": "curl/8.2.1",
        "action": "allowed",
        "category": "cloud_storage",
        "auth_result": "none"
    }
    label = {
        "type": "volume_spike",
        "client_id": client.client_id,
        "start_ts": ts.isoformat(),
        "end_ts": ts.isoformat(),
        "details": {
            "bytes_out": bytes_out,
            "domain": domain
        }
    }
    return event, label


def inject_rare_domain(
    client: SimulatedClient,
    ts: datetime,
    domain: str = "super-sketchy-pastebin.ru"
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Injects access to a suspicious / low-reputation or rare domain with large payload."""
    event = {
        "event_id": str(uuid4()),
        "ts": ts.isoformat(),
        "client_id": client.client_id,
        "src_ip": client.src_ip,
        "method": "POST",
        "url": f"https://{domain}/drop",
        "domain": domain,
        "status": 200,
        "bytes_out": 250_000,
        "bytes_in": 500,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "action": "allowed",
        "category": "suspicious",
        "auth_result": "none"
    }
    label = {
        "type": "rare_domain",
        "client_id": client.client_id,
        "start_ts": ts.isoformat(),
        "end_ts": ts.isoformat(),
        "details": {
            "domain": domain,
            "bytes_out": 250_000
        }
    }
    return event, label


def inject_beaconing(
    client: SimulatedClient,
    start_ts: datetime,
    count: int = 15,
    interval_seconds: float = 5.0,
    domain: str = "c2-command-callback.net"
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Injects periodic heartbeat requests with near-zero jitter to simulate C2 beaconing."""
    events = []
    current_ts = start_ts
    for _ in range(count):
        event = {
            "event_id": str(uuid4()),
            "ts": current_ts.isoformat(),
            "client_id": client.client_id,
            "src_ip": client.src_ip,
            "method": "GET",
            "url": f"https://{domain}/api/poll",
            "domain": domain,
            "status": 200,
            "bytes_out": 312,
            "bytes_in": 128,
            "user_agent": "PowerShell/7.3.0",
            "action": "allowed",
            "category": "unknown",
            "auth_result": "none"
        }
        events.append(event)
        current_ts += timedelta(seconds=interval_seconds)

    label = {
        "type": "beaconing",
        "client_id": client.client_id,
        "start_ts": start_ts.isoformat(),
        "end_ts": (current_ts - timedelta(seconds=interval_seconds)).isoformat(),
        "details": {
            "domain": domain,
            "count": count,
            "interval_seconds": interval_seconds
        }
    }
    return events, label


def make_malformed_event(malform_type: str = "invalid_json") -> Any:
    """Generates a message guaranteed to fail validation for DLQ testing."""
    if malform_type == "invalid_json":
        return "{this is not valid json! bad syntax"
    elif malform_type == "status_999":
        return {
            "event_id": str(uuid4()),
            "ts": datetime.now(UTC).isoformat(),
            "client_id": "malformed-client",
            "src_ip": "1.2.3.4",
            "method": "GET",
            "url": "http://bad.com",
            "domain": "bad.com",
            "status": 999,  # invalid status
            "bytes_out": 100,
            "bytes_in": 100,
            "user_agent": "test",
            "action": "allowed",
            "category": "test",
            "auth_result": "none"
        }
    elif malform_type == "negative_bytes":
        return {
            "event_id": str(uuid4()),
            "ts": datetime.now(UTC).isoformat(),
            "client_id": "malformed-client",
            "src_ip": "1.2.3.4",
            "method": "GET",
            "url": "http://bad.com",
            "domain": "bad.com",
            "status": 200,
            "bytes_out": -500,  # negative
            "bytes_in": 100,
            "user_agent": "test",
            "action": "allowed",
            "category": "test",
            "auth_result": "none"
        }
    elif malform_type == "missing_required":
        return {
            "client_id": "malformed-client",
            "status": 200
        }
    else:
        return "malformed raw payload string"
