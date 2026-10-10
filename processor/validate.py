import ipaddress
import json
from datetime import datetime, timezone
from uuid import UUID
from typing import Any, Dict, Union

from common.models import Event, ReasonCode, ValidationFailure

MAX_PAYLOAD_BYTES = 64 * 1024  # 64 KB
VALID_ACTIONS = {"allowed", "blocked"}
VALID_AUTH_RESULTS = {"success", "failure", "none"}
REQUIRED_FIELDS = [
    "event_id", "ts", "client_id", "src_ip", "method", "url",
    "domain", "status", "bytes_out", "bytes_in", "user_agent",
    "action", "category", "auth_result"
]


def validate_event_payload(payload_bytes: bytes) -> Event:
    """Validates raw message bytes against the strict pipeline contract.
    
    Raises ValidationFailure with an explicit ReasonCode if invalid.
    Returns the validated Event model instance on success.
    """
    if len(payload_bytes) > MAX_PAYLOAD_BYTES:
        raise ValidationFailure(ReasonCode.PAYLOAD_TOO_LARGE, f"Size {len(payload_bytes)} > {MAX_PAYLOAD_BYTES}")

    try:
        decoded_text = payload_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ValidationFailure(ReasonCode.INVALID_JSON, f"Non-UTF-8 payload: {e}")

    try:
        data = json.loads(decoded_text)
    except json.JSONDecodeError as e:
        raise ValidationFailure(ReasonCode.INVALID_JSON, f"Malformed JSON: {e}")

    if not isinstance(data, dict):
        raise ValidationFailure(ReasonCode.INVALID_JSON, "Payload must be a JSON object")

    # Check required fields
    for field_name in REQUIRED_FIELDS:
        if field_name not in data or data[field_name] is None:
            raise ValidationFailure(ReasonCode.MISSING_REQUIRED_FIELD, f"Missing field: {field_name}")

    # Validate UUID
    try:
        UUID(str(data["event_id"]))
    except (ValueError, AttributeError, TypeError):
        raise ValidationFailure(ReasonCode.INVALID_UUID, f"Invalid UUID: {data['event_id']}")

    # Validate IP address
    try:
        ipaddress.ip_address(str(data["src_ip"]))
    except ValueError:
        raise ValidationFailure(ReasonCode.INVALID_IP, f"Invalid IP address: {data['src_ip']}")

    # Validate timestamp
    raw_ts = data["ts"]
    if isinstance(raw_ts, str):
        try:
            parsed_ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValidationFailure(ReasonCode.NAIVE_TIMESTAMP, f"Unparsable timestamp: {e}")
    elif isinstance(raw_ts, datetime):
        parsed_ts = raw_ts
    else:
        raise ValidationFailure(ReasonCode.NAIVE_TIMESTAMP, "Timestamp must be ISO-8601 string")

    if parsed_ts.tzinfo is None:
        raise ValidationFailure(ReasonCode.NAIVE_TIMESTAMP, "Timestamp must have timezone")

    now = datetime.now(timezone.utc)
    if (parsed_ts - now).total_seconds() > 86400:
        raise ValidationFailure(ReasonCode.FUTURE_TIMESTAMP, "Timestamp is > 24 hours in the future")

    # Validate HTTP status
    try:
        status_code = int(data["status"])
        if not (100 <= status_code <= 599):
            raise ValidationFailure(ReasonCode.STATUS_OUT_OF_RANGE, f"Status code {status_code} out of range")
    except (ValueError, TypeError):
        raise ValidationFailure(ReasonCode.STATUS_OUT_OF_RANGE, f"Invalid status: {data['status']}")

    # Validate bytes
    try:
        bytes_out = int(data["bytes_out"])
        bytes_in = int(data["bytes_in"])
        if bytes_out < 0 or bytes_in < 0:
            raise ValidationFailure(ReasonCode.NEGATIVE_BYTES, f"Bytes cannot be negative: out={bytes_out}, in={bytes_in}")
    except (ValueError, TypeError):
        raise ValidationFailure(ReasonCode.NEGATIVE_BYTES, "Byte counts must be valid integers")

    # Validate action
    if data["action"] not in VALID_ACTIONS:
        raise ValidationFailure(ReasonCode.UNKNOWN_ACTION, f"Invalid action: {data['action']}")

    # Validate auth_result
    if data["auth_result"] not in VALID_AUTH_RESULTS:
        raise ValidationFailure(ReasonCode.UNKNOWN_AUTH_RESULT, f"Invalid auth_result: {data['auth_result']}")

    # Construct and return validated Event
    return Event(**data)
