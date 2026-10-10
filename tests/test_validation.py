import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from common.models import ReasonCode, ValidationFailure
from processor.validate import validate_event_payload


def get_base_valid_dict():
    return {
        "event_id": str(uuid4()),
        "ts": datetime.now(UTC).isoformat(),
        "client_id": "client-0042",
        "src_ip": "192.168.1.100",
        "method": "GET",
        "url": "https://service.corp/api",
        "domain": "service.corp",
        "status": 200,
        "bytes_out": 300,
        "bytes_in": 1500,
        "user_agent": "Mozilla/5.0",
        "action": "allowed",
        "category": "productivity",
        "auth_result": "none"
    }


def test_valid_payload():
    data = get_base_valid_dict()
    payload = json.dumps(data).encode("utf-8")
    event = validate_event_payload(payload)
    assert str(event.event_id) == data["event_id"]
    assert event.status == 200


def test_invalid_json_syntax():
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(b"{not valid json")
    assert exc_info.value.reason_code == ReasonCode.INVALID_JSON


def test_payload_too_large():
    huge_payload = b"x" * (65 * 1024)
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(huge_payload)
    assert exc_info.value.reason_code == ReasonCode.PAYLOAD_TOO_LARGE


def test_missing_required_field():
    data = get_base_valid_dict()
    del data["auth_result"]
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.MISSING_REQUIRED_FIELD


def test_invalid_uuid():
    data = get_base_valid_dict()
    data["event_id"] = "not-a-valid-uuid-1234"
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.INVALID_UUID


def test_invalid_ip():
    data = get_base_valid_dict()
    data["src_ip"] = "999.888.777.666"
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.INVALID_IP


def test_naive_timestamp():
    data = get_base_valid_dict()
    data["ts"] = "2026-05-14T12:00:00"  # No timezone indicator
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.NAIVE_TIMESTAMP


def test_future_timestamp():
    data = get_base_valid_dict()
    far_future = datetime.now(UTC) + timedelta(days=5)
    data["ts"] = far_future.isoformat()
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.FUTURE_TIMESTAMP


def test_status_out_of_range():
    data = get_base_valid_dict()
    data["status"] = 99
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.STATUS_OUT_OF_RANGE


def test_negative_bytes():
    data = get_base_valid_dict()
    data["bytes_out"] = -5
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.NEGATIVE_BYTES


def test_unknown_action():
    data = get_base_valid_dict()
    data["action"] = "ignored"
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.UNKNOWN_ACTION


def test_unknown_auth_result():
    data = get_base_valid_dict()
    data["auth_result"] = "maybe"
    payload = json.dumps(data).encode("utf-8")
    with pytest.raises(ValidationFailure) as exc_info:
        validate_event_payload(payload)
    assert exc_info.value.reason_code == ReasonCode.UNKNOWN_AUTH_RESULT
