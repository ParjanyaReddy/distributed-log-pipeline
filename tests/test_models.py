from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from common.models import Event


def make_valid_event_dict():
    return {
        "event_id": str(uuid4()),
        "ts": datetime.now(UTC).isoformat(),
        "client_id": "client-0001",
        "src_ip": "10.0.1.25",
        "method": "GET",
        "url": "https://example.com/api/v1/resource",
        "domain": "example.com",
        "status": 200,
        "bytes_out": 450,
        "bytes_in": 12000,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "action": "allowed",
        "category": "technology",
        "auth_result": "success",
    }


def test_valid_event():
    data = make_valid_event_dict()
    event = Event(**data)
    assert event.status == 200
    assert event.client_id == "client-0001"
    assert event.action == "allowed"


def test_invalid_status_out_of_range():
    data = make_valid_event_dict()
    data["status"] = 999
    with pytest.raises(ValidationError):
        Event(**data)


def test_negative_bytes():
    data = make_valid_event_dict()
    data["bytes_out"] = -10
    with pytest.raises(ValidationError):
        Event(**data)


def test_invalid_action():
    data = make_valid_event_dict()
    data["action"] = "not_an_action"
    with pytest.raises(ValidationError):
        Event(**data)


def test_future_timestamp():
    data = make_valid_event_dict()
    data["ts"] = (datetime.now(UTC) + timedelta(days=2)).isoformat()
    with pytest.raises(ValidationError):
        Event(**data)
