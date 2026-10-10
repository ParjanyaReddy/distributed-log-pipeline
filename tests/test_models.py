import pytest
from datetime import datetime, timezone
from uuid import uuid4
from common.models import Event

def test_valid_event():
    event = Event(
        event_id=uuid4(),
        ts=datetime.now(timezone.utc),
        client_id="client-1",
        status=200
    )
    assert event.status == 200

def test_invalid_status():
    with pytest.raises(ValueError):
        Event(
            event_id=uuid4(),
            ts=datetime.now(timezone.utc),
            client_id="client-1",
            status=999
        )
