import pytest
from datetime import datetime, timezone
from uuid import uuid4
from common.models import Event
from processor.enrich import enrich_event, DOMAIN_CATEGORY_MAP


def test_enrich_known_domain():
    event = Event(
        event_id=uuid4(),
        ts=datetime.now(timezone.utc),
        client_id="client-0001",
        src_ip="10.0.1.5",
        method="GET",
        url="https://github.com/org/repo",
        domain="github.com",
        status=200,
        bytes_out=500,
        bytes_in=10000,
        user_agent="git/2.40.1",
        action="allowed",
        category="unknown",
        auth_result="none"
    )

    enriched = enrich_event(event)
    assert enriched.category == "development"
    assert enriched.domain == "github.com"


def test_enrich_domain_normalization():
    event = Event(
        event_id=uuid4(),
        ts=datetime.now(timezone.utc),
        client_id="client-0002",
        src_ip="192.168.1.1",
        method="GET",
        url="https://SLACK.COM./messages",
        domain="SLACK.COM.",
        status=200,
        bytes_out=150,
        bytes_in=500,
        user_agent="Mozilla/5.0",
        action="allowed",
        category="general",
        auth_result="none"
    )

    enriched = enrich_event(event)
    assert enriched.domain == "slack.com"
    assert enriched.category == "collaboration"


def test_enrich_unknown_domain_preserves_category():
    event = Event(
        event_id=uuid4(),
        ts=datetime.now(timezone.utc),
        client_id="client-0003",
        src_ip="172.16.0.50",
        method="GET",
        url="https://custom-internal-site.local/index",
        domain="custom-internal-site.local",
        status=200,
        bytes_out=100,
        bytes_in=1000,
        user_agent="Mozilla/5.0",
        action="allowed",
        category="intranet",
        auth_result="none"
    )

    enriched = enrich_event(event)
    assert enriched.category == "intranet"
