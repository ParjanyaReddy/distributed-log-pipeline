from datetime import UTC, datetime

from common.models import Event
from producers.anomalies import (
    inject_beaconing,
    inject_brute_force,
    inject_volume_spike,
)
from producers.profiles import create_population


def test_population_size_and_uniqueness():
    clients = create_population(num_clients=350, seed=42)
    assert len(clients) == 350

    # Ensure client_ids and IPs are unique
    client_ids = set(c.client_id for c in clients)
    src_ips = set(c.src_ip for c in clients)
    assert len(client_ids) == 350
    assert len(src_ips) == 350


def test_client_event_schema_conformance():
    clients = create_population(num_clients=10, seed=123)
    for client in clients:
        for _ in range(20):
            ev_dict = client.generate_event()
            # Must pass Pydantic validation cleanly
            event = Event(**ev_dict)
            assert event.client_id == client.client_id
            assert event.src_ip == client.src_ip


def test_reproducibility_with_seed():
    clients_a = create_population(num_clients=5, seed=99)
    clients_b = create_population(num_clients=5, seed=99)

    for ca, cb in zip(clients_a, clients_b):
        assert ca.client_id == cb.client_id
        assert ca.src_ip == cb.src_ip
        assert ca.profile.name == cb.profile.name
        # Seeded PRNG should generate identical sequences
        ev_a = ca.generate_event(datetime(2026, 1, 1, tzinfo=UTC))
        ev_b = cb.generate_event(datetime(2026, 1, 1, tzinfo=UTC))
        assert ev_a["domain"] == ev_b["domain"]
        assert ev_a["bytes_out"] == ev_b["bytes_out"]


def test_brute_force_anomaly_generation():
    client = create_population(num_clients=1, seed=1)[0]
    start_ts = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    events, label = inject_brute_force(client, start_ts, num_failures=10)

    assert len(events) == 11  # 10 failures + 1 success
    assert label["type"] == "brute_force"
    assert label["client_id"] == client.client_id
    assert label["details"]["failures"] == 10

    # Verify all generated events conform to Event schema
    for ev in events:
        Event(**ev)


def test_volume_spike_anomaly():
    client = create_population(num_clients=1, seed=1)[0]
    ts = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    ev, label = inject_volume_spike(client, ts, bytes_out=50_000_000)

    assert label["type"] == "volume_spike"
    assert ev["bytes_out"] == 50_000_000
    Event(**ev)


def test_beaconing_anomaly():
    client = create_population(num_clients=1, seed=1)[0]
    start_ts = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    events, label = inject_beaconing(client, start_ts, count=10, interval_seconds=3.0)

    assert len(events) == 10
    assert label["type"] == "beaconing"
    for ev in events:
        Event(**ev)
