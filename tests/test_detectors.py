from datetime import UTC, datetime, timedelta
from uuid import uuid4

from common.models import Event
from processor.detectors.beaconing import BeaconingDetector
from processor.detectors.brute_force import BruteForceDetector
from processor.detectors.engine import AnomalyEngine
from processor.detectors.rare_domain import RareDomainDetector
from processor.detectors.volume_spike import VolumeSpikeDetector


def make_event(
    client_id: str = "client-0001",
    domain: str = "example.com",
    bytes_out: int = 500,
    auth_result: str = "none",
    category: str = "general",
    ts: datetime | None = None
) -> Event:
    return Event(
        event_id=uuid4(),
        ts=ts or datetime.now(UTC),
        client_id=client_id,
        src_ip="10.0.0.1",
        method="POST" if auth_result != "none" else "GET",
        url=f"https://{domain}/path",
        domain=domain,
        status=200 if auth_result != "failure" else 401,
        bytes_out=bytes_out,
        bytes_in=1000,
        user_agent="Mozilla/5.0",
        action="allowed" if auth_result != "failure" else "blocked",
        category=category,
        auth_result=auth_result
    )


# -------------------------------------------------------------
# 1. Brute Force Detector Tests
# -------------------------------------------------------------

def test_brute_force_detection():
    detector = BruteForceDetector(failure_threshold=5, window_seconds=60.0)
    base_ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)

    # 4 failures: no alert yet
    for i in range(4):
        ev = make_event(client_id="user-1", auth_result="failure", ts=base_ts + timedelta(seconds=i * 2))
        alerts = detector.process_event(ev)
        assert len(alerts) == 0

    # 5th failure: triggers alert
    ev5 = make_event(client_id="user-1", auth_result="failure", ts=base_ts + timedelta(seconds=8))
    alerts = detector.process_event(ev5)
    assert len(alerts) == 1
    assert alerts[0]["type"] == "brute_force"
    assert alerts[0]["severity"] == "high"


def test_brute_force_compromise_escalation():
    detector = BruteForceDetector(failure_threshold=3, window_seconds=60.0)
    base_ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)

    for i in range(3):
        detector.process_event(make_event(client_id="user-2", auth_result="failure", ts=base_ts + timedelta(seconds=i)))

    # Success event following brute force escalates to critical
    ev_success = make_event(client_id="user-2", auth_result="success", ts=base_ts + timedelta(seconds=4))
    alerts = detector.process_event(ev_success)
    # The subsequent alert window marks severity as critical
    assert any(a["severity"] == "critical" or a.get("details", {}).get("followed_by_success") for a in alerts or [])


# -------------------------------------------------------------
# 2. Volume Spike (EWMA Z-Score) Tests
# -------------------------------------------------------------

def test_volume_spike_warmup_guard():
    detector = VolumeSpikeDetector(alpha=0.1, z_thresh=3.0, warmup=30)
    base_ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)

    # During warmup (< 30 samples), even a massive spike does not alert
    for i in range(15):
        ev = make_event(client_id="user-3", bytes_out=500, ts=base_ts + timedelta(seconds=i))
        detector.process_event(ev)

    spike_event = make_event(client_id="user-3", bytes_out=100_000_000, ts=base_ts + timedelta(seconds=16))
    alerts = detector.process_event(spike_event)
    assert len(alerts) == 0  # Warmup protected


def test_volume_spike_zero_variance_guard():
    detector = VolumeSpikeDetector(alpha=0.1, z_thresh=3.0, warmup=10)
    base_ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)

    # Exactly identical byte counts (variance = 0)
    for i in range(15):
        ev = make_event(client_id="user-4", bytes_out=1000, ts=base_ts + timedelta(seconds=i))
        alerts = detector.process_event(ev)
        assert len(alerts) == 0  # No zero-division error


def test_volume_spike_detection():
    detector = VolumeSpikeDetector(alpha=0.1, z_thresh=3.0, warmup=10)
    base_ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)

    # Feed baseline traffic with natural small variance
    for i in range(20):
        bytes_val = 500 + (i % 5) * 20  # 500 to 580 bytes
        ev = make_event(client_id="user-5", bytes_out=bytes_val, ts=base_ts + timedelta(seconds=i))
        detector.process_event(ev)

    # Massive exfiltration spike
    spike_ev = make_event(client_id="user-5", bytes_out=50_000_000, ts=base_ts + timedelta(seconds=21))
    alerts = detector.process_event(spike_ev)
    assert len(alerts) == 1
    assert alerts[0]["type"] == "volume_spike"
    assert alerts[0]["details"]["z_score"] > 3.0


# -------------------------------------------------------------
# 3. Rare Domain Detector Tests
# -------------------------------------------------------------

def test_rare_domain_detection():
    detector = RareDomainDetector(min_bytes_out=50_000)
    base_ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)

    # Low bytes to new domain: no alert
    ev1 = make_event(client_id="user-6", domain="normal-site.com", bytes_out=500, ts=base_ts)
    assert len(detector.process_event(ev1)) == 0

    # Large transfer to suspicious domain category
    ev2 = make_event(
        client_id="user-6",
        domain="exfil-portal.xyz",
        bytes_out=250_000,
        category="suspicious",
        ts=base_ts + timedelta(seconds=5)
    )
    alerts = detector.process_event(ev2)
    assert len(alerts) == 1
    assert alerts[0]["type"] == "rare_domain"


# -------------------------------------------------------------
# 4. Beaconing Detector Tests
# -------------------------------------------------------------

def test_beaconing_detection():
    detector = BeaconingDetector(min_samples=6, cv_threshold=0.15)
    base_ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)

    # Strict periodic requests every 5.0 seconds (near-zero jitter)
    alerts = []
    for i in range(8):
        ev = make_event(
            client_id="user-7",
            domain="c2-callback.net",
            ts=base_ts + timedelta(seconds=i * 5.0)
        )
        alerts.extend(detector.process_event(ev))

    assert len(alerts) >= 1
    assert alerts[0]["type"] == "beaconing"
    assert alerts[0]["details"]["coefficient_of_variation"] <= 0.15


def test_beaconing_irregular_traffic_no_alert():
    detector = BeaconingDetector(min_samples=6, cv_threshold=0.15)
    base_ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)

    # Irregular human requests (highly variable intervals)
    delays = [1.0, 15.0, 3.0, 45.0, 2.0, 30.0, 8.0, 60.0]
    current_time = base_ts
    alerts = []
    for delay in delays:
        current_time += timedelta(seconds=delay)
        ev = make_event(client_id="user-8", domain="google.com", ts=current_time)
        alerts.extend(detector.process_event(ev))

    # Should not trigger beaconing alert
    assert len(alerts) == 0


# -------------------------------------------------------------
# 5. Engine Error Isolation Test
# -------------------------------------------------------------

def test_anomaly_engine_error_isolation():
    engine = AnomalyEngine()
    ev = make_event(client_id="user-9", bytes_out=1000)
    # Engine processes event across all detectors without raising errors
    alerts = engine.evaluate_event(ev)
    assert isinstance(alerts, list)
