from web.server import bridge


def test_bridge_initialization_and_traffic():
    assert bridge is not None
    assert len(bridge.clients) == 50
    assert bridge.total_consumed >= 0


def test_bridge_anomaly_injection():
    # Test brute force
    res_bf = bridge.trigger_anomaly("brute_force")
    assert res_bf["status"] == "triggered"
    assert res_bf["type"] == "brute_force"

    # Test volume spike
    res_vs = bridge.trigger_anomaly("volume_spike")
    assert res_vs["status"] == "triggered"
    assert res_vs["type"] == "volume_spike"

    # Test rare domain
    res_rd = bridge.trigger_anomaly("rare_domain")
    assert res_rd["status"] == "triggered"
    assert res_rd["type"] == "rare_domain"

    # Test beaconing
    res_bc = bridge.trigger_anomaly("beaconing")
    assert res_bc["status"] == "triggered"
    assert res_bc["type"] == "beaconing"

    # Buffer should contain logs and alerts
    assert len(bridge.logs_buffer) > 0
    assert len(bridge.alerts_buffer) > 0


def test_bridge_malformed_event_dlq():
    prev_dlq = len(bridge.dlq.in_memory_records)
    # Feed garbage bytes
    bridge.process_raw_event(b"bad-non-json-corrupt-payload", partition=2)
    assert len(bridge.dlq.in_memory_records) == prev_dlq + 1
