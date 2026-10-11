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


def test_api_router_endpoints_and_error_handling():
    from web.server import api_router

    # Health probe
    res_health = api_router.dispatch_get("/api/v1/health")
    assert res_health is not None
    assert res_health.status_code == 200
    assert b"healthy" in res_health.serialize()

    # Stats
    res_stats = api_router.dispatch_get("/api/v1/stats")
    assert res_stats is not None
    assert res_stats.status_code == 200
    assert b"rate_ops" in res_stats.serialize()

    # Logs
    res_logs = api_router.dispatch_get("/api/v1/logs?limit=10")
    assert res_logs is not None
    assert res_logs.status_code == 200

    # Alerts
    res_alerts = api_router.dispatch_get("/api/v1/alerts?limit=5")
    assert res_alerts is not None
    assert res_alerts.status_code == 200

    # Simulate attack valid
    res_sim = api_router.dispatch_post("/api/v1/simulate/attack", b'{"type": "brute_force"}')
    assert res_sim is not None
    assert res_sim.status_code == 200

    # Simulate attack invalid type (422)
    res_sim_bad = api_router.dispatch_post("/api/v1/simulate/attack", b'{"type": "unknown_exploit"}')
    assert res_sim_bad is not None
    assert res_sim_bad.status_code == 422

    # Malformed JSON body (400)
    res_sim_malformed = api_router.dispatch_post("/api/v1/simulate/attack", b'not-json')
    assert res_sim_malformed is not None
    assert res_sim_malformed.status_code == 400

    # Method not allowed (405)
    res_method_not_allowed = api_router.dispatch_get("/api/v1/simulate/attack")
    assert res_method_not_allowed is not None
    assert res_method_not_allowed.status_code == 405

    # Not found (404)
    res_not_found = api_router.dispatch_get("/api/v1/non_existent_route")
    assert res_not_found is not None
    assert res_not_found.status_code == 404

