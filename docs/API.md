# Apex Stream SOC Console & Telemetry REST API

## Overview
The Apex Stream REST API exposes real-time telemetry, log streams, anomaly detection alerts, and interactive adversary attack simulation triggers.

The API is structured with both versioned routes (`/api/v1/...`) and backwards-compatible legacy paths (`/api/...`).

---

## Base URL
- Local Development: `http://localhost:8080`
- Configurable via `LP_WEB_HOST` and `LP_WEB_PORT` in `.env`.

---

## Response Envelope & Error Handling Model

### Standard Success Response
JSON responses return HTTP `200 OK` with serialized entity payloads or lists.

### Standard Error Response
When an error occurs, the API returns a structured JSON payload with corresponding HTTP status codes:

```json
{
  "success": false,
  "error": {
    "code": "BAD_REQUEST",
    "message": "Detailed description of what caused the failure",
    "details": null,
    "timestamp": "2026-10-11T12:00:00.000Z"
  }
}
```

### Standard HTTP Status Codes
| HTTP Status | Code String | Condition |
|---|---|---|
| `200 OK` | - | Request completed successfully |
| `400 Bad Request` | `INVALID_JSON` / `BAD_REQUEST` | Malformed JSON request body or missing parameters |
| `404 Not Found` | `NOT_FOUND` | Unregistered API endpoint |
| `405 Method Not Allowed` | `METHOD_NOT_ALLOWED` | HTTP method not supported on the target path |
| `422 Unprocessable Entity`| `UNPROCESSABLE_ENTITY` | Semantic validation failed (e.g. unknown attack type) |
| `500 Internal Error` | `INTERNAL_ERROR` | Unhandled backend exception (logged with stack trace) |

---

## Endpoints Reference

### 1. Health Probe
- **Endpoint:** `GET /api/v1/health` (alias: `GET /api/health`)
- **Description:** Returns service status, uptime, and streaming generator state.
- **Sample Response:**
```json
{
  "status": "healthy",
  "service": "apex-stream-soc",
  "version": "2.4.0",
  "uptime_seconds": 128.45,
  "environment": "development",
  "streaming_active": true
}
```

---

### 2. Pipeline Telemetry & Stats
- **Endpoint:** `GET /api/v1/stats` (alias: `GET /api/stats`)
- **Description:** Returns real-time velocity, total ingested, database store counts, DLQ record counts, p95 latency, and 6-shard partition distribution.
- **Sample Response:**
```json
{
  "rate_ops": 12.5,
  "total_consumed": 1420,
  "total_stored": 1418,
  "total_dlq": 2,
  "total_alerts": 5,
  "partition_counts": [235, 240, 230, 245, 232, 238],
  "partition_details": [
    {
      "partition_id": 0,
      "leader": "broker-1",
      "isr": "3/3",
      "high_watermark": 235,
      "count": 235,
      "share_pct": 16.5,
      "status": "HEALTHY"
    }
  ],
  "streaming_active": true,
  "cluster_info": {
    "cluster_id": "kraft-cluster-01",
    "brokers_online": 3,
    "consumer_group": "cg-log-processors",
    "consumer_state": "STABLE",
    "p95_latency_ms": 4.2,
    "consumer_lag": 0
  }
}
```

---

### 3. Log Stream Buffer
- **Endpoint:** `GET /api/v1/logs` (alias: `GET /api/logs`)
- **Query Parameters:**
  - `limit` (integer, optional, default: 50, max: 200): Maximum records to return.
- **Sample Response:**
```json
[
  {
    "event_id": "c1f7a4e0-3921-4f32-8e12-4299bdf908c1",
    "ts": "2026-10-11T12:00:00.125Z",
    "client_id": "client-0042",
    "src_ip": "10.10.1.42",
    "method": "POST",
    "url": "https://auth.internal-corp.net/login",
    "domain": "auth.internal-corp.net",
    "status": 401,
    "bytes_out": 420,
    "bytes_in": 1250,
    "action": "blocked",
    "category": "authentication",
    "auth_result": "failure",
    "partition": 2
  }
]
```

---

### 4. Anomaly Alerts Stream
- **Endpoint:** `GET /api/v1/alerts` (alias: `GET /api/alerts`)
- **Query Parameters:**
  - `limit` (integer, optional, default: 30, max: 100): Maximum alerts to return.
- **Sample Response:**
```json
[
  {
    "type": "brute_force",
    "severity": "critical",
    "client_id": "client-0042",
    "dedupe_key": "bf-client-0042-2026-10-11T12:00:00",
    "ts": "2026-10-11T12:00:00.125Z",
    "details": {
      "failure_count": 8,
      "window_seconds": 60
    }
  }
]
```

---

### 5. Simulate Adversary Attack
- **Endpoint:** `POST /api/v1/simulate/attack` (aliases: `POST /api/simulate/attack`, `POST /api/v1/attacks/simulate`)
- **Request Body (`application/json`):**
```json
{
  "type": "brute_force"
}
```
*Supported types:* `"brute_force"`, `"volume_spike"`, `"rare_domain"`, `"beaconing"`
- **Success Response (200 OK):**
```json
{
  "status": "triggered",
  "type": "brute_force",
  "client_id": "client-0012",
  "label": {
    "technique": "T1110.001",
    "name": "Password Guessing"
  }
}
```
- **Error Response (422 Unprocessable Entity):**
```json
{
  "success": false,
  "error": {
    "code": "UNPROCESSABLE_ENTITY",
    "message": "Invalid attack type 'sql_injection'. Must be one of: ['beaconing', 'brute_force', 'rare_domain', 'volume_spike']",
    "details": null,
    "timestamp": "2026-10-11T12:00:00.000Z"
  }
}
```

---

### 6. Toggle Traffic Streaming
- **Endpoint:** `POST /api/v1/traffic/toggle` (aliases: `POST /api/traffic/toggle`, `POST /api/v1/stream/toggle`)
- **Request Body:** None or `{}`
- **Success Response (200 OK):**
```json
{
  "streaming_active": false,
  "status": "paused"
}
```
