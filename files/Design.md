# Design.md: Style guide

This project has no web UI of its own. "Design" covers Grafana dashboards, alert output, logs, and README visuals, so everything is consistent and readable.

## 1. Color & theme (dark mode default)
| Token | Hex | Use |
|---|---|---|
| bg | #0F1419 | Dashboard background |
| panel | #161B22 | Panel background |
| text | #E6EDF3 | Primary text |
| muted | #8B949E | Axis, labels |
| accent | #2F81F7 | Throughput, primary series |
| ok | #3FB950 | Healthy / written events |
| warn | #D29922 | Medium severity, lag rising |
| crit | #F85149 | High/critical alerts, DLQ errors |
| info | #A371F7 | Secondary series (p95 latency) |

**Severity mapping:** low = muted/blue, medium = warn, high = crit, critical = crit + bold. Color is never the only signal; also show the severity text.

## 2. Fonts
- Dashboards: Grafana default (Inter). Code/README: monospace (JetBrains Mono / Fira Code).
- Alert JSON and log lines: monospace.

## 3. Dashboard layout (top to bottom)
1. Stat row: events/sec, p95 latency, total lag, DLQ rate, alerts (last 15 min)
2. Throughput: produced vs consumed vs written (one panel, 3 series)
3. Consumer lag per partition (6 series)
4. Latency: p50/p95/p99 of `processing_latency_seconds`
5. Alerts by type (stacked bars) + recent-alerts table
6. DLQ by reason + detector errors + late events

Conventions: same time range and 5s refresh across panels; units set on every panel (ops/s, s, bytes); thresholds visible as lines; titles state what and unit ("Consumer lag (messages)").

## 4. Typography & spacing (docs)
- README headings: H1 title, H2 sections, H3 subsections. No skipped levels.
- Tables for comparisons/results; fenced blocks with language tags.
- Diagrams: ASCII or Mermaid, max width 100 chars.
- Screenshots: 1600px wide, dark theme, file names `chaos-01-kill-consumer.png`.

## 5. Alert output format (JSON, one object per alert)
```json
{"dedupe_key":"brute_force|user-0421|2026-05-14T10:21:00Z","ts":"...","type":"brute_force","severity":"high","client_id":"user-0421","details":{"failures":12,"window_s":60,"threshold":5}}
```
Always include threshold and observed value so an alert is explainable.

## 6. Logging format
JSON lines: `{"ts","level","component","msg", ...context}`. Include partition/offset on errors. No PII beyond simulated data.

## 7. Naming
- Metrics: `snake_case`, unit suffix (`_seconds`, `_total`).
- Topics: `kebab-case` with `.dlq` suffix. Python: PEP 8. Env vars: `UPPER_SNAKE` prefixed `LP_`.
