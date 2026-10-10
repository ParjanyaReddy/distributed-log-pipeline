from prometheus_client import Counter, Histogram, Gauge

# Message processing counters
EVENTS_CONSUMED = Counter(
    "events_consumed_total",
    "Total number of events consumed from raw-logs"
)

EVENTS_WRITTEN = Counter(
    "events_written_total",
    "Total number of events successfully written to the database"
)

DLQ_TOTAL = Counter(
    "dlq_total",
    "Total number of invalid events sent to the dead-letter queue",
    ["reason"]
)

# Latency and performance histograms
BATCH_WRITE_SECONDS = Histogram(
    "batch_write_seconds",
    "Time taken to write a batch to the database",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)

PROCESSING_LATENCY_SECONDS = Histogram(
    "processing_latency_seconds",
    "End-to-end processing latency from event timestamp to offset commit",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

# Anomaly detection and alerting metrics
ALERTS_TOTAL = Counter(
    "alerts_total",
    "Total number of anomaly alerts triggered",
    ["type", "severity"]
)

DETECTOR_ERRORS = Counter(
    "detector_errors_total",
    "Total number of exceptions caught inside detector evaluations",
    ["detector"]
)

LATE_EVENTS = Counter(
    "late_events_total",
    "Total number of events dropped from real-time windows due to excessive lateness"
)
