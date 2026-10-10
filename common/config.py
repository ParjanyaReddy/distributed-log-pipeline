import os

KAFKA_BROKERS = os.getenv("LP_KAFKA_BROKERS", "localhost:9094")
DB_URI = os.getenv("LP_DB_URI", "postgresql://postgres:postgres@localhost:5432/postgres")
REDIS_URI = os.getenv("LP_REDIS_URI", "redis://localhost:6379/0")

# Topic names
TOPIC_RAW_LOGS = os.getenv("LP_TOPIC_RAW_LOGS", "raw-logs")
TOPIC_DLQ = os.getenv("LP_TOPIC_DLQ", "raw-logs.dlq")
TOPIC_ALERTS = os.getenv("LP_TOPIC_ALERTS", "alerts")

# Producer configs
PRODUCER_RATE = float(os.getenv("LP_PRODUCER_RATE", "500.0"))
NUM_CLIENTS = int(os.getenv("LP_NUM_CLIENTS", "350"))
LABELS_PATH = os.getenv("LP_LABELS_PATH", "labels.jsonl")
RANDOM_SEED = int(os.getenv("LP_RANDOM_SEED", "42"))

# Consumer batch configs
MAX_BATCH_SIZE = int(os.getenv("LP_MAX_BATCH_SIZE", "1000"))
BATCH_TIMEOUT_SEC = float(os.getenv("LP_BATCH_TIMEOUT_SEC", "1.0"))

