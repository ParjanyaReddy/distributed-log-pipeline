import os

KAFKA_BROKERS = os.getenv("LP_KAFKA_BROKERS", "localhost:9094")
DB_URI = os.getenv("LP_DB_URI", "postgresql://postgres:postgres@localhost:5432/postgres")
REDIS_URI = os.getenv("LP_REDIS_URI", "redis://localhost:6379/0")

MAX_BATCH_SIZE = int(os.getenv("LP_MAX_BATCH_SIZE", "1000"))
BATCH_TIMEOUT_SEC = float(os.getenv("LP_BATCH_TIMEOUT_SEC", "1.0"))
