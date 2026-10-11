"""Centralized Environment and Application Configuration Module.

This module provides structured, type-safe settings loaded from environment
variables with optional fallback to local .env configuration files.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


def _load_env_file(filepath: Path | str = ".env") -> None:
    """Lightweight .env file parser to populate os.environ without third-party dependencies."""
    path = Path(filepath)
    if not path.is_file():
        return

    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        # Gracefully handle file read permissions or encoding anomalies
        pass


# Automatically load from local .env if present
_load_env_file()


@dataclass(frozen=True)
class KafkaSettings:
    """Apache Kafka broker and topic configurations."""
    brokers: str = field(default_factory=lambda: os.getenv("LP_KAFKA_BROKERS", "localhost:9094"))
    topic_raw_logs: str = field(default_factory=lambda: os.getenv("LP_TOPIC_RAW_LOGS", "raw-logs"))
    topic_dlq: str = field(default_factory=lambda: os.getenv("LP_TOPIC_DLQ", "raw-logs.dlq"))
    topic_alerts: str = field(default_factory=lambda: os.getenv("LP_TOPIC_ALERTS", "alerts"))


@dataclass(frozen=True)
class DatabaseSettings:
    """Database connection configurations."""
    uri: str = field(
        default_factory=lambda: os.getenv("LP_DB_URI", "postgresql://postgres:postgres@localhost:5432/postgres")
    )


@dataclass(frozen=True)
class RedisSettings:
    """Redis cache and detector state store configurations."""
    uri: str = field(default_factory=lambda: os.getenv("LP_REDIS_URI", "redis://localhost:6379/0"))


@dataclass(frozen=True)
class ProducerSettings:
    """Synthetic log generator and profile simulation settings."""
    rate: float = field(default_factory=lambda: float(os.getenv("LP_PRODUCER_RATE", "500.0")))
    num_clients: int = field(default_factory=lambda: int(os.getenv("LP_NUM_CLIENTS", "350")))
    labels_path: str = field(default_factory=lambda: os.getenv("LP_LABELS_PATH", "labels.jsonl"))
    random_seed: int = field(default_factory=lambda: int(os.getenv("LP_RANDOM_SEED", "42")))


@dataclass(frozen=True)
class ConsumerSettings:
    """Stream processing and batch sink settings."""
    max_batch_size: int = field(default_factory=lambda: int(os.getenv("LP_MAX_BATCH_SIZE", "1000")))
    batch_timeout_sec: float = field(default_factory=lambda: float(os.getenv("LP_BATCH_TIMEOUT_SEC", "1.0")))


@dataclass(frozen=True)
class WebServerSettings:
    """SOC Console and REST API server settings."""
    host: str = field(default_factory=lambda: os.getenv("LP_WEB_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("LP_WEB_PORT", "8080")))
    environment: Literal["development", "production", "testing"] = field(
        default_factory=lambda: os.getenv("LP_ENVIRONMENT", "development").lower()  # type: ignore[return-value]
    )
    log_level: str = field(default_factory=lambda: os.getenv("LP_LOG_LEVEL", "INFO"))


@dataclass(frozen=True)
class AppConfig:
    """Composite application configuration tree."""
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    redis: RedisSettings = field(default_factory=RedisSettings)
    producer: ProducerSettings = field(default_factory=ProducerSettings)
    consumer: ConsumerSettings = field(default_factory=ConsumerSettings)
    web: WebServerSettings = field(default_factory=WebServerSettings)


# Singleton application configuration instance
config = AppConfig()

# ------------------------------------------------------------------------------
# Backwards-compatible constants for legacy imports
# ------------------------------------------------------------------------------
KAFKA_BROKERS = config.kafka.brokers
DB_URI = config.database.uri
REDIS_URI = config.redis.uri

TOPIC_RAW_LOGS = config.kafka.topic_raw_logs
TOPIC_DLQ = config.kafka.topic_dlq
TOPIC_ALERTS = config.kafka.topic_alerts

PRODUCER_RATE = config.producer.rate
NUM_CLIENTS = config.producer.num_clients
LABELS_PATH = config.producer.labels_path
RANDOM_SEED = config.producer.random_seed

MAX_BATCH_SIZE = config.consumer.max_batch_size
BATCH_TIMEOUT_SEC = config.consumer.batch_timeout_sec

WEB_HOST = config.web.host
WEB_PORT = config.web.port
ENVIRONMENT = config.web.environment
