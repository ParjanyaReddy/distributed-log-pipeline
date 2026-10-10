import argparse

from common.logging import get_logger
from producers.generator import LogGenerator

logger = get_logger("demo")


def run_demo(rate: float = 300.0, duration: float = 60.0):
    logger.info(f"Starting pipeline demonstration: rate={rate} ev/s, duration={duration}s")
    generator = LogGenerator(
        target_rate=rate,
        num_clients=100,
        anomaly_interval_sec=10.0,
        malformed_ratio=0.01,
        dry_run=False
    )
    generator.run(duration_sec=duration)
    logger.info("Pipeline demo completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=300.0)
    parser.add_argument("--duration", type=float, default=60.0)
    args = parser.parse_args()
    run_demo(args.rate, args.duration)
