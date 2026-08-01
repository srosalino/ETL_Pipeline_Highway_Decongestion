"""Generate simulated toll traffic events and publish them to Kafka."""

from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


LOGGER = logging.getLogger(__name__)

DEFAULT_VEHICLE_TYPES = (
    ["car"] * 11
    + ["truck"] * 4
    + ["van"] * 2
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate simulated toll traffic events."
    )

    parser.add_argument(
        "--message-count",
        type=int,
        default=20,
        help="Number of messages to generate.",
    )

    parser.add_argument(
        "--max-delay",
        type=float,
        default=2.0,
        help="Maximum delay in seconds between messages.",
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )
    topic = os.getenv("KAFKA_TOPIC", "toll")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=5,
    )

    interrupted = False

    def stop_gracefully(
        signum: int,
        frame: object,
    ) -> None:
        nonlocal interrupted
        interrupted = True
        LOGGER.info("Shutdown requested; finishing pending messages.")

    signal.signal(signal.SIGINT, stop_gracefully)
    signal.signal(signal.SIGTERM, stop_gracefully)

    try:
        for _ in range(args.message_count):
            if interrupted:
                break

            vehicle_id = random.randint(10_000, 10_000_000)
            vehicle_type = random.choice(DEFAULT_VEHICLE_TYPES)
            toll_plaza_id = random.randint(4_000, 4_010)

            event_timestamp = datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")

            message = (
                f"{event_timestamp},"
                f"{vehicle_id},"
                f"{vehicle_type},"
                f"{toll_plaza_id}"
            )

            future = producer.send(topic, value=message)

            try:
                metadata = future.get(timeout=10)
            except KafkaError:
                LOGGER.exception("Failed to publish message: %s", message)
                return 1

            LOGGER.info(
                "Published %s event for vehicle %s to "
                "partition %s, offset %s.",
                vehicle_type,
                vehicle_id,
                metadata.partition,
                metadata.offset,
            )

            time.sleep(random.random() * args.max_delay)

    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)

    LOGGER.info("Producer stopped successfully.")
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    sys.exit(main())