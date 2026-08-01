"""Consume toll traffic events from Kafka and load them into MySQL."""

from __future__ import annotations

import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import mysql.connector
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from mysql.connector import MySQLConnection


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TollEvent:
    event_timestamp: datetime
    vehicle_id: int
    vehicle_type: str
    toll_plaza_id: int


def parse_event(message: ConsumerRecord) -> TollEvent:
    raw_value = message.value.decode("utf-8")
    fields = raw_value.split(",")

    if len(fields) != 4:
        raise ValueError(
            f"Expected exactly 4 fields, but found "
            f"{len(fields)}: {raw_value!r}"
        )

    timestamp_text, vehicle_id_text, vehicle_type, plaza_id_text = fields

    return TollEvent(
        event_timestamp=datetime.strptime(
            timestamp_text,
            "%Y-%m-%d %H:%M:%S",
        ),
        vehicle_id=int(vehicle_id_text),
        vehicle_type=vehicle_type.strip(),
        toll_plaza_id=int(plaza_id_text),
    )


def create_database_connection() -> MySQLConnection:
    password = os.getenv("MYSQL_PASSWORD")

    if not password:
        raise RuntimeError(
            "MYSQL_PASSWORD environment variable is required."
        )

    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "tolldata"),
        user=os.getenv("MYSQL_USER", "root"),
        password=password,
        autocommit=False,
    )


def main() -> int:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )
    topic = os.getenv("KAFKA_TOPIC", "toll")
    group_id = os.getenv(
        "KAFKA_CONSUMER_GROUP",
        "toll-database-writer",
    )

    connection: Optional[MySQLConnection] = None
    consumer: Optional[KafkaConsumer] = None
    running = True

    def stop_gracefully(
        signum: int,
        frame: object,
    ) -> None:
        nonlocal running
        running = False
        LOGGER.info("Shutdown requested.")

    signal.signal(signal.SIGINT, stop_gracefully)
    signal.signal(signal.SIGTERM, stop_gracefully)

    try:
        LOGGER.info("Connecting to MySQL.")
        connection = create_database_connection()
        cursor = connection.cursor()

        LOGGER.info("Connecting to Kafka at %s.", bootstrap_servers)
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        insert_sql = """
            INSERT INTO livetolldata (
                event_timestamp,
                vehicle_id,
                vehicle_type,
                toll_plaza_id
            )
            VALUES (%s, %s, %s, %s)
        """

        LOGGER.info("Reading events from topic %s.", topic)

        while running:
            message_batches = consumer.poll(
                timeout_ms=1_000,
                max_records=100,
            )

            for records in message_batches.values():
                for message in records:
                    try:
                        event = parse_event(message)

                        cursor.execute(
                            insert_sql,
                            (
                                event.event_timestamp,
                                event.vehicle_id,
                                event.vehicle_type,
                                event.toll_plaza_id,
                            ),
                        )

                        connection.commit()
                        consumer.commit()

                        LOGGER.info(
                            "Inserted vehicle %s from partition %s, "
                            "offset %s.",
                            event.vehicle_id,
                            message.partition,
                            message.offset,
                        )

                    except (ValueError, mysql.connector.Error):
                        connection.rollback()
                        LOGGER.exception(
                            "Failed to process partition %s, offset %s.",
                            message.partition,
                            message.offset,
                        )

        return 0

    except Exception:
        LOGGER.exception("Streaming consumer terminated unexpectedly.")
        return 1

    finally:
        if consumer is not None:
            consumer.close()

        if connection is not None and connection.is_connected():
            connection.close()

        LOGGER.info("Consumer resources closed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    sys.exit(main())