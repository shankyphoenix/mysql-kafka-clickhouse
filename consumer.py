"""
consumer.py
-----------
Generic Kafka → ClickHouse consumer.
Driven entirely by config.py — no table-specific logic here.

Run one process per topic:
    python consumer.py --topic companies_systemwise
    python consumer.py --topic contacts

Batching: flush on BATCH_SIZE records OR BATCH_TIMEOUT_SECS, whichever first.
"""

import argparse
import json
import logging
import os
import signal
import time
from datetime import datetime

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

# Load .env from the same directory as this script
_base_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_base_dir, ".env"))

from config import PIPELINES

# ---------------------------------------------------------------------------
# Settings from environment
# ---------------------------------------------------------------------------
KAFKA_BROKERS       = os.getenv("KAFKA_BROKERS",      "16.58.103.76:9094")
KAFKA_GROUP_PREFIX  = os.getenv("KAFKA_GROUP_PREFIX",  "clickhouse-consumer")
KAFKA_AUTO_OFFSET   = os.getenv("KAFKA_AUTO_OFFSET",   "earliest")

CLICKHOUSE_HOST     = os.getenv("CLICKHOUSE_HOST",     "localhost")
CLICKHOUSE_PORT     = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB       = os.getenv("CLICKHOUSE_DB",       "default")
CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER",     "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

BATCH_SIZE          = int(os.getenv("BATCH_SIZE",         "1000"))
BATCH_TIMEOUT_SECS  = float(os.getenv("BATCH_TIMEOUT_SECS", "5"))
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("kafka_consumer")


# ---------------------------------------------------------------------------
# Field mapping helper
# ---------------------------------------------------------------------------

def apply_field_map(payload: dict, field_map: dict) -> dict:
    """Rename keys in payload according to field_map."""
    if not field_map:
        return payload
    return {field_map.get(k, k): v for k, v in payload.items()}


# ---------------------------------------------------------------------------
# ClickHouse flush
# ---------------------------------------------------------------------------

def flush_to_clickhouse(client, table: str, columns: list, batch: list) -> None:
    if not batch:
        return
    start = time.monotonic()
    client.insert(
        table=table,
        data=batch,
        column_names=columns,
        database=CLICKHOUSE_DB,
    )
    log.info("Flushed %d rows → %s in %.2fs", len(batch), table, time.monotonic() - start)


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run(topic: str) -> None:
    if topic not in PIPELINES:
        raise SystemExit(
            f"Topic '{topic}' not found in PIPELINES config.\n"
            f"Available: {', '.join(PIPELINES.keys())}"
        )

    pipeline = PIPELINES[topic]
    ch_table  = pipeline["clickhouse_table"]
    columns   = pipeline["columns"]
    coerce    = pipeline["coerce"]
    field_map = pipeline.get("field_map", {})
    group_id  = f"{KAFKA_GROUP_PREFIX}-{topic}"

    log.info("Starting consumer | topic=%s → CH table=%s | group=%s", topic, ch_table, group_id)

    # --- ClickHouse ---
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    log.info("Connected to ClickHouse at %s:%s", CLICKHOUSE_HOST, CLICKHOUSE_PORT)

    # --- Kafka ---
    consumer = Consumer({
        "bootstrap.servers":         KAFKA_BROKERS,
        "group.id":                  group_id,
        "auto.offset.reset":         KAFKA_AUTO_OFFSET,
        "enable.auto.commit":        False,
        "max.poll.interval.ms":      300000,
        "session.timeout.ms":        30000,
        "heartbeat.interval.ms":     10000,
        "fetch.max.bytes":           52428800,
        "max.partition.fetch.bytes": 10485760,
    })
    consumer.subscribe([topic])

    # --- Graceful shutdown ---
    running = True

    def _shutdown(sig, frame):
        nonlocal running
        log.info("[%s] Shutdown signal received", topic)
        running = False

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    batch: list[list] = []
    last_flush = time.monotonic()
    error_count = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("[%s] End of partition %d", topic, msg.partition())
                else:
                    log.error("[%s] Kafka error: %s", topic, msg.error())
                    error_count += 1
                    if error_count > 10:
                        log.critical("[%s] Too many errors — exiting", topic)
                        break
            else:
                error_count = 0
                try:
                    envelope = json.loads(msg.value().decode("utf-8"))
                    action   = envelope.get("action", "upsert")
                    payload  = envelope.get("payload", {})

                    if action == "upsert":
                        mapped = apply_field_map(payload, field_map)
                        row    = coerce(mapped)
                        batch.append(row)
                    elif action == "delete":
                        log.debug("[%s] Skipping delete id=%s", topic, payload.get("id"))
                    else:
                        log.warning("[%s] Unknown action: %s", topic, action)

                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    log.error("[%s] Parse error: %s | msg: %s", topic, e, msg.value()[:200])

            # Flush on size OR timeout
            now = time.monotonic()
            if batch and (len(batch) >= BATCH_SIZE or (now - last_flush) >= BATCH_TIMEOUT_SECS):
                try:
                    flush_to_clickhouse(ch_client, ch_table, columns, batch)
                    consumer.commit(asynchronous=False)
                    batch = []
                    last_flush = time.monotonic()
                except Exception as e:
                    log.error("[%s] Flush failed: %s — retaining batch", topic, e)

    finally:
        if batch:
            log.info("[%s] Final flush: %d records", topic, len(batch))
            try:
                flush_to_clickhouse(ch_client, ch_table, columns, batch)
                consumer.commit(asynchronous=False)
            except Exception as e:
                log.error("[%s] Final flush failed: %s", topic, e)

        consumer.close()
        log.info("[%s] Consumer shut down cleanly", topic)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka → ClickHouse consumer")
    parser.add_argument(
        "--topic",
        required=True,
        choices=list(PIPELINES.keys()),
        help="Kafka topic / pipeline to consume",
    )
    args = parser.parse_args()
    run(args.topic)
