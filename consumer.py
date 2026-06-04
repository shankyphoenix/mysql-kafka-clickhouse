"""
consumer.py
-----------
Kafka → ClickHouse batch consumer for companies_systemwise / companies_by_system.

Batching strategy: flush when EITHER
  - batch reaches BATCH_SIZE records, OR
  - BATCH_TIMEOUT_SECONDS seconds have elapsed since last flush

ClickHouse upsert uses ReplacingMergeTree — just INSERT; deduplication is
handled by the engine on merge (ORDER BY key matches the MySQL UNIQUE KEY).

For immediate deduplication without waiting for merges, use:
  FINAL keyword on SELECT, or
  call OPTIMIZE TABLE companies_by_system FINAL (expensive, use sparingly)
"""

import json
import logging
import os
import signal
import time
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

# Always load .env from the same directory as this script
_base_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_base_dir, ".env"))

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError, KafkaException

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
KAFKA_BROKERS        = os.getenv("KAFKA_BROKERS",       "16.58.103.76:9094")
KAFKA_TOPIC          = os.getenv("KAFKA_TOPIC",         "companies_systemwise")
KAFKA_GROUP_ID       = os.getenv("KAFKA_GROUP_ID",      "clickhouse-companies-consumer")
KAFKA_AUTO_OFFSET    = os.getenv("KAFKA_AUTO_OFFSET",   "earliest")

CLICKHOUSE_HOST      = os.getenv("CLICKHOUSE_HOST",     "localhost")
CLICKHOUSE_PORT      = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB        = os.getenv("CLICKHOUSE_DB",       "default")
CLICKHOUSE_USER      = os.getenv("CLICKHOUSE_USER",     "default")
CLICKHOUSE_PASSWORD  = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_TABLE     = os.getenv("CLICKHOUSE_TABLE",    "companies_by_system")

BATCH_SIZE           = int(os.getenv("BATCH_SIZE",      "1000"))
BATCH_TIMEOUT_SECS   = float(os.getenv("BATCH_TIMEOUT_SECS", "5"))

LOG_LEVEL            = os.getenv("LOG_LEVEL", "INFO")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("kafka_consumer")

# ---------------------------------------------------------------------------
# Column list — must match ClickHouse table order exactly
# ---------------------------------------------------------------------------
COLUMNS = [
    "id", "is_dedupe_global", "dedupe_global_connect_id",
    "dupe_global_total_cost", "dupe_global_total_sales", "duns_global_count",
    "company_id", "record_hash", "record_hash2", "record_hash3",
    "ils_unique_key", "ils_unique_duns", "table_name", "service_type",
    "company_type", "system_id", "system_unique_id", "system_name",
    "client_specific_name", "customer_unique_key", "dnb_name", "duns",
    "address", "phone_researched", "city", "state", "county", "country",
    "postal_code", "parent_name", "web_address", "dnb_sales_value",
    "dnb_trade_style", "sic", "naics", "latitude", "longitude",
    "line_of_business", "no_of_employees", "google_cust_name", "google_address",
    "google_city", "google_state", "google_country", "google_postal_code",
    "google_phone_researched", "google_web_address",
    "lead_source_1", "lead_source_2", "lead_source_3", "lead_source_4",
    "last_updated", "system_count", "deleted_tracker_id",
    "total_sales", "total_cost", "date_added", "mapped_on",
    "client_type", "distributor_id", "distributor_name",
    "sic_desc", "naics_desc", "solr_synced", "is_cddb_client",
    "interlynx_call_host", "interlynx_rep", "interlynx_am",
    "interlynx_company_type", "supplier_type", "researched_company_name",
    "is_company_mapped", "searched_on_google", "interlynx_name", "is_rep",
    "meta_data", "company_description", "company_keywords",
    "sync_data_to_system_flag", "meta", "total_records",
    "total", "total_quote_value", "total_sale_value", "total_cost_value",
]

# ---------------------------------------------------------------------------
# Type coercion helpers
# ---------------------------------------------------------------------------

def _dt(value) -> Optional[datetime]:
    """Parse MySQL datetime string → Python datetime, or None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    log.warning("Could not parse datetime: %r", value)
    return None


def _int(value) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _float(value) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _str(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dict):
        return json.dumps(value)
    return str(value)


def coerce_record(raw: dict) -> list:
    """
    Convert a raw dict (from Kafka JSON) into an ordered list
    matching COLUMNS, with correct Python types for clickhouse-connect.
    """
    r = raw  # shorthand

    return [
        _int(r.get("id")),
        _int(r.get("is_dedupe_global")),
        _str(r.get("dedupe_global_connect_id")),
        _float(r.get("dupe_global_total_cost")),
        _float(r.get("dupe_global_total_sales")),
        _int(r.get("duns_global_count")),
        _int(r.get("company_id", 0)) or 0,
        _str(r.get("record_hash")),
        _str(r.get("record_hash2")),
        _str(r.get("record_hash3")),
        _str(r.get("ils_unique_key")),
        _str(r.get("ils_unique_duns")),
        _str(r.get("table_name")),
        _str(r.get("service_type")),
        _str(r.get("company_type")),
        _int(r.get("system_id")),
        _int(r.get("system_unique_id")),
        _str(r.get("system_name")),
        _str(r.get("client_specific_name")),
        _str(r.get("customer_unique_key")),
        _str(r.get("dnb_name")),
        _str(r.get("duns")),
        _str(r.get("address")),
        _str(r.get("phone_researched")),
        _str(r.get("city")),
        _str(r.get("state")),
        _str(r.get("county")),
        _str(r.get("country")),
        _str(r.get("postal_code")),
        _str(r.get("parent_name")),
        _str(r.get("web_address")),
        _float(r.get("dnb_sales_value")),
        _str(r.get("dnb_trade_style")),
        _str(r.get("sic")),
        _str(r.get("naics")),
        _str(r.get("latitude")),
        _str(r.get("longitude")),
        _str(r.get("line_of_business")),
        _str(r.get("no_of_employees")),
        _str(r.get("google_cust_name")),
        _str(r.get("google_address")),
        _str(r.get("google_city")),
        _str(r.get("google_state")),
        _str(r.get("google_country")),
        _str(r.get("google_postal_code")),
        _str(r.get("google_phone_researched")),
        _str(r.get("google_web_address")),
        _str(r.get("lead_source_1")),
        _str(r.get("lead_source_2")),
        _str(r.get("lead_source_3")),
        _str(r.get("lead_source_4")),
        _dt(r.get("last_updated")) or datetime.utcnow(),
        _int(r.get("system_count")),
        _str(r.get("deleted_tracker_id")) or "0",
        _float(r.get("total_sales")),
        _float(r.get("total_cost")),
        _dt(r.get("date_added")),
        _dt(r.get("mapped_on")),
        _int(r.get("client_type")),
        _int(r.get("distributor_id")),
        _str(r.get("distributor_name")),
        _str(r.get("sic_desc")),
        _str(r.get("naics_desc")),
        int(r.get("solr_synced") or 0),
        int(r.get("is_cddb_client") or 0),
        _str(r.get("interlynx_call_host")),
        _str(r.get("interlynx_rep")),
        _str(r.get("interlynx_am")),
        _str(r.get("interlynx_company_type")),
        _str(r.get("supplier_type")),
        _str(r.get("researched_company_name")),
        int(r.get("is_company_mapped") or 0),
        _str(r.get("searched_on_google")) or "No",
        _str(r.get("interlynx_name")),
        int(r.get("is_rep") or 0),
        _str(r.get("meta_data")),
        _str(r.get("company_description")),
        _str(r.get("company_keywords")),
        int(r.get("sync_data_to_system_flag") or 0),
        _str(r.get("meta")),
        _int(r.get("total_records")),
        _float(r.get("total")),
        _float(r.get("total_quote_value")),
        _float(r.get("total_sale_value")),
        _float(r.get("total_cost_value")),
    ]


# ---------------------------------------------------------------------------
# ClickHouse flush
# ---------------------------------------------------------------------------

def flush_to_clickhouse(client, batch: list[list]) -> None:
    if not batch:
        return
    start = time.monotonic()
    client.insert(
        table=CLICKHOUSE_TABLE,
        data=batch,
        column_names=COLUMNS,
        database=CLICKHOUSE_DB,
    )
    elapsed = time.monotonic() - start
    log.info("Flushed %d rows to ClickHouse in %.2fs", len(batch), elapsed)


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run():
    # --- ClickHouse client ---
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    log.info("Connected to ClickHouse at %s:%s", CLICKHOUSE_HOST, CLICKHOUSE_PORT)

    # --- Kafka consumer ---
    consumer = Consumer({
        "bootstrap.servers":        KAFKA_BROKERS,
        "group.id":                 KAFKA_GROUP_ID,
        "auto.offset.reset":        KAFKA_AUTO_OFFSET,
        "enable.auto.commit":       False,          # manual commit after flush
        "max.poll.interval.ms":     300000,
        "session.timeout.ms":       30000,
        "heartbeat.interval.ms":    10000,
        "fetch.max.bytes":          52428800,       # 50 MB
        "max.partition.fetch.bytes": 10485760,      # 10 MB
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Subscribed to topic: %s", KAFKA_TOPIC)

    # --- Graceful shutdown ---
    running = True

    def _shutdown(sig, frame):
        nonlocal running
        log.info("Shutdown signal received")
        running = False

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # --- Batch state ---
    batch: list[list] = []
    last_flush = time.monotonic()
    error_count = 0

    try:
        while running:
            # Poll with a short timeout so the timeout-based flush can fire
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass  # no message this poll cycle
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("Reached end of partition %s [%d]",
                              msg.topic(), msg.partition())
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.error("Unknown topic or partition: %s", msg.error())
                    break
                else:
                    log.error("Kafka error: %s", msg.error())
                    error_count += 1
                    if error_count > 10:
                        log.critical("Too many Kafka errors — exiting")
                        break
            else:
                error_count = 0
                try:
                    envelope = json.loads(msg.value().decode("utf-8"))
                    action   = envelope.get("action", "upsert")
                    payload  = envelope.get("payload", {})

                    if action == "upsert":
                        row = coerce_record(payload)
                        batch.append(row)
                    elif action == "delete":
                        # ReplacingMergeTree doesn't support real deletes;
                        # log and skip (or implement a tombstone pattern if needed)
                        log.debug("Skipping delete for id=%s", payload.get("id"))
                    else:
                        log.warning("Unknown action: %s", action)

                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    log.error("Failed to parse message: %s | error: %s",
                              msg.value(), e)

            # --- Flush condition: size OR timeout ---
            now = time.monotonic()
            should_flush = (
                len(batch) >= BATCH_SIZE
                or (batch and (now - last_flush) >= BATCH_TIMEOUT_SECS)
            )

            if should_flush:
                try:
                    flush_to_clickhouse(ch_client, batch)
                    consumer.commit(asynchronous=False)  # commit after successful flush
                    batch = []
                    last_flush = time.monotonic()
                except Exception as e:
                    log.error("Flush failed: %s — retaining batch for retry", e)
                    # Don't commit; messages will be re-consumed on restart

    finally:
        # Flush any remaining messages
        if batch:
            log.info("Flushing remaining %d records before exit", len(batch))
            try:
                flush_to_clickhouse(ch_client, batch)
                consumer.commit(asynchronous=False)
            except Exception as e:
                log.error("Final flush failed: %s", e)

        consumer.close()
        log.info("Consumer shut down cleanly")


if __name__ == "__main__":
    run()