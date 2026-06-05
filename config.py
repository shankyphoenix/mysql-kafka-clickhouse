"""
config.py
---------
Central config for all Kafka → ClickHouse topic pipelines.

To add a new table:
1. Add an entry to PIPELINES with the topic name as key.
2. Define `clickhouse_table`, `columns`, `coerce` function, and optionally `field_map`.
3. Start a new consumer process: python consumer.py --topic <topic>
"""

from datetime import datetime
from typing import Optional, Any


# ---------------------------------------------------------------------------
# Shared type coercion helpers
# ---------------------------------------------------------------------------

def _dt(v) -> Optional[datetime]:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(v), fmt)
        except ValueError:
            continue
    return None

def _int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None

def _float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None

def _str(v) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        import json
        return json.dumps(v)
    return str(v)

def _int_d(v, default=0) -> int:
    return _int(v) if v is not None else default

def _str_d(v, default="") -> str:
    return _str(v) if v is not None else default


# ---------------------------------------------------------------------------
# Pipeline definitions
# ---------------------------------------------------------------------------
# Each pipeline entry:
#
#   'topic_name': {
#       'clickhouse_table': str,
#
#       'field_map': dict  (optional)
#           Rename fields BEFORE coercion.
#           { 'incoming_kafka_field': 'clickhouse_column_name' }
#           Fields not in field_map are passed through as-is.
#
#       'columns': list[str]
#           Ordered list of ClickHouse column names to insert.
#
#       'coerce': callable(dict) -> list
#           Receives the (already field-mapped) payload dict.
#           Returns an ordered list matching `columns`.
#   }
# ---------------------------------------------------------------------------

def _coerce_companies(r: dict) -> list:
    return [
        _int(r.get("id")),
        _int(r.get("is_dedupe_global")),
        _str(r.get("dedupe_global_connect_id")),
        _float(r.get("dupe_global_total_cost")),
        _float(r.get("dupe_global_total_sales")),
        _int(r.get("duns_global_count")),
        _int_d(r.get("company_id"), 0),
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
        _str_d(r.get("deleted_tracker_id"), "0"),
        _float(r.get("total_sales")),
        _float(r.get("total_cost")),
        _dt(r.get("date_added")),
        _dt(r.get("mapped_on")),
        _int(r.get("client_type")),
        _int(r.get("distributor_id")),
        _str(r.get("distributor_name")),
        _str(r.get("sic_desc")),
        _str(r.get("naics_desc")),
        _int_d(r.get("solr_synced"), 0),
        _int_d(r.get("is_cddb_client"), 0),
        _str(r.get("interlynx_call_host")),
        _str(r.get("interlynx_rep")),
        _str(r.get("interlynx_am")),
        _str(r.get("interlynx_company_type")),
        _str(r.get("supplier_type")),
        _str(r.get("researched_company_name")),
        _int_d(r.get("is_company_mapped"), 0),
        _str_d(r.get("searched_on_google"), "No"),
        _str(r.get("interlynx_name")),
        _int_d(r.get("is_rep"), 0),
        _str(r.get("meta_data")),
        _str(r.get("company_description")),
        _str(r.get("company_keywords")),
        _int_d(r.get("sync_data_to_system_flag"), 0),
        _str(r.get("meta")),
        _int(r.get("total_records")),
        _float(r.get("total")),
        _float(r.get("total_quote_value")),
        _float(r.get("total_sale_value")),
        _float(r.get("total_cost_value")),
    ]


def _coerce_contacts(r: dict) -> list:
    """
    Example coerce function for a contacts table.
    Adjust columns to match your actual ClickHouse contacts table.
    """
    return [
        _int(r.get("id")),
        _int(r.get("contact_id")),
        _int(r.get("system_id")),
        _str(r.get("customer_name")),   # remapped from cust_name via field_map
        _str(r.get("email")),           # remapped from cust_email via field_map
        _str(r.get("phone")),
        _str(r.get("city")),
        _str(r.get("state")),
        _str(r.get("country")),
        _dt(r.get("last_updated")) or datetime.utcnow(),
        _dt(r.get("date_added")),
    ]



def _coerce_distributor_rep(r: dict) -> list:
    return [
        _int(r.get("id")),
        _int(r.get("system_id")),
        _int(r.get("system_unique_id")),   # already renamed via field_map
        _str(r.get("company_type")),
        _str(r.get("dist_or_rep")),
        _str(r.get("system_name")),
        _str(r.get("name")),
        _str(r.get("email")),
        _str(r.get("account_number")),
        _str(r.get("is_rep")),
        _dt(r.get("created_at")),
        _dt(r.get("updated_at")),
    ]

# ---------------------------------------------------------------------------
# PIPELINES registry — add new tables here
# ---------------------------------------------------------------------------

PIPELINES: dict[str, dict] = {

    "companies_systemwise": {
        "clickhouse_table": "companies_by_system",
        "field_map": {},   # names are identical between MySQL and ClickHouse
        "columns": [
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
        ],
        "coerce": _coerce_companies,
    },

    "contacts": {
        "clickhouse_table": "contacts_by_system",
        "field_map": {
            "cust_name":  "customer_name",   # rename kafka field → clickhouse column
            "cust_email": "email",
        },
        "columns": [
            "id", "contact_id", "system_id", "customer_name", "email",
            "phone", "city", "state", "country", "last_updated", "date_added",
        ],
        "coerce": _coerce_contacts,
    },

    
    "distributors": {
        "clickhouse_table": "distributors",
        "field_map": {},
        "columns": [
            "id", "system_id", "system_unique_id", "company_type","dist_or_rep",
            "system_name", "name", "email", "account_number","is_rep",
            "created_at", "updated_at",
        ],
        "coerce": _coerce_distributor_rep,
    },

    # Add more pipelines here:
    # "your_topic": {
    #     "clickhouse_table": "your_ch_table",
    #     "field_map": { "old_name": "new_name" },
    #     "columns": [ ... ],
    #     "coerce": _coerce_your_table,
    # },
}