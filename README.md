# Kafka Pipeline v2 — Multi-table

```
MySQL tables (any)
      │
      │  Laravel Producer
      │  config/kafka.php  ← central map
      ▼
Kafka topics (one per table)
      │
      │  Python consumers (one process per topic)
      │  config.py  ← central map
      ▼
ClickHouse tables (one per topic)
ENGINE = ReplacingMergeTree
```

---

## Adding a new table (checklist)

### 1. config/kafka.php (Laravel)
Add an entry to the `tables` array:
```php
'your_table' => [
    'mysql_table'    => 'your_mysql_table',
    'topic'          => 'your_topic',
    'partition_key'  => 'record_hash2',       // or whatever uniquely IDs a record
    'order_by'       => ['col1', 'col2'],      // must match CH ORDER BY
    'exclude_fields' => ['sensitive_col'],
    'field_map'      => ['old_name' => 'new_name'],
],
```

### 2. config.py (Python consumer)
Add a `_coerce_your_table(r)` function and an entry in `PIPELINES`:
```python
def _coerce_your_table(r: dict) -> list:
    return [
        _int(r.get("id")),
        _str(r.get("name")),
        # ... one entry per column
    ]

PIPELINES["your_topic"] = {
    "clickhouse_table": "your_ch_table",
    "field_map": {},
    "columns": ["id", "name", ...],
    "coerce": _coerce_your_table,
}
```

### 3. kafka/create_topics.sh
Add the topic name to the `TOPICS` array, then run the script.

### 4. Register the observer in Laravel
```php
// In AppServiceProvider::boot()
YourModel::observe(KafkaModelObserver::class);
```
And set `$kafkaTableKey` on the model:
```php
class YourModel extends Model
{
    public string $kafkaTableKey = 'your_table';
}
```

### 5. Start a consumer process
```bash
# Manually
python consumer.py --topic your_topic

# As a systemd service
sudo systemctl enable kafka-consumer@your_topic
sudo systemctl start kafka-consumer@your_topic
```

---

## Setup

### Kafka
```bash
cd kafka/
docker compose up -d
chmod +x create_topics.sh && ./create_topics.sh
```

### Laravel Producer
Copy files into your project:
```
config/kafka.php                          → config/kafka.php
Services/KafkaProducerService.php         → app/Services/KafkaProducerService.php
Services/KafkaModelObserver.php           → app/Observers/KafkaModelObserver.php
Console/Commands/StreamTableToKafka.php   → app/Console/Commands/StreamTableToKafka.php
```

Register in `AppServiceProvider::boot()`:
```php
use App\Models\PdbCompanySystemwise;
use App\Observers\KafkaModelObserver;

PdbCompanySystemwise::observe(KafkaModelObserver::class);
// PdbContact::observe(KafkaModelObserver::class);
```

Add to `.env`:
```
KAFKA_BROKERS=16.58.103.76:9094
KAFKA_TOPIC=companies_systemwise   # not needed anymore but harmless
```

Backfill / incremental stream:
```bash
php artisan kafka:stream companies_systemwise
php artisan kafka:stream contacts --since="2024-06-01 00:00:00"
php artisan kafka:stream --all
```

### Python Consumer
```bash
cd consumer/
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env — fill in CLICKHOUSE_* values
```

Run consumers:
```bash
python consumer.py --topic companies_systemwise
python consumer.py --topic contacts
```

As systemd services (parameterized template — one unit file, multiple instances):
```bash
sudo cp kafka-consumer@.service /etc/systemd/system/
sudo systemctl daemon-reload

sudo systemctl enable kafka-consumer@companies_systemwise
sudo systemctl enable kafka-consumer@contacts
sudo systemctl start kafka-consumer@companies_systemwise
sudo systemctl start kafka-consumer@contacts

# Logs
sudo journalctl -u kafka-consumer@companies_systemwise -f
sudo journalctl -u kafka-consumer@contacts -f
```

---

## ClickHouse upsert notes

- `ReplacingMergeTree(last_updated)` keeps the row with the **highest `last_updated`** per ORDER BY key.
- Deduplication happens on background merge — use `FINAL` for immediate consistency:
  ```sql
  SELECT * FROM companies_by_system FINAL WHERE system_id = 1;
  ```
