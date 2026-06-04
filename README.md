# Kafka Pipeline — companies_systemwise → ClickHouse

```
MySQL (pdb_companies_systemwise)
        │
        │  Laravel Producer
        │  (Observer or Artisan command)
        ▼
  Kafka  (16.58.103.76:9094)
  topic: companies_systemwise
        │
        │  Python Consumer
        │  batch flush: 1000 rows OR 5s
        ▼
  ClickHouse (companies_by_system)
  ENGINE = ReplacingMergeTree(last_updated)
```

---

## 1. Kafka (Docker)

### Start

```bash
cd kafka/
docker compose up -d
```

### Create topic

```bash
chmod +x create_topic.sh
./create_topic.sh
```

### Kafka UI

Open `http://16.58.103.76:8080` in your browser.

### Firewall (open required ports)

```bash
sudo ufw allow 9094/tcp   # Kafka external
sudo ufw allow 8080/tcp   # Kafka UI (restrict to your IP in production)
```

---

## 2. Laravel Producer

### Install php-rdkafka

```bash
# Ubuntu/Debian
sudo apt-get install librdkafka-dev
pecl install rdkafka
echo "extension=rdkafka.so" >> /etc/php/8.x/cli/php.ini
```

### Copy files into your Laravel project

```
config/kafka.php                    → config/kafka.php
KafkaProducerService.php            → app/Services/KafkaProducerService.php
CompanySystemwiseObserver.php       → app/Observers/CompanySystemwiseObserver.php
StreamCompaniesToKafka.php          → app/Console/Commands/StreamCompaniesToKafka.php
```

### Register the observer in AppServiceProvider

```php
use App\Models\PdbCompanySystemwise;
use App\Observers\CompanySystemwiseObserver;

public function boot(): void
{
    PdbCompanySystemwise::observe(CompanySystemwiseObserver::class);
}
```

### Add to .env

```
KAFKA_BROKERS=16.58.103.76:9094
KAFKA_TOPIC=companies_systemwise
```

### Initial full sync (one-time backfill)

```bash
php artisan kafka:stream-companies
```

### Incremental sync (e.g. from a scheduled job)

```bash
php artisan kafka:stream-companies --since="2024-06-01 00:00:00"
```

---

## 3. Python Consumer

### Setup

```bash
cd consumer/
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env — set CLICKHOUSE_* vars
```

### Run manually

```bash
source venv/bin/activate
python consumer.py
```

### Run as systemd service (production)

```bash
sudo cp kafka-consumer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable kafka-consumer
sudo systemctl start kafka-consumer
sudo journalctl -u kafka-consumer -f
```

---

## 4. ClickHouse upsert notes

`ReplacingMergeTree(last_updated)` deduplicates rows sharing the same ORDER BY key:

```sql
ORDER BY (
    coalesce(system_id, 0),
    coalesce(system_unique_id, 0),
    coalesce(company_type, ''),
    coalesce(service_type, ''),
    coalesce(table_name, '')
)
```

- **Deduplication happens on merge**, not immediately on insert.
- For queries that need fully deduplicated results right now:
  ```sql
  SELECT * FROM companies_by_system FINAL WHERE ...;
  ```
- To force a merge (use sparingly — expensive on large tables):
  ```sql
  OPTIMIZE TABLE companies_by_system FINAL;
  ```

---

## 5. Tuning

| Parameter | Default | Notes |
|---|---|---|
| `BATCH_SIZE` | 1000 | Increase to 5000+ for high throughput |
| `BATCH_TIMEOUT_SECS` | 5 | Decrease for lower latency |
| Kafka partitions | 4 | Scale consumer instances = partition count |
| `KAFKA_GROUP_ID` | fixed | Change to reset offsets to earliest |
