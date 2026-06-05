<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use App\Services\KafkaProducerService;

/**
 * Stream any configured MySQL table to its Kafka topic.
 *
 * Usage:
 *   php artisan kafka:stream companies_systemwise
 *   php artisan kafka:stream contacts --since="2024-01-01 00:00:00"
 *   php artisan kafka:stream companies_systemwise --chunk=500
 *   php artisan kafka:stream --all                           # stream all tables
 *   php artisan kafka:stream --all --since="2024-06-01 00:00:00"
 */
class StreamTableToKafka extends Command
{
    protected $signature = 'kafka:stream
                            {table_key?           : Config key from kafka.tables (e.g. companies_systemwise)}
                            {--all                : Stream all tables defined in config}
                            {--since=             : Only rows updated after this datetime (Y-m-d H:i:s)}
                            {--chunk=1000         : Rows per DB chunk / Kafka batch}
                            {--updated-col=last_updated : Datetime column to filter with --since}
                            {--dry-run            : Count rows without sending}';

    protected $description = 'Stream a configured MySQL table to its Kafka topic';

    public function handle(KafkaProducerService $kafka): int
    {
        $tableKey  = $this->argument('table_key');
        $streamAll = $this->option('all');

        if (!$tableKey && !$streamAll) {
            $this->error('Provide a table_key argument or use --all');
            $this->line('Available tables: ' . implode(', ', array_keys(config('kafka.tables', []))));
            return Command::FAILURE;
        }

        $tableKeys = $streamAll
            ? array_keys(config('kafka.tables', []))
            : [$tableKey];

        foreach ($tableKeys as $key) {
            $this->streamTable($key, $kafka);
        }

        return Command::SUCCESS;
    }

    private function streamTable(string $tableKey, KafkaProducerService $kafka): void
    {
        $tableConfig = config("kafka.tables.{$tableKey}");

        if (!$tableConfig) {
            $this->error("No config found for table key: '{$tableKey}'");
            return;
        }

        $mysqlTable  = $tableConfig['mysql_table'];
        $topic       = $tableConfig['topic'];
        $since       = $this->option('since');
        $chunk       = (int) $this->option('chunk');
        $dryRun      = $this->option('dry-run');
        $updatedCol  = $this->option('updated-col');
        $total       = 0;

        $this->info("[$tableKey] Streaming {$mysqlTable} → topic:{$topic}" . ($since ? " (since {$since})" : ''));

        $query = DB::table($mysqlTable)->orderBy('id');

        if ($since) {
            $query->where($updatedCol, '>=', $since);
        }

        $query->chunk($chunk, function ($rows) use ($kafka, $tableKey, $dryRun, $topic, &$total) {
            $records = $rows->map(fn($r) => (array) $r)->toArray();
            $total  += count($records);

            if ($dryRun) {
                $this->line("  [dry-run] Would send " . count($records) . " records (total: {$total})");
                return;
            }

            $kafka->sendBatch($tableKey, $records, 'upsert');
            $this->line("  [{$topic}] Sent " . count($records) . " records (total: {$total})");
        });

        $this->info("[$tableKey] Done. Total: {$total}");
    }
}
