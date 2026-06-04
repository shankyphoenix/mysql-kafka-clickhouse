<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use App\Services\KafkaProducerService;

/**
 * Stream rows from pdb_companies_systemwise → Kafka topic.
 *
 * Usage:
 *   php artisan kafka:stream-companies              # full table
 *   php artisan kafka:stream-companies --since="2024-01-01 00:00:00"   # incremental
 *   php artisan kafka:stream-companies --chunk=500  # custom batch size
 */
class StreamCompaniesToKafka extends Command
{
    protected $signature = 'kafka:stream-companies
                            {--since= : Only send rows updated after this datetime (Y-m-d H:i:s)}
                            {--chunk=1000 : Rows per DB chunk / Kafka batch}
                            {--dry-run : Print count without sending}';

    protected $description = 'Stream pdb_companies_systemwise rows to Kafka for ClickHouse upsert';

    public function handle(KafkaProducerService $kafka): int
    {
        $since   = $this->option('since');
        $chunk   = (int) $this->option('chunk');
        $dryRun  = $this->option('dry-run');
        $total   = 0;

        $this->info("Starting stream" . ($since ? " (since {$since})" : " (full table)"));

        $query = DB::table('pdb_companies_systemwise')
            ->orderBy('id');

        if ($since) {
            $query->where('last_updated', '>=', $since);
        }

        $query->chunk($chunk, function ($rows) use ($kafka, $dryRun, &$total) {
            $records = $rows->map(fn ($row) => (array) $row)->toArray();
            $total  += count($records);

            if ($dryRun) {
                $this->line("  [dry-run] Would send " . count($records) . " records (total so far: {$total})");
                return;
            }

            $kafka->sendBatch($records, 'upsert');
            $this->line("  Sent " . count($records) . " records (total: {$total})");
        });

        $this->info("Done. Total records sent: {$total}");

        return Command::SUCCESS;
    }
}
