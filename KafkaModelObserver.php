<?php

namespace App\Observers;

use App\Services\KafkaProducerService;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Log;

/**
 * Generic Kafka observer.
 *
 * Instead of one observer per table, use this base class.
 * Each model just needs to define a $kafkaTableKey property.
 *
 * Usage in your Eloquent model:
 *
 *   class PdbCompanySystemwise extends Model
 *   {
 *       public string $kafkaTableKey = 'companies_systemwise';
 *   }
 *
 * Register in AppServiceProvider::boot():
 *
 *   PdbCompanySystemwise::observe(KafkaModelObserver::class);
 *   PdbContact::observe(KafkaModelObserver::class);
 */
class KafkaModelObserver
{
    public function __construct(private KafkaProducerService $kafka) {}

    public function created(Model $model): void
    {
        $this->produce($model, 'upsert');
    }

    public function updated(Model $model): void
    {
        $this->produce($model, 'upsert');
    }

    public function deleted(Model $model): void
    {
        $this->produce($model, 'delete');
    }

    private function produce(Model $model, string $action): void
    {
        $tableKey = $model->kafkaTableKey ?? null;

        if (!$tableKey) {
            Log::warning('[Kafka Observer] Model has no $kafkaTableKey defined', [
                'model' => get_class($model),
                'id'    => $model->getKey(),
            ]);
            return;
        }

        try {
            $this->kafka->send($tableKey, $model->toArray(), $action);
        } catch (\Throwable $e) {
            Log::error('[Kafka Observer] Failed to produce', [
                'table_key' => $tableKey,
                'action'    => $action,
                'id'        => $model->getKey(),
                'error'     => $e->getMessage(),
            ]);
        }
    }
}
