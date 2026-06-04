<?php

namespace App\Observers;

use App\Models\PdbCompanySystemwise;
use App\Services\KafkaProducerService;
use Illuminate\Support\Facades\Log;

/**
 * Automatically streams any create/update/delete on PdbCompanySystemwise to Kafka.
 *
 * Register in App\Providers\AppServiceProvider::boot():
 *   PdbCompanySystemwise::observe(CompanySystemwiseObserver::class);
 */
class CompanySystemwiseObserver
{
    public function __construct(private KafkaProducerService $kafka) {}

    public function created(PdbCompanySystemwise $model): void
    {
        $this->produce($model, 'upsert');
    }

    public function updated(PdbCompanySystemwise $model): void
    {
        $this->produce($model, 'upsert');
    }

    public function deleted(PdbCompanySystemwise $model): void
    {
        // For soft-deletes or hard-deletes — consumer will handle tombstone logic
        $this->produce($model, 'delete');
    }

    private function produce(PdbCompanySystemwise $model, string $action): void
    {
        try {
            $this->kafka->sendCompany($model->toArray(), $action);
        } catch (\Throwable $e) {
            Log::error('[Kafka Observer] Failed to produce message', [
                'action' => $action,
                'id'     => $model->id,
                'error'  => $e->getMessage(),
            ]);
        }
    }
}
