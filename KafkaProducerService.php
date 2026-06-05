<?php

namespace App\Services;

use RdKafka\Conf;
use RdKafka\Producer;
use Illuminate\Support\Facades\Log;

class KafkaProducerService
{
    private Producer $producer;

    /** @var array<string, \RdKafka\ProducerTopic> */
    private array $topicHandles = [];

    public function __construct()
    {
        $conf = new Conf();
        $cfg  = config('kafka.producer');

        $conf->set('metadata.broker.list',          config('kafka.brokers'));
        $conf->set('compression.type',              $cfg['compression.type']);
        $conf->set('batch.num.messages',            $cfg['batch.num.messages']);
        $conf->set('queue.buffering.max.ms',        $cfg['queue.buffering.max.ms']);
        $conf->set('queue.buffering.max.messages',  $cfg['queue.buffering.max.messages']);
        $conf->set('message.max.bytes',             $cfg['message.max.bytes']);
        $conf->set('request.required.acks',         $cfg['request.required.acks']);
        $conf->set('retries',                       $cfg['retries']);

        $conf->setDrMsgCb(function ($kafka, $message) {
            if ($message->err) {
                Log::error('[Kafka] Delivery failed', [
                    'error'   => rd_kafka_err2str($message->err),
                    'topic'   => $message->topic_name,
                    'payload' => substr($message->payload, 0, 200),
                ]);
            }
        });

        $this->producer = new Producer($conf);
    }

    /**
     * Send a single record for a named table config key.
     *
     * @param  string  $tableKey  Key in config('kafka.tables'), e.g. 'companies_systemwise'
     * @param  array   $record    Raw MySQL row as array
     * @param  string  $action    'upsert' | 'delete'
     */
    public function send(string $tableKey, array $record, string $action = 'upsert'): void
    {
        $tableConfig = $this->getTableConfig($tableKey);

        $mapped       = $this->applyFieldMap($record, $tableConfig);
        $partitionKey = (string) ($record[$tableConfig['partition_key']] ?? '');

        $payload = json_encode([
            'action'    => $action,
            'table_key' => $tableKey,
            'payload'   => $mapped,
        ]);

        $topic = $this->getTopic($tableConfig['topic']);
        $topic->producev(RD_KAFKA_PARTITION_UA, 0, $payload, $partitionKey);

        $this->producer->poll(0);
    }

    /**
     * Send a batch of records for a named table config key.
     *
     * @param  string  $tableKey
     * @param  array   $records   Array of raw MySQL rows
     * @param  string  $action
     */
    public function sendBatch(string $tableKey, array $records, string $action = 'upsert'): void
    {
        foreach ($records as $record) {
            $this->send($tableKey, $record, $action);
        }

        $result = $this->producer->flush(10000);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            Log::error('[Kafka] Flush incomplete', [
                'table_key' => $tableKey,
                'result'    => $result,
            ]);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private function getTableConfig(string $tableKey): array
    {
        $config = config("kafka.tables.{$tableKey}");

        if (!$config) {
            throw new \InvalidArgumentException(
                "[Kafka] No config found for table key: '{$tableKey}'. Add it to config/kafka.php"
            );
        }

        return $config;
    }

    private function getTopic(string $topicName): \RdKafka\ProducerTopic
    {
        if (!isset($this->topicHandles[$topicName])) {
            $this->topicHandles[$topicName] = $this->producer->newTopic($topicName);
        }

        return $this->topicHandles[$topicName];
    }

    /**
     * Apply field_map and exclude_fields from config to a raw record.
     */
    private function applyFieldMap(array $record, array $tableConfig): array
    {
        $excludes = $tableConfig['exclude_fields'] ?? [];
        $fieldMap = $tableConfig['field_map'] ?? [];

        $result = [];

        foreach ($record as $col => $value) {
            // Skip excluded fields
            if (in_array($col, $excludes, true)) {
                continue;
            }

            // Rename if mapped, otherwise keep original name
            $targetCol          = $fieldMap[$col] ?? $col;
            $result[$targetCol] = $value;
        }

        return $result;
    }

    public function __destruct()
    {
        $this->producer->flush(5000);
    }
}
