<?php

namespace App\Services;

use RdKafka\Conf;
use RdKafka\Producer;
use RdKafka\TopicConf;
use Illuminate\Support\Facades\Log;

class KafkaProducerService
{
    private Producer $producer;
    private \RdKafka\ProducerTopic $topic;

    public function __construct()
    {
        $conf = new Conf();

        $conf->set('metadata.broker.list', config('kafka.brokers'));
        $conf->set('compression.type',     config('kafka.producer.compression.type', 'lz4'));
        $conf->set('batch.num.messages',   config('kafka.producer.batch.num.messages', 1000));
        $conf->set('queue.buffering.max.ms', config('kafka.producer.queue.buffering.max.ms', 50));
        $conf->set('queue.buffering.max.messages', config('kafka.producer.queue.buffering.max.messages', 100000));
        $conf->set('message.max.bytes',    config('kafka.producer.message.max.bytes', 10485760));
        $conf->set('request.required.acks', config('kafka.producer.request.required.acks', -1));
        $conf->set('retries',              config('kafka.producer.retries', 3));

        // Delivery report callback — logs errors
        $conf->setDrMsgCb(function ($kafka, $message) {
            if ($message->err) {
                Log::error('[Kafka] Delivery failed', [
                    'error'   => rd_kafka_err2str($message->err),
                    'payload' => $message->payload,
                ]);
            }
        });

        $this->producer = new Producer($conf);
        $this->topic    = $this->producer->newTopic(config('kafka.topic'));
    }

    /**
     * Produce a single company record.
     *
     * @param  array  $record   Full row from pdb_companies_systemwise
     * @param  string $action   'upsert' | 'delete'
     */
    public function sendCompany(array $record, string $action = 'upsert'): void
    {
        $payload = json_encode([
            'action'  => $action,
            'payload' => $record,
        ]);

        // Partition by company_id for ordering guarantees per company
        $partitionKey = (string) ($record['company_id'] ?? $record['id'] ?? '');

        $this->topic->producev(
            RD_KAFKA_PARTITION_UA,   // let librdkafka pick partition via key hash
            0,                       // msgflags
            $payload,
            $partitionKey
        );

        $this->producer->poll(0);    // non-blocking poll to trigger callbacks
    }

    /**
     * Produce a batch of company records.
     *
     * @param  array  $records  Array of rows
     * @param  string $action   'upsert' | 'delete'
     */
    public function sendBatch(array $records, string $action = 'upsert'): void
    {
        foreach ($records as $record) {
            $this->sendCompany($record, $action);
        }

        // Flush with 10-second timeout — ensures all messages are delivered
        $result = $this->producer->flush(10000);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            Log::error('[Kafka] Flush incomplete', ['result' => $result]);
        }
    }

    public function __destruct()
    {
        $this->producer->flush(5000);
    }
}
