<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Kafka Broker(s)
    |--------------------------------------------------------------------------
    | Comma-separated list of host:port pairs.
    | Uses the EXTERNAL listener exposed on port 9094.
    */
    'brokers' => env('KAFKA_BROKERS', '16.58.103.76:9094'),

    'topic' => env('KAFKA_TOPIC', 'companies_systemwise'),

    /*
    |--------------------------------------------------------------------------
    | Producer settings (passed to librdkafka via php-rdkafka)
    |--------------------------------------------------------------------------
    */
    'producer' => [
        'compression.type'         => 'lz4',
        'batch.num.messages'       => 1000,
        'queue.buffering.max.ms'   => 50,
        'queue.buffering.max.messages' => 100000,
        'message.max.bytes'        => 10485760,   // 10 MB
        'request.required.acks'    => -1,          // all replicas ack (safe)
        'retries'                  => 3,
    ],

];
