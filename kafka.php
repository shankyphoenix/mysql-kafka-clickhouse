<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Kafka Broker(s)
    |--------------------------------------------------------------------------
    */
    'brokers' => env('KAFKA_BROKERS', '16.58.103.76:9094'),

    /*
    |--------------------------------------------------------------------------
    | Producer settings
    |--------------------------------------------------------------------------
    */
    'producer' => [
        'compression.type'              => 'lz4',
        'batch.num.messages'            => 1000,
        'queue.buffering.max.ms'        => 50,
        'queue.buffering.max.messages'  => 100000,
        'message.max.bytes'             => 10485760,
        'request.required.acks'         => -1,
        'retries'                       => 3,
    ],

    /*
    |--------------------------------------------------------------------------
    | Table → Topic Map
    |--------------------------------------------------------------------------
    | Each entry defines:
    |
    |   'mysql_table'     => MySQL source table name
    |   'topic'           => Kafka topic name
    |   'partition_key'   => Field used to partition messages (for ordering)
    |   'order_by'        => Fields that form the unique key (must match
    |                        ClickHouse ORDER BY) — used for upsert identity
    |   'field_map'       => [ 'mysql_column' => 'clickhouse_column' ]
    |                        Only needed when names differ. Columns with the
    |                        same name in both can be omitted.
    |   'exclude_fields'  => MySQL columns to never send to Kafka
    |
    | Add a new table by adding a new entry to this array.
    |--------------------------------------------------------------------------
    */
    'tables' => [

        'companies_systemwise' => [
            'mysql_table'    => 'pdb_companies_systemwise',
            'topic'          => 'companies_systemwise',
            'partition_key'  => 'record_hash2',
            'order_by'       => ['system_id', 'system_unique_id', 'company_type', 'service_type', 'table_name'],
            'exclude_fields' => [],
            'field_map'      => [
                // 'mysql_col' => 'clickhouse_col'
                // same names — nothing to remap
            ],
        ],

        'contacts' => [
            'mysql_table'    => 'pdb_contacts',
            'topic'          => 'contacts',
            'partition_key'  => 'record_hash2',
            'order_by'       => ['contact_id', 'system_id'],
            'exclude_fields' => ['internal_notes'],
            'field_map'      => [
                'cust_name'  => 'customer_name',
                'cust_email' => 'email',
            ],
        ],

        // Add more tables here following the same pattern:
        // 'config_key' => [
        //     'mysql_table'   => '...',
        //     'topic'         => '...',
        //     'partition_key' => '...',
        //     'order_by'      => [...],
        //     'exclude_fields'=> [],
        //     'field_map'     => [],
        // ],

    ],

];
