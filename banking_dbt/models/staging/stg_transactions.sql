
{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        transaction_id,
        account_id,
        amount::NUMERIC(18,2) AS amount,
        txn_type AS transaction_type,
        related_account_id,
        status,
        created_at AS transaction_time,
        ingestion_timestamp AS load_timestamp,
        operation_type,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY kafka_partition DESC, kafka_offset DESC
        ) AS rn
    FROM {{ source('raw', 'transactions') }}
)
SELECT transaction_id, account_id, amount, transaction_type, related_account_id, status, transaction_time, load_timestamp
FROM ranked
WHERE rn = 1 AND operation_type <> 'd'
