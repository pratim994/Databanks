{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        account_id,
        customer_id,
        account_type,
        balance::NUMERIC(18,2) AS balance,
        currency,
        created_at,
        ingestion_timestamp AS load_timestamp,
        operation_type = 'd' AS is_deleted,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY kafka_partition DESC, kafka_offset DESC
        ) AS rn
    FROM {{ source('raw', 'accounts') }}
)
SELECT account_id, customer_id, account_type, balance, currency, created_at, load_timestamp, is_deleted
FROM ranked
WHERE rn = 1
