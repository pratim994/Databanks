{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        ingestion_timestamp AS load_timestamp,
        operation_type = 'd' AS is_deleted,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY kafka_partition DESC, kafka_offset DESC
        ) AS rn
    FROM {{ source('raw', 'customers') }}
)
SELECT customer_id, first_name, last_name, email, created_at, load_timestamp, is_deleted
FROM ranked
WHERE rn = 1
