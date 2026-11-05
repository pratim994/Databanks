{{ config(materialized='view') }}

SELECT account_id, customer_id, account_type, balance, currency, created_at, load_timestamp
FROM {{ ref('stg_account_snapshot_source') }}
WHERE NOT is_deleted

