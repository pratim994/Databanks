
{{ config(materialized='view') }}

SELECT customer_id, first_name, last_name, email, created_at, load_timestamp
FROM {{ ref('stg_customer_snapshot_source') }}
WHERE NOT is_deleted
