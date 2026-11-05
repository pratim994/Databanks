
{{ config(materialized='table') }}

WITH versions AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        dbt_valid_from AS effective_from,
        dbt_valid_to AS effective_to,
        dbt_scd_id,
        dbt_updated_at,
        is_deleted,
        dbt_valid_to IS NULL AS is_current
    FROM {{ ref('customers_snapshot') }}
)
SELECT
    md5(customer_id::text || ':' || dbt_scd_id) AS customer_key,
    customer_id AS customer_natural_key,
    first_name,
    last_name,
    email,
    created_at,
    effective_from,
    effective_to,
    dbt_updated_at,
    is_current
FROM versions
WHERE NOT is_deleted
