{{ config(materialized='table') }}

SELECT
    md5(account_id::text || ':' || dbt_scd_id) AS account_key,
    dbt_scd_id,
    account_id AS account_natural_key,
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,

    dbt_valid_from AS effective_from,
    dbt_valid_to AS effective_to,
    dbt_updated_at,

    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current

FROM {{ ref('accounts_snapshot') }}
WHERE NOT is_deleted
