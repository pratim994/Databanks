
{{ config(materialized='incremental', unique_key='transaction_id', incremental_strategy='delete+insert') }}

SELECT
	t.transaction_id,
	t.account_id,
	a.customer_id,
	t.amount,
	t.related_account_id,
	t.status,
	t.transaction_type,
	t.transaction_time,
    t.load_timestamp
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_accounts') }} a
  ON t.account_id = a.account_id
