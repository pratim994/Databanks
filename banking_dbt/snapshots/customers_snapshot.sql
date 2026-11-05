
{% snapshot customers_snapshot %}
{{
    config(
      target_schema='analytics',
      unique_key='customer_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'email', 'is_deleted']
    )
}}
SELECT * FROM {{ ref('stg_customer_snapshot_source') }}
{% endsnapshot %}
