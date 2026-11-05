{% snapshot accounts_snapshot %}

{{
    config(
        target_schema='analytics',
        unique_key='account_id',
        strategy='check',
        check_cols=[
            'customer_id',
            'account_type',
            'balance',
            'currency',
            'is_deleted'
        ]
    )
}}

SELECT * FROM {{ ref('stg_account_snapshot_source') }}

{% endsnapshot %}
