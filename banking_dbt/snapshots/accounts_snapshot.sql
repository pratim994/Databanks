
{% snapshot account_snapshot %}

{{
	config(
		target_schema='ANALYTICS',
		unique_keys='acount_id',
		strategy='check',
		check_cols=['customer_id', 'account_type', 'balance']	
	)

}}


SELECT * FROM {{ ref('stg_accounts') }}

{% endsnapshot %}
