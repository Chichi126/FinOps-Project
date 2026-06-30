
{{ config(
    materialized='view'
) }}


with raw_payment as (
    select * from {{ source('raw', 'payments') }}
),
deduplicated_payment as (
    select *,
    qualify row_number() over (partition by payment_id
    ORDER BY initiated_at DESC) as rn

    FROM raw_payment
)

select
    cast(payment_id as String) as payment_id,
    cast(user_id as String) as user_id,
    cast(merchant_id as String) as merchant_id,
    cast(transaction_type as String) as transaction_type,
    cast(channel as String) as channel,
    cast(currency as String) as currency,
    cast(amount as Float64) as amount,
    cast(status as String) as status,
    cast(initiated_at as DateTime) as initiated_at,
    cast(completed_at as Nullable(DateTime)) as completed_at,
    cast(failure_mode as Nullable(String)) as failure_mode
from deduplicated_payment
where rn = 1