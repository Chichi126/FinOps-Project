{{ config(
    materialized='view'
) }}



with raw_ledger as (
    select * from {{ source('raw', 'ledger_entries') }}
), 

deduplicated_ledger as (
    select *,
    qualify row_number() over (partition by entry_id
    ORDER BY posted_at DESC) as rn

    FROM raw_ledger
)
select
    cast(entry_id as String) as entry_id,
    cast(payment_id as String) as payment_id,
    cast(user_id as String) as user_id,
    cast(entry_type as String) as entry_type,
    cast(currency as String) as currency,
    cast(amount as Float64) as amount,
    cast(posted_at as DateTime) as posted_at,
    cast(failure_mode as Nullable(String)) as failure_mode
from deduplicated_ledger
where rn = 1