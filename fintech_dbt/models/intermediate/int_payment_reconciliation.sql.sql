{{ config(materialized='view') }}

with payments as (
    select * from {{ ref('stg_payments') }}
),

ledger as (
    select * from {{ ref('stg_ledger_entries') }}
)

select
    p.payment_id,
    p.user_id,
    p.merchant_id,
    p.transaction_type,
    p.channel,
    p.currency,
    p.amount as payment_amount,
    p.status as payment_status,
    p.initiated_at,
    
    -- Ledger attributes
    l.entry_id,
    l.entry_type,
    l.amount as ledger_amount,
    l.posted_at,
    
    -- Reconciliation Flag
    case 
        when p.status = 'success' and l.entry_id is null then 'unreconciled_missing_ledger'
        when p.status = 'failed' and l.entry_id is not null then 'unreconciled_unexpected_ledger'
        else 'reconciled'
    end as reconciliation_status

from payments p
left join ledger l on p.payment_id = l.payment_id