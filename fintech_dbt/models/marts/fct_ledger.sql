{{ config(materialized='table') }}


SELECT
    entry_id,
    payment_id,
    user_id,
    entry_type,
    currency,
    ledger_amount,
    posted_at,
    failure_mode
FROM {{ ref('stg_ledger_entries') }}