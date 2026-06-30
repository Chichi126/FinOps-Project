{{ config(materialized='table') }}


SELECT
    payment_id,
    user_id,
    entry_id,
    merchant_id,
    transaction_type,
    channel,
    currency,
    payment_amount as amount,
    payment_status as status,
    initiated_at,
    completed_at,
    reconciliation_status,
    CASE
        WHEN reconciliation_status = 'reconciled' THEN ledger_amount
        ELSE NULL
    END AS ledger_amount,
    CASE
        WHEN reconciliation_status = 'reconciled' THEN posted_at
        ELSE NULL
    END AS posted_at
FROM {{ ref('int_payment_reconciliation') }}
   