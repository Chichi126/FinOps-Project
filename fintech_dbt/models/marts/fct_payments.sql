{{ config(materialized='table',
            engine='MergeTree',
            order_by='(posted_at, user_id)') }}


SELECT
    payment_id,
    user_id,
    merchant_id,
    transaction_type,
    channel,
    currency,
    amount,
    
    failure_mode,
    reconciliation_status
FROM {{ ref('stg_payments') }}

WHERE payment_id IS NOT NULL