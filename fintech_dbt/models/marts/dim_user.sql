{{ config(materialized='table') }}


SELECT DISTINCT user_id, MIN(initiated_at) AS first_transaction_date
FROM {{ ref('int_payment_reconciliation') }}
GROUP BY user_id