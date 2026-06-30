{{ config(materialized='table') }}

select distinct
    merchant_id,
    min(initiated_at) as partner_since_at
from {{ ref('int_payments_clean') }}
where merchant_id is not null
group by merchant_id