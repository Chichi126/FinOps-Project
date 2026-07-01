{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='date_key'
) }}

WITH date_bounds AS (
    SELECT 
        toDate('{{ var("start_date") }}') AS start_date,
        toDate('{{ var("end_date") }}') AS end_date
)

SELECT 
    (start_date + number) AS date_key,
    toYear(start_date + number) AS year,
    toMonth(start_date + number) AS month,
    toDayOfMonth(start_date + number) AS day_of_month,
    toDayOfWeek(start_date + number) AS day_of_week,
    toQuarter(start_date + number) AS quarter
FROM numbers(100000)
CROSS JOIN date_bounds
WHERE number <= dateDiff('day', start_date, end_date)