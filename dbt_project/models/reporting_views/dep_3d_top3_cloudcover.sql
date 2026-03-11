{{ config(materialized='view') }}

SELECT
    department,
    avg(depavg_cloudcover) AS depavg_cloudcover

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY department
ORDER BY depavg_cloudcover DESC
LIMIT 3
