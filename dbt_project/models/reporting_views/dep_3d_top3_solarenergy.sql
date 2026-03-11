{{ config(materialized='view') }}

SELECT
    department,
    avg(depavg_solarenergy_kwhpm2) AS depavg_solarenergy_kwhpm2

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY department
ORDER BY depavg_solarenergy_kwhpm2 DESC
LIMIT 3
