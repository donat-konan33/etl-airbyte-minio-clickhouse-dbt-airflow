{{ config(materialized='view') }}

SELECT
    department,
    avg(depavg_solarradiation) AS depavg_solarradiation

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY department
ORDER BY depavg_solarradiation DESC
LIMIT 3
