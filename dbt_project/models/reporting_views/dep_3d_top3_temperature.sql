{{ config(materialized='view') }}

SELECT
    department,
    avg(depavg_temp_c) AS depavg_temperature

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY department
ORDER BY depavg_temperature DESC
LIMIT 3
