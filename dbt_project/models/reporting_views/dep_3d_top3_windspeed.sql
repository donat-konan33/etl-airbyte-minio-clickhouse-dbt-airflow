{{ config(materialized='view') }}

SELECT
    department,
    avg(depavg_windspeed) AS depavg_windspeed

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY department
ORDER BY depavg_windspeed DESC
LIMIT 3
