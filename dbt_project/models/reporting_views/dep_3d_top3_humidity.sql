{{ config(materialized='view') }}

SELECT
    department,
    avg(depavg_humidity) AS depavg_humidity

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY department
ORDER BY depavg_humidity DESC
LIMIT 3
