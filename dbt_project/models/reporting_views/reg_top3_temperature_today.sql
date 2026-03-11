{{ config(materialized='view') }}

SELECT
    reg_name,
    round(avg(temp), 2) AS regavg_temperature

FROM {{ ref('mart_today_stats') }}
GROUP BY reg_name
ORDER BY regavg_temperature DESC
LIMIT 3
