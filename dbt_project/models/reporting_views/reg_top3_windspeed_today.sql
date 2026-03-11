{{ config(materialized='view') }}

SELECT
    reg_name,
    round(avg(windspeed), 2) AS regavg_windspeed

FROM {{ ref('mart_today_stats') }}
GROUP BY reg_name
ORDER BY regavg_windspeed DESC
LIMIT 3
