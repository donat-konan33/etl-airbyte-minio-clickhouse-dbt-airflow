{{ config(materialized='view') }}

SELECT
    reg_name,
    avg(regavg_windspeed) AS regavg_windspeed

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY reg_name
ORDER BY regavg_windspeed DESC
LIMIT 3
