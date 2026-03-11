{{ config(materialized='view') }}

SELECT
    reg_name,
    avg(regavg_temp_c) AS regavg_temperature

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY reg_name
ORDER BY regavg_temperature DESC
LIMIT 3
