{{ config(materialized='view') }}

SELECT
    reg_name,
    avg(regavg_solarradiation) AS regavg_solarradiation

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY reg_name
ORDER BY regavg_solarradiation ASC
LIMIT 3
