{{ config(materialized='view') }}

SELECT
    reg_name,
    avg(regavg_solarenergy_kwhpm2) AS regavg_solarenergy_kwhpm2

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY reg_name
ORDER BY regavg_solarenergy_kwhpm2 ASC
LIMIT 3
