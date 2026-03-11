{{ config(materialized='view') }}

SELECT
    reg_name,
    round(avg(solarenergy_kwhpm2), 2) AS regavg_solarenergy

FROM {{ ref('mart_today_stats') }}
GROUP BY reg_name
ORDER BY regavg_solarenergy DESC
LIMIT 3
