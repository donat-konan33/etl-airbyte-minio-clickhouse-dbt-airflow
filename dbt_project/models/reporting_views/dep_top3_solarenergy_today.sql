{{ config(materialized='view') }}

SELECT
    department,
    solarenergy_kwhpm2

FROM {{ ref('mart_today_stats') }}
ORDER BY solarenergy_kwhpm2 DESC
LIMIT 3
