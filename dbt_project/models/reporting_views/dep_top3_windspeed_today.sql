{{ config(materialized='view') }}

SELECT
    department,
    windspeed

FROM {{ ref('mart_today_stats') }}
ORDER BY windspeed DESC
LIMIT 3
