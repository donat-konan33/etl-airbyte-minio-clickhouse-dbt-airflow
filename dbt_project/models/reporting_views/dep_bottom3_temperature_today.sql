{{ config(materialized='view') }}

SELECT
    department,
    temp AS temperature

FROM {{ ref('mart_today_stats') }}
ORDER BY temperature ASC
LIMIT 3
