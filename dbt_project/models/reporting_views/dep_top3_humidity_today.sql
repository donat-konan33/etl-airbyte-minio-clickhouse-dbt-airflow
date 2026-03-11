{{ config(materialized='view') }}

SELECT
    department,
    humidity

FROM {{ ref('mart_today_stats') }}
ORDER BY humidity DESC
LIMIT 3
