{{ config(materialized='view') }}

SELECT
    department,
    cloudcover

FROM {{ ref('mart_today_stats') }}
ORDER BY cloudcover DESC
LIMIT 3
