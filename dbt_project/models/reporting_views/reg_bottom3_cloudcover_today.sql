{{ config(materialized='view') }}

SELECT
    reg_name,
    round(avg(cloudcover), 2) AS regavg_cloudcover

FROM {{ ref('mart_today_stats') }}
GROUP BY reg_name
ORDER BY regavg_cloudcover ASC
LIMIT 3
