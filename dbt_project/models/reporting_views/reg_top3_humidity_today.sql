{{ config(materialized='view') }}

SELECT
    reg_name,
    round(avg(humidity), 2) AS regavg_humidity

FROM {{ ref('mart_today_stats') }}
GROUP BY reg_name
ORDER BY regavg_humidity DESC
LIMIT 3
