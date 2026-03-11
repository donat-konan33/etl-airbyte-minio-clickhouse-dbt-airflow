{{ config(materialized='view') }}

SELECT
    reg_name,
    avg(regavg_humidity) AS regavg_humidity

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY reg_name
ORDER BY regavg_humidity DESC
LIMIT 3
