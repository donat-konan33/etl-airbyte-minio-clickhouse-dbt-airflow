{{ config(materialized='view') }}

SELECT
    reg_name,
    avg(regavg_cloudcover) AS regavg_cloudcover

FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY reg_name
ORDER BY regavg_cloudcover ASC
LIMIT 3
