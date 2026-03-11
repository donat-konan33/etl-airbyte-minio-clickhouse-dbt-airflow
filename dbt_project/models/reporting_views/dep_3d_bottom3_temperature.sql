-- dep_3d_bottom3_temperature.sql
-- DBT view definition placeholder

-- datawarehouse.dep_3d_bottom3_temperature source

{{ config(materialized='view') }}

SELECT
    department,
    avg(depavg_temp_c) AS depavg_temperature
FROM {{ ref('mart_next_3_days_stats') }}
GROUP BY department
ORDER BY depavg_temperature
LIMIT 3
