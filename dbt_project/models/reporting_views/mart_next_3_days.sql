-- mart_next_3_days.sql
-- DBT view definition placeholder
-- datawarehouse.mart_next_3_days source

{{ config(materialized='view') }}

WITH min_date AS (

    SELECT min(dates) AS min_date
    FROM {{ ref('mart_newdata_') }}

)

SELECT
    reg_name,
    department,
    temp,
    solarenergy_kwhpm2,
    solarradiation,
    humidity,
    windspeed,
    pressure,
    cloudcover

FROM {{ ref('mart_newdata_') }}

WHERE dates > (SELECT min_date FROM min_date)
  AND dates <= (SELECT min_date FROM min_date) + 3
