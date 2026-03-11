{{ config(materialized='view') }}

WITH min_date_ AS (

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

WHERE dates = (SELECT min_date FROM min_date_)
