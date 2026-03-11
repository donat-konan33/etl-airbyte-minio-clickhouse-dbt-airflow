{{ config(materialized='view') }}

SELECT
    reg_name,
    department,

    temp,
    round(avg(temp) OVER (PARTITION BY mt.reg_name), 1) AS regavg_temp,

    solarenergy_kwhpm2,
    round(avg(solarenergy_kwhpm2) OVER (PARTITION BY mt.reg_name), 2) AS regavg_solarenergy_kwhpm2,

    solarradiation,
    round(avg(solarradiation) OVER (PARTITION BY mt.reg_name), 2) AS regavg_solarradiation,

    humidity,
    round(avg(humidity) OVER (PARTITION BY mt.reg_name), 2) AS regavg_humidity,

    windspeed,
    round(avg(windspeed) OVER (PARTITION BY mt.reg_name), 2) AS regavg_windspeed,

    pressure,
    round(avg(pressure) OVER (PARTITION BY mt.reg_name), 2) AS regavg_pressure,

    cloudcover,
    round(avg(cloudcover) OVER (PARTITION BY mt.reg_name), 2) AS regavg_cloudcover

FROM {{ ref('mart_today') }} AS mt
