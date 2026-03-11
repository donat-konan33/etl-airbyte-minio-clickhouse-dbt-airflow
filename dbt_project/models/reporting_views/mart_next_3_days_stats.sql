{{ config(materialized='view') }}

SELECT
    reg_name,
    department,

    temp,
    round(avg(temp) OVER (PARTITION BY mn3d.department), 1) AS depavg_temp_c,
    round(avg(temp) OVER (PARTITION BY mn3d.reg_name), 1) AS regavg_temp_c,

    solarenergy_kwhpm2,
    round(avg(solarenergy_kwhpm2) OVER (PARTITION BY mn3d.department), 2) AS depavg_solarenergy_kwhpm2,
    round(avg(solarenergy_kwhpm2) OVER (PARTITION BY mn3d.reg_name), 2) AS regavg_solarenergy_kwhpm2,

    solarradiation,
    round(avg(solarradiation) OVER (PARTITION BY mn3d.department), 2) AS depavg_solarradiation,
    round(avg(solarradiation) OVER (PARTITION BY mn3d.reg_name), 2) AS regavg_solarradiation,

    humidity,
    round(avg(humidity) OVER (PARTITION BY mn3d.department), 2) AS depavg_humidity,
    round(avg(humidity) OVER (PARTITION BY mn3d.reg_name), 2) AS regavg_humidity,

    windspeed,
    round(avg(windspeed) OVER (PARTITION BY mn3d.department), 2) AS depavg_windspeed,
    round(avg(windspeed) OVER (PARTITION BY mn3d.reg_name), 2) AS regavg_windspeed,

    pressure,
    round(avg(pressure) OVER (PARTITION BY mn3d.department), 2) AS depavg_pressure,
    round(avg(pressure) OVER (PARTITION BY mn3d.reg_name), 2) AS regavg_pressure,

    cloudcover,
    round(avg(cloudcover) OVER (PARTITION BY mn3d.department), 2) AS depavg_cloudcover,
    round(avg(cloudcover) OVER (PARTITION BY mn3d.reg_name), 2) AS regavg_cloudcover

FROM {{ ref('mart_next_3_days') }} AS mn3d
