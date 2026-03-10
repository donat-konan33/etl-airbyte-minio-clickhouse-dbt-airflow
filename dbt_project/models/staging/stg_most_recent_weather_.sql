{{
  config(
    materialized="view"
  )
}}

with average_snowdepth as (
  select ROUND(AVG(snowdepth), 1) AS avg_snowdepth
  from {{ source("clickhouse", "latest_weather_") }}
) -- in order to fill missing values with

select
  id as record_id,
  toDate(datetime) as dates, -- Use toDate to extract date from datetime
  datetimeEpoch,
  department,
  resolvedAddress as locations,
  latitude,
  longitude,
  solarenergy,
  solarradiation,
  uvindex,
  temp,
  tempmax,
  tempmin,
  feelslike,
  feelslikemax,
  feelslikemin,
  precip,
  precipprob,
  precipcover,
  snow,
  COALESCE(snowdepth, avg_snowdepth) AS snowdepth_filled,
  dew,
  humidity,
  winddir,
  windspeed,
  windgust,
  pressure,
  severerisk,
  icon,
  cloudcover,
  conditions,
  moonphase,
  description as descriptions,
  sunrise,
  sunset,
  source,
  sunriseEpoch,
  sunsetEpoch,

from {{ source("clickhouse", "latest_weather_") }},
    average_snowdepth
