{{
  config(
    materialized='view'
  )
}}

select
  geo_point_2d,
  geo_shape,
  reg_name,
  reg_code,
  dep_name,
  LOWER(TRIM((REGEXP_REPLACE(dep_normalized, '[^A-Za-z0-9]', '')))) AS department_lower,
  dep_code,
  dep_status
from {{ ref('stg_depcode_') }}
