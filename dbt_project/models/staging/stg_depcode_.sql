--keep all columns and replace dep_status when null
{{
  config(
    materialized='view',
  )
}}

-- CTE
with depcode_ as (
      select *
      from {{ source('clickhouse', 'raw_depcode_') }}
)

select geo_point_2d,
       geo_shape,
       reg_name,
       reg_code,
       dep_name_upper as dep_name,
       dep_current_code as dep_code,
       IFNULL(dep_status, 'undefined') as dep_status,
       department,
       dep_normalized

from depcode_
