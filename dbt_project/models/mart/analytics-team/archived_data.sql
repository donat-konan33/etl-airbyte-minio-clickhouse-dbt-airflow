{{
    config(
        materialized='incremental',
        engine='MergeTree',
        partition_by='toYYYYMM(dates)',
        order_by='(dates, department, record_id)',
        on_schema_change='sync_all_columns'
    )
}}

SELECT *
FROM {{ ref('mart_newdata_') }}

{% if is_incremental() %}

WHERE record_id NOT IN (
    SELECT record_id
    FROM {{ this }}
)

{% endif %}
