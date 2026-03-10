{{
  config(
    materialized='table',
    engine='ReplacingMergeTree',
    post_hook="
      CREATE TABLE IF NOT EXISTS {{ this }}
      ENGINE = ReplacingMergeTree(version)
      PARTITION BY toYYYYMM(dates)
      ORDER BY (dates, department)
    "
  )
}}

SELECT * FROM {{ source('clickhouse', 'archived_data') }}
UNION ALL
SELECT * FROM {{ ref('mart_newdata') }}
