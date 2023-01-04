{{
config({
    "materialized": "incremental",
    "unique_key": "index",
    "post-hook": [
      "{{ index(this, 'index')}}"
    ],
    })
}}

with base as (select row_number() over (order by "reactionDate"::timestamp) as index,
    "reactionDate"::timestamp as date_at,
    "reactionType" as reaction_type,
    "reportAlias" as report_alias,
    event,
    "trackingHash" as tracking_hash
from
    inxmail.reactions_all)

select *,row_number() over() "unused_id" from base
{% if is_incremental() %}
  where index > (select max(index) from {{ this }})
{% endif %}
