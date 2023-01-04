{{
config({
    "materialized": "incremental",
    "unique_key": "index",
    "post-hook": [
      "{{ index(this, 'index')}}"
    ],
    })
}}

with base as (select row_number() over (order by (sendouts::json -> 0 ->> 'deliveryDate')::timestamp) as index,
(sendouts::json -> 0 ->> 'deliveryDate')::timestamp as delivery_date
,sendouts::json -> 0 ->> 'email' as email
,"deliveryId" as delivery_id
,"sendingId" as sending_id
,sendouts::json -> 0 ->> 'targetHost' as target_host
,sendouts::json -> 0 ->> 'targetIp' as target_ip
,sendouts::json -> 0 ->> 'dsnMessage' as dsn_message
,sendouts::json -> 0 ->> 'dsnCode' as dsn_code
,sendouts::json -> 0 ->> 'status' as status
 from inxmail.deliveries
)

select *,row_number() over() "unused_id"  from base
{% if is_incremental() %}
  where index > (select max(index) from {{ this }})
{% endif %}
