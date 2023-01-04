{{
config({
    "materialized": "incremental",
    "unique_key": "index",
    "post-hook": [
      "{{ index(this, 'index')}}"
    ],
    })
}}

with base as (select row_number() over (order by "sendDate"::timestamp) as index,
"sendDate"::timestamp as send_date
,"sendingId" as sending_id
,event, "eventId" as event_id
,email
,"mailingId" as mailing_id
,"mailingName" as mailing_name
,_links::json -> 'self' ->> 'href' as url_link

 from inxmail.sendings)

 select *,row_number() over() "unused_id" from base  
{% if is_incremental() %}
  where index > (select max(index) from {{ this }})
{% endif %}