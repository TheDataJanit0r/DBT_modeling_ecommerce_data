with sendings as (
select send_date::date as date_at, count(*) as sendings from {{ref('sendings')}} group by 1)

,bounces as (select delivery_date::date as date_at, count(*) as bounces from {{ref('deliveries')}} where status = 'bounced' group by 1)

,opens as (select date_at::date, count(*) as opens from {{ref('reactions')}} where reaction_type = 'open' group by 1)

,clicks as (select date_at::date, count(*) as clicks from {{ref('reactions')}} where reaction_type = 'click' group by 1)

select * from sendings
full outer join bounces using (date_at)
full outer join opens using (date_at)
full outer join clicks using (date_at)
order by 1 desc

