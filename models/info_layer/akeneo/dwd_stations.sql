select 
to_date(date_start::text, 'YYYYMMDD') as start_date
,to_date(date_end::text, 'YYYYMMDD') as end_date
, * 
from {{ref('dwd_weather_stations')}}
