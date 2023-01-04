

select *
from {{ref('coke_matching_stammdaten_batch_1')}}

union all

select distinct on  ("Kollex Customer ID") *
from {{ref('coke_matching_stammdaten_batch_2')}}

union all

select distinct on  ("Kollex Customer ID") *
from {{ref('coke_matching_stammdaten_batch_3')}}

union all

select distinct on  ("Kollex Customer ID") *
from {{ref('coke_matching_stammdaten_batch_4')}}

union all

select distinct on  ("Kollex Customer ID") *
from {{ref('coke_matching_stammdaten_batch_5')}}


union all

select distinct on  ("Kollex Customer ID") *
from {{ref('coke_matching_stammdaten_batch_6')}}

