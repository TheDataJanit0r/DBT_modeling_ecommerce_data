select
   T.organic_certified AS organic_certified 
from
   (
      select distinct
         m.raw_values::JSON->'organic_certified'->'<all_channels>'->>'<all_locales>' AS organic_certified 
      from
         {{ref('pim_catalog_product_model')}} m 
      where
         (
            m.raw_values::JSON->'organic_certified'->'<all_channels>'->>'<all_locales>' is not null
         )
   )
   T 
where
   (
       T.organic_certified not similar to '^[A-Z]{2}-(BIO|ÖKO)-[0-9]{3}$'
   )
