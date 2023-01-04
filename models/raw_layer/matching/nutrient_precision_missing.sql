select
   code 
from
   {{ref('pim_catalog_product_model')}} p
where
   (
      p.raw_values::JSON->'protein_precision'->'<all_channels>'->>'<all_locales>' is null

      and 
      (
      p.raw_values::JSON->'protein'->'<all_channels>'->>'<all_locales>' is not null
      )
) 
      or 
      (
      p.raw_values::JSON->'fat_precision'->'<all_channels>'->>'<all_locales>' is null
         and 
         (
      p.raw_values::JSON->'fat'->'<all_channels>'->>'<all_locales>' is not null
         )
      )
      or 
      (
      p.raw_values::JSON->'choavl_precision'->'<all_channels>'->>'<all_locales>' is null
         and 
         (
      p.raw_values::JSON->'choavl'->'<all_channels>'->>'<all_locales>' is not null
         )
      )
      or 
      (
      p.raw_values::JSON->'fasat_precision'->'<all_channels>'->>'<all_locales>' is null
         and 
         (
      p.raw_values::JSON->'fasat'->'<all_channels>'->>'<all_locales>' is not null
         )
      )
      or 
      (
      p.raw_values::JSON->'salteq_precision'->'<all_channels>'->>'<all_locales>' is null
         and 
         (
      p.raw_values::JSON->'salteq'->'<all_channels>'->>'<all_locales>' is not null
         )
      )
      or 
      (
      p.raw_values::JSON->'sugar_precision'->'<all_channels>'->>'<all_locales>' is null
         and 
         (
      p.raw_values::JSON->'sugar'->'<all_channels>'->>'<all_locales>' is not null
         )
      )
   
