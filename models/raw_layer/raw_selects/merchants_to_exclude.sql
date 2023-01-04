select unnest(
  string_to_array(
    replace(
      replace({{merchants_to_exclude()}}::text, ')', '')
      , '(', '')
    , ','))  merchant_key

order by 1
