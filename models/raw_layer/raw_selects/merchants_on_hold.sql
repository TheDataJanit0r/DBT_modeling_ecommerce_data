select unnest(
  string_to_array(
    replace(
      replace({{merchants_on_hold()}}::text, ')', '')
      , '(', '')
    , ','))  merchant_key

order by 1