
-- postgres get table sizes
select
  concat(quote_ident(table_schema), '.', quote_ident(table_name)),
  pg_size_pretty(pg_relation_size(concat(quote_ident(table_schema), '.', quote_ident(table_name)))),
  pg_relation_size(concat(quote_ident(table_schema), '.', quote_ident(table_name)))
from information_schema.tables
where table_schema != 'pg_catalog'
order by 3 desc;