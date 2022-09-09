select 
table_Schema,
table_name,
column_name,
ordinal_position,
is_nullable,
data_type,
CHARACTER_MAXIMUM_LENGTH,
CHARACTER_OCTET_LENGTH,
numeric_precision,
numeric_precision_radix,
numeric_scale,
datetime_precision,
character_set_name
from information_schema.COLUMNS
order by table_schema, table_name, ORDINAL_POSITION