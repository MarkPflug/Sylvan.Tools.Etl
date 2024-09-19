select 
	table_schema,
	table_name,
	column_name,
	ordinal_position,
	is_nullable,
	data_type,
	character_maximum_length,
	character_octet_length,
	numeric_precision,
	numeric_precision_radix,
	numeric_scale,
	datetime_precision,
	character_set_name
from information_schema.columns
order by 
	table_schema, 
	table_name, 
	ordinal_position