select 
	c.table_schema,
	c.table_name,
	c.column_name,
	c.ordinal_position,
	c.is_nullable,
	c.data_type,
	c.character_maximum_length,
	c.character_octet_length,
	c.numeric_precision,
	c.numeric_precision_radix,
	c.numeric_scale,
	c.datetime_precision,
	c.character_set_name
from information_schema.columns c
join information_schema.tables t
	on c.table_catalog = t.table_catalog and c.table_schema = t.table_schema and c.table_name = t.table_name
where t.table_type = 'BASE TABLE'
order by 
	table_schema, 
	table_name, 
	ordinal_position