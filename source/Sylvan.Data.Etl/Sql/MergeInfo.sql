select 
kcu.column_name
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
join INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
	on tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG
	and tc.CONSTRAINT_schema = kcu.CONSTRAINT_schema
	and tc.CONSTRAINT_Name = kcu.CONSTRAINT_name
 
where tc.constraint_Type = 'primary key'
and kcu.table_schema = 'dbo'
and kcu.table_name = 'test'
order by 
tc.constraint_name,
kcu.ORDINAL_POSITION

select 
column_name,
data_type
from INFORMATION_SCHEMA.columns
where 
table_schema = 'dbo' and
table_name = 'test'
order by ordinal_position
