
select 
tc.constraint_name,
kcu.table_schema,
kcu.table_name,
kcu.column_name,
kcu.ORDINAL_POSITION
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
join INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
	on tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG
	and tc.CONSTRAINT_schema = kcu.CONSTRAINT_schema
	and tc.CONSTRAINT_Name = kcu.CONSTRAINT_name
 
where tc.constraint_Type = 'primary key'
order by 
tc.constraint_name,
kcu.ORDINAL_POSITION