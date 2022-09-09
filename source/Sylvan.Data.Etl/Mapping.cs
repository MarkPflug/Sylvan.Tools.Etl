namespace Sylvan.Data.Etl;

interface IMapping
{
	TableInfo? MapTable(TableInfo sourceTable);

	ColumnInfo? MapColumn(TableInfo sourceTable, ColumnInfo sourceColumn);
}

class Mapping : IMapping
{
	public static IMapping Identity => new Mapping();

	public virtual ColumnInfo? MapColumn(TableInfo sourceTable, ColumnInfo sourceColumn)
	{
		return sourceColumn;
	}

	public virtual TableInfo? MapTable(TableInfo sourceTable)
	{
		return sourceTable;
	}
}

