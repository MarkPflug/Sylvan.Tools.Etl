using System.Data;

namespace Sylvan.Data.Etl;

public interface IMapping
{
	TableInfo? MapTable(TableInfo sourceTable);

	ColumnInfo? MapColumn(TableInfo sourceTable, ColumnInfo sourceColumn);
}

public class Mapping : IMapping
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

public class NameStyleMapping : Mapping
{
	IdentifierStyle style;
	public NameStyleMapping(IdentifierStyle style)
	{
		this.style = style;
	}

	public override TableInfo? MapTable(TableInfo sourceTable)
	{
		var schema = style.Convert(sourceTable.TableSchema);
		var name = style.Convert(sourceTable.TableName);
		return new TableInfo(schema, name);
	}

	public override ColumnInfo? MapColumn(TableInfo sourceTable, ColumnInfo sourceColumn)
	{
		var name = style.Convert(sourceColumn.ColumnName);
		return new ColumnInfo(name, sourceColumn.DataTypeName, sourceColumn.DbType, sourceColumn.AllowDBNull ?? true, sourceColumn.ColumnSize);
	}
}
