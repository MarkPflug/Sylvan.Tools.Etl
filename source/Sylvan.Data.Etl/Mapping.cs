using Sylvan.CodeGeneration;

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
		var schema = ProcessName(sourceTable.TableSchema);
		var name = ProcessName(sourceTable.TableName);
		return new TableInfo(schema, name);
	}

	public override ColumnInfo? MapColumn(TableInfo sourceTable, ColumnInfo sourceColumn)
	{
		var name = ProcessName(sourceColumn.ColumnName);
		return new ColumnInfo(
			name, 
			sourceColumn.DataTypeName, 
			sourceColumn.DbType, 
			sourceColumn.AllowDBNull ?? true, 
			sourceColumn.ColumnSize, 
			sourceColumn.NumericPrecision, 
			sourceColumn.NumericScale
		);
	}

	string ProcessName(string name)
	{
		if (name.Contains('-'))
		{
			return name.ToLower();
		}
		return style.Convert(name);
	}
}
