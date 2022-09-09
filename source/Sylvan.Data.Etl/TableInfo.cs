using System.Data.Common;

namespace Sylvan.Data.Etl;

class TableInfo
{
	public TableInfo(string tableSchema, string tableName)
	{
		this.TableSchema = tableSchema;
		this.TableName = tableName;
	}

	public string TableSchema { get; }
	public string TableName { get; }

	public override string ToString()
	{
		return $"\"{TableSchema}\".\"{TableName}\"";
	}
}

class ColumnInfo
{
	public ColumnInfo(string name)
	{
		this.Name = name;
	}

	public string Name { get; }

	public static implicit operator ColumnInfo(DbColumn col)
	{
		return new ColumnInfo(col.ColumnName);
	}
}