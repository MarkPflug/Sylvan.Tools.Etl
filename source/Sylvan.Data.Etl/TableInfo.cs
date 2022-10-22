using System.Data;
using System.Data.Common;

namespace Sylvan.Data.Etl;

public class TableInfo
{
	public TableInfo(string tableSchema, string tableName)
	{
		this.TableSchema = tableSchema;
		this.TableName = tableName;
		this.Columns = new List<ColumnInfo>();
	}

	public string TableSchema { get; }

	public string TableName { get; }

	public List<ColumnInfo> Columns { get; }

	public override string ToString()
	{
		return $"\"{TableSchema}\".\"{TableName}\"";
	}
}

public class ColumnInfo : DbColumn
{
	public ColumnInfo(string name, string typeName, DbType type, bool allowNull, int? textLength)
	{
		this.ColumnName = name;
		this.DataTypeName = typeName;
		this.DbType = type;
		this.DataType = GetDataType(type);
		this.AllowDBNull = allowNull;
		this.ColumnSize = textLength;
	}

	Type GetDataType(DbType type)
	{
		switch (type)
		{
			case DbType.Byte:
				return typeof(byte);
			case DbType.Boolean:
				return typeof(bool);
			case DbType.Int16:
				return typeof(short);
			case DbType.Int32:
				return typeof(int);
			case DbType.Int64:
				return typeof(long);
			case DbType.String:
			case DbType.StringFixedLength:
			case DbType.AnsiString:
			case DbType.AnsiStringFixedLength:
				return typeof(string);
			case DbType.Decimal:
				return typeof(decimal);
			case DbType.Date:
			case DbType.DateTime:
			case DbType.DateTime2:
				return typeof(DateTime);
			case DbType.Double:
				return typeof(double);
			case DbType.Single:
				return typeof(float);
			case DbType.Binary:
				return typeof(byte[]);
			default:
				throw new NotSupportedException();
		}
	}

	public DbType DbType { get; }

	public override string ToString()
	{
		return $"{this.ColumnName} ({this.DbType}) {(this.AllowDBNull == false ? "" : "null")}";
	}
}

public class DatabaseMapping
{
	public DatabaseMapping(List<TableMapping> tableMappings)
	{
		this.TableMappings = tableMappings;
	}

	public List<TableMapping> TableMappings { get; }

	public TableMapping? this[string schema, string table]
	{
		get
		{
			var c = StringComparer.OrdinalIgnoreCase;
			foreach (var m in TableMappings)
			{
				if (c.Equals(schema, m.SourceTable.TableSchema) && c.Equals(table, m.SourceTable.TableName))
				{
					return m;
				}
			}
			return null;
		}
	}
}

public class TableMapping
{
	public TableMapping(TableInfo source, TableInfo? target)
	{
		this.SourceTable = source;
		this.TargetTable = target;
		this.ColumnMappings = new List<ColumnMapping>();
	}

	public TableInfo SourceTable { get; set; }

	public TableInfo? TargetTable { get; set; }

	public List<ColumnMapping> ColumnMappings { get; set; }
}

public class ColumnMapping
{
	public ColumnInfo SourceColumn { get; }

	public ColumnInfo? TargetColumn { get; }

	public ColumnMapping(ColumnInfo source, ColumnInfo? target)
	{
		this.SourceColumn = source;
		this.TargetColumn = target;
	}

	public override string ToString()
	{
		return $"{SourceColumn.ColumnName} => {TargetColumn?.ColumnName}";
	}
}
