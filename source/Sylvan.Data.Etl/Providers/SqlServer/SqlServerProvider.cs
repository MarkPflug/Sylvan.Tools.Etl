using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace Sylvan.Data.Etl.Providers.SqlServer;

public class SqlServerProvider : DbProvider
{

	static readonly Dictionary<string, DbType> TypeMap;

	static SqlServerProvider()
	{
		TypeMap = new Dictionary<string, DbType>(StringComparer.OrdinalIgnoreCase)
			{
				{"bit", DbType.Boolean },
				{"tinyint", DbType.Byte },
				{"smallint", DbType.Int16 },
				{"int", DbType.Int32 },
				{"bigint", DbType.Int64 },
				{"binary", DbType.Binary },
				{"date", DbType.Date },
				{"datetime", DbType.DateTime },
				{"datetime2", DbType.DateTime2 },
				{"datetimeoffset", DbType.DateTimeOffset },
				{"varchar", DbType.AnsiString },
				{"char", DbType.AnsiStringFixedLength },
				{"real", DbType.Single },
				{"float", DbType.Double },
				{"decimal", DbType.Decimal },
				{"numeric", DbType.Decimal },
				{"smallmoney", DbType.Decimal },
				{"money", DbType.Decimal },
				{"ntext", DbType.String },
				{"image", DbType.Binary },
			};
	}

	string connectionString;

	string BuildTable(string name, IEnumerable<DbColumn> cols)
	{
		var w = new StringWriter();

		w.WriteLine("create table [" + name + "] (");

		var first = true;
		foreach (var col in cols)
		{
			if (first)
			{
				first = false;
			}
			else
			{
				w.WriteLine(",");
			}
			w.Write('[');
			w.Write(col.ColumnName);
			w.Write(']');
			w.Write(' ');
			switch (Type.GetTypeCode(col.DataType))
			{
				case TypeCode.Boolean:
					w.Write("bit");
					break;
				case TypeCode.Int32:
					w.Write("int");
					break;
				case TypeCode.Int64:
					w.Write("bigint");
					break;
				case TypeCode.String:
					if (col.IsLong == true)
					{
						w.Write("varchar(max)");
					}
					else
					{
						w.Write("varchar");
						var size = col.ColumnSize;
						var len = size == null ? 512 : Math.Min(8000, size.Value * 2);
						len = Math.Max(8000, len);
						w.Write('(');
						w.Write(len);
						w.Write(')');
					}
					break;
				case TypeCode.DateTime:
					w.Write("datetime2");
					break;
				case TypeCode.Single:
					w.Write("float(24)");
					break;
				case TypeCode.Double:
					w.Write("float(53)");
					break;
				case TypeCode.Decimal:
					w.Write("numeric");
					break;
				default:
					throw new NotSupportedException();
			}

			w.Write(' ');
			w.Write(col.DataType != typeof(string) && col.AllowDBNull == false ? " not null" : "null");
		}
		w.WriteLine();
		w.WriteLine(");");
		return w.ToString();
	}

	public SqlServerProvider(string connectionString)
	{
		this.connectionString = connectionString;
	}

	public override DbConnection GetConnection()
	{
		var conn = new SqlConnection(this.connectionString);
		conn.Open();
		return conn;
	}

	public override long LoadData(TableMapping table, DbDataReader data)
	{
		using var sqlConn = (SqlConnection)GetConnection();
		//var tbl = BuildTable(table, data.GetColumnSchema());
		//var cmd = sqlConn.CreateCommand();
		//cmd.CommandText = tbl;
		try
		{
			//	cmd.ExecuteNonQuery();
		}
		catch (Exception e)
		{
			throw new InvalidOperationException($"Failed to create table {table}.", e);
		}

		using var bc = new SqlBulkCopy(sqlConn);
		bc.BulkCopyTimeout = 0;
		bc.EnableStreaming = true;

		var t = table.TargetTable!;
		bc.DestinationTableName = t.TableSchema + "." +t.TableName;
		bc.WriteToServer(data);
		return -1;		
	}

	public override DbType GetType(string typeName)
	{
		return TypeMap.TryGetValue(typeName, out var type) ? type : DbType.Object;
	}
}