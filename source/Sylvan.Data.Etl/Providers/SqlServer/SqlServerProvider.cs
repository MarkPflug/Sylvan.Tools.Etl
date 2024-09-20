using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data.Etl.Providers.SqlServer;

[Flags]
public enum MergeAction
{
	None = 0,
	Insert = 1,
	Update = 2,
	Delete = 4,
	All = Insert | Update | Delete
}

public sealed class SqlServerProvider : DbProvider
{
	static readonly Dictionary<string, DbType> TypeMap;

	public const string DefaultSchemaName = "dbo";

	public override string DefaultSchema => DefaultSchemaName;

	public override int MaxIdentifierLength => 128;

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
				{"nvarchar", DbType.String },
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

	protected override string BuildTable(string name, IEnumerable<DbColumn> cols)
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
		if (connectionString.Contains("="))
		{
			this.connectionString = connectionString;
		}
		else
		{
			var csb = new SqlConnectionStringBuilder()
			{
				InitialCatalog = connectionString,
				DataSource = ".",
				IntegratedSecurity = true,
				TrustServerCertificate = true,
			};

			this.connectionString = csb.ConnectionString;
		}
	}

	public override DbConnection GetConnection()
	{
		var conn = new SqlConnection(this.connectionString);
		conn.Open();
		return conn;
	}

	public SqlConnection GetSqlConnection()
	{
		return (SqlConnection)GetConnection();
	}

	public override long LoadData(TableMapping table, DbDataReader data)
	{
		using var sqlConn = (SqlConnection)GetConnection();

		using var bc = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.TableLock, null);
		bc.BulkCopyTimeout = 0;
		bc.EnableStreaming = true;
		bc.BatchSize = 0x1000;

		var t = table.TargetTable!;
		bc.DestinationTableName = t.TableSchema + "." + t.TableName;
		bc.WriteToServer(data);
		return -1;
	}

	public override DbType? GetType(string typeName)
	{
		return TypeMap.TryGetValue(typeName, out var type) ? type : null;
	}

	public string BuildMergeCommand(string src, string dest, MergeAction action = MergeAction.Insert | MergeAction.Update)
	{
		if (action == MergeAction.None)
			throw new ArgumentOutOfRangeException("action");

		var conn = GetSqlConnection();

		using var cmd = conn.CreateCommand();

		cmd.CommandText = Sql.MergeInfo;

		using var reader = cmd.ExecuteReader();

		var keyCols = new HashSet<string>();
		var cols = new HashSet<string>();

		while (reader.Read())
		{
			var colName = reader.GetString(0);
			keyCols.Add(colName);
		}
		reader.NextResult();
		while (reader.Read())
		{
			var colName = reader.GetString(0);
			if (!keyCols.Contains(colName))

				cols.Add(colName);
		}

		var sw = new StringWriter();
		sw.Write("merge " + dest + " as d ");
		sw.Write("using ( select ");
		bool first = true;

		foreach (var c in keyCols.Concat(cols))
		{
			if (!first)
				sw.Write(",");
			first = false;
			sw.Write(c);
		}
		sw.Write(" from " + src + ") as s ");
		sw.Write("on ");

		first = true;
		foreach (var c in keyCols)
		{
			if (!first)
				sw.Write(" and ");
			first = false;
			sw.Write($"s.{c} = d.{c} ");
		}
		if (action.HasFlag(MergeAction.Insert))
		{
			sw.Write(" when not matched by target then insert (");

			first = true;
			foreach (var c in keyCols.Concat(cols))
			{
				if (!first)
					sw.Write(",");
				first = false;
				sw.Write(c);
			}

			sw.Write(") values (");
			first = true;
			foreach (var c in keyCols.Concat(cols))
			{
				if (!first)
					sw.Write(",");
				first = false;
				sw.Write(c);
			}

			sw.Write(")");
		}
		if (action.HasFlag(MergeAction.Update))
		{
			sw.Write(" when matched then update set ");

			first = true;
			foreach (var c in cols)
			{
				if (!first)
					sw.Write(",");
				first = false;
				sw.Write($" d.{c} = s.{c} ");
			}
		}

		if (action.HasFlag(MergeAction.Delete))
		{
			sw.Write(" when not matched by source then delete ");
		}
		sw.Write(";");

		return sw.ToString();
	}
}
