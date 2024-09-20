using System.Data;
using System.Data.Common;
using Npgsql;
using NpgsqlTypes;

namespace Sylvan.Data.Etl.Providers.Npgsql;

public class NpgsqlProvider : DbProvider
{
	public const string DefaultSchemaName = "public";

	public override string DefaultSchema => DefaultSchemaName; 

	readonly string connectionString;

	public NpgsqlProvider(string connectionString)
	{
		this.connectionString = connectionString;
	}

	public override int MaxIdentifierLength => 63;

	public override DbConnection GetConnection()
	{
		var conn = new NpgsqlConnection(connectionString);
		conn.Open();
		return conn;
	}

	public override IEnumerable<DbColumn> GetSchema(string tableName)
	{
		using var conn = GetConnection();
		using var cmd = conn.CreateCommand();
		cmd.CommandText = $"select * from \"{tableName}\" limit 0;";
		cmd.CommandType = CommandType.Text;
		using var reader = cmd.ExecuteReader();
		return reader.GetColumnSchema();
	}

	string BuildTable(TableInfo table)
	{
		var w = new StringWriter();
		w.WriteLine($"create table \"{table.TableSchema}\".\"{table.TableName}\" (");

		var first = true;
		foreach (var col in table.Columns)
		{
			if (first)
			{
				first = false;
			}
			else
			{
				w.WriteLine(",");
			}

			var colName = col.ColumnName;
			w.Write('\"');
			w.Write(colName);
			w.Write('\"');
			w.Write(' ');
			var type = col.DataType;
			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					w.Write("boolean");
					break;
				case TypeCode.Byte:
					w.Write("smallint");
					break;
				case TypeCode.Int16:
					w.Write("smallint");
					break;
				case TypeCode.Int32:
					w.Write("integer");
					break;
				case TypeCode.Int64:
					w.Write("bigint");
					break;
				case TypeCode.String:
					w.Write("text");
					//w.Write("(");
					//w.Write(Math.Min(col.ColumnSize ?? 32, 256));
					//w.Write(")");
					break;
				case TypeCode.DateTime:
					w.Write("timestamp");
					break;
				case TypeCode.Single:
					w.Write("real");
					break;
				case TypeCode.Double:
					w.Write("double precision");
					break;
				case TypeCode.Decimal:
					w.Write("numeric");
					if(col.NumericPrecision != null)
					{
						w.Write($"({col.NumericPrecision}, {col.NumericScale})");
					}
					break;
				default:
					if (type == typeof(byte[]))
					{
						w.Write("bytea");
						break;
					}
					if (type == typeof(Guid))
					{
						w.Write("uuid");
						break;
					}

					throw new NotSupportedException();
			}

			w.Write(' ');
			w.Write(col.DataType != typeof(string) && col.AllowDBNull == false ? " not null" : "null");
		}
		w.WriteLine();
		w.WriteLine(");");
		//w.Write($"alter table {tableName} add primary key (");
		//first = true;
		//foreach (var col in cols)

		//{
		//	if (col.IsKey == true)
		//	{
		//		if (first)
		//		{
		//			first = false;
		//		}
		//		else
		//		{
		//			w.Write(",");
		//		}
		//		w.WriteLine(DbStyle.Convert(col.ColumnName));
		//	}
		//}
		//w.WriteLine(");");
		return w.ToString();
	}

	public override long LoadData(TableMapping mapping, DbDataReader data)
	{
		using var conn = (NpgsqlConnection)GetConnection();
		var schema = data.GetColumnSchema();
		var cmd = conn.CreateCommand();

		return WriteData(conn, mapping.TargetTable!, data);
	}

	static long WriteData(NpgsqlConnection conn, TableInfo table, DbDataReader data)
	{
		var schema = table.Columns;

		var sw = new StringWriter();
		sw.WriteLine($"copy {table.TableSchema}.\"{table.TableName}\" (");
		for (int i = 0; i < schema.Count; i++)
		{
			if (i > 0)
				sw.Write(", ");

			var colSchema = schema[i];
			sw.Write('\"');
			sw.Write(colSchema.ColumnName);
			sw.Write('\"');
		}

		sw.Write(")");
		sw.Write("from stdin (format binary);");

		var cmd = sw.ToString();

		var bi = conn.BeginBinaryImport(cmd);
		while (data.Read())
		{
			bi.StartRow();
			for (var i = 0; i < data.FieldCount; i++)
			{
				if (data.IsDBNull(i))
				{
					bi.WriteNull();
					continue;
				}
				var type = schema[i].DataType;
				var dbType = GetType(type);

				switch (dbType)
				{
					case NpgsqlDbType.Text:
						var t = data.GetString(i);
						bi.Write(t, dbType);
						break;
					case NpgsqlDbType.Char:
						var str = data.GetString(i);
						bi.Write(str, dbType);
						break;
					case NpgsqlDbType.Smallint:
						// TODO: need to figure out "tinyint" scenario. npg doesn't support it.
						bi.Write(data.GetInt16(i), dbType);
						break;
					case NpgsqlDbType.Integer:
						bi.Write(data.GetInt32(i), dbType);
						break;
					case NpgsqlDbType.Bigint:
						bi.Write(data.GetInt64(i), dbType);
						break;
					case NpgsqlDbType.Boolean:
						bi.Write(data.GetBoolean(i), dbType);
						break;
					case NpgsqlDbType.Double:
						bi.Write(data.GetDouble(i), dbType);
						break;
					case NpgsqlDbType.Real:
						bi.Write(data.GetFloat(i), dbType);
						break;
					case NpgsqlDbType.Money:
					case NpgsqlDbType.Numeric:
						bi.Write(data.GetDecimal(i), dbType);
						break;
					case NpgsqlDbType.Timestamp:
						bi.Write(data.GetDateTime(i), dbType);
						break;
					case NpgsqlDbType.Uuid:
						bi.Write(data.GetGuid(i), dbType);
						break;
					case NpgsqlDbType.Bytea:
						bi.Write((byte[])data.GetValue(i), dbType);
						break;
					default:
						throw new NotSupportedException();
				}
			}
		}
		return (long)bi.Complete();
	}

	static NpgsqlDbType GetType(Type type)
	{
		if (type == typeof(string))
			return NpgsqlDbType.Text;

		if (type == typeof(byte))
			return NpgsqlDbType.Smallint;

		if (type == typeof(int))
			return NpgsqlDbType.Integer;

		if (type == typeof(short))
			return NpgsqlDbType.Smallint;

		if (type == typeof(long))
			return NpgsqlDbType.Bigint;

		if (type == typeof(bool))
			return NpgsqlDbType.Boolean;

		if (type == typeof(float))
			return NpgsqlDbType.Real;

		if (type == typeof(double))
			return NpgsqlDbType.Double;

		if (type == typeof(decimal))
			return NpgsqlDbType.Numeric;

		if (type == typeof(DateTime))
			return NpgsqlDbType.Timestamp;

		if (type == typeof(byte[]))
			return NpgsqlDbType.Bytea;

		if (type == typeof(Guid))
			return NpgsqlDbType.Uuid;

		throw new NotSupportedException();
	}

	public override DbType? GetType(string typeName)
	{
		return null;
	}

	protected override string BuildTable(string name, IEnumerable<DbColumn> cols)
	{
		throw new NotImplementedException();
	}
}
