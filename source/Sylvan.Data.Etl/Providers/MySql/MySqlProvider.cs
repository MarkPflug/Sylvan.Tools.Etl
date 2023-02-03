using MySqlConnector;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data.Etl;

public class MySqlProvider : DbProvider
{
	string connectionString;

	void BuildTable(TableInfo table, TextWriter w)
	{
		w.WriteLine($"create table {table.TableSchema}.{table.TableName} (");

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
			w.Write(colName);
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
	}

	public MySqlProvider(string connectionString)
	{
		this.connectionString = connectionString;
	}

	public override DbConnection GetConnection()
	{
		var conn = new MySqlConnection(this.connectionString);
		conn.Open();
		return conn;
	}

	public override long LoadData(TableMapping table, DbDataReader data)
	{
		using var conn = GetConnection();

		{
			var cmd = conn.CreateCommand();
			var sw = new StringWriter();
			BuildTable(table.TargetTable!, sw);

			cmd.CommandText = sw.ToString();
			cmd.ExecuteNonQuery();
		}

		ReadOnlyCollection<DbColumn> ss;
		{
			var cmd = conn.CreateCommand();
			cmd.CommandText = "select * from " + table + " limit 0;";
			var r = cmd.ExecuteReader();
			ss = r.GetColumnSchema();
		}

		var cmdW = new StringWriter();
		cmdW.Write("insert into " + table + " values(");
		int i = 0;
		foreach (var c in ss)
		{
			if (i > 0)
				cmdW.Write(",");
			cmdW.Write("$p" + i++);
		}

		cmdW.Write(");");
		var cmdt = cmdW.ToString();
		long count = 0;

		using (var tx = conn.BeginTransaction())
		{
			var cmd = conn.CreateCommand();
			cmd.CommandText = cmdt;
			for (i = 0; i < data.FieldCount; i++)
			{
				var p = cmd.CreateParameter();
				p.ParameterName = "$p" + i;
				cmd.Parameters.Add(p);
			}
			cmd.Prepare();
			while (data.Read())
			{
				for (i = 0; i < data.FieldCount; i++)
				{
					cmd.Parameters[i].Value = data.GetValue(i);
				}
				count++;
				cmd.ExecuteNonQuery();
			}

			tx.Commit();
		}
		return count;
	}

	public override DbType GetType(string typeName)
	{
		throw new NotImplementedException();
	}
}