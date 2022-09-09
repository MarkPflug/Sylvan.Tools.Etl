using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;

namespace Sylvan.Data.Etl;

public class SqliteProvider : DbProvider
{
	string connectionString;

	string BuildTable(string name, IEnumerable<DbColumn> cols)
	{
		var w = new StringWriter();

		w.WriteLine("create table " + name + " (");

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

			w.Write('\"');
			w.Write(col.ColumnName);
			w.Write('\"');
			w.Write(' ');
			switch (Type.GetTypeCode(col.DataType))
			{
				case TypeCode.Boolean:
				case TypeCode.Int32:
				case TypeCode.Int64:
					w.Write("integer");
					break;
				case TypeCode.String:
					w.Write("text");
					break;
				case TypeCode.DateTime:
					w.Write("int");
					break;
				case TypeCode.Single:
				case TypeCode.Double:
					w.Write("real");
					break;
				case TypeCode.Decimal:
					w.Write("numeric");
					break;
				default:
					throw new NotSupportedException();
			}
		}
		w.WriteLine();
		w.WriteLine(");");
		return w.ToString();
	}

	public SqliteProvider(string connectionString)
	{
		this.connectionString = connectionString;
	}

	public override DbConnection GetConnection()
	{
		var conn = new SQLiteConnection(this.connectionString);
		conn.Open();
		return conn;
	}

	public override long LoadData(string table, DbDataReader data)
	{
		var conn = GetConnection();

		{ 
			var cmd = conn.CreateCommand();
			var tbl = BuildTable(table, data.GetColumnSchema());

			cmd.CommandText = tbl;
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