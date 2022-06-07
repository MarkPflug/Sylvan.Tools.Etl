using System;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;
//using Microsoft.Data.Sqlite;
using System.Data.SQLite;

using System.Collections.Generic;

namespace Sylvan.Data.Etl
{
	class SqliteLoader : DataLoader
	{
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

		public override DbConnection GetConnection(string database)
		{
			var conn = new SQLiteConnection("Data Source=" + database);
			conn.Open();
			return conn;
		}

		public override void Load(DbConnection conn, string table, DbDataReader data)
		{
			try
			{
				var cmd = conn.CreateCommand();
				var tbl = BuildTable(table, data.GetColumnSchema());

				cmd.CommandText = tbl;
				cmd.ExecuteNonQuery();
			}
			catch (Exception e)
			{
				throw new InvalidOperationException($"Failed to create table {table}. Verify that it doesn't already exist.", e);
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
					cmd.ExecuteNonQuery();
				}

				tx.Commit();
			}
		}
	}
}