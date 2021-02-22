using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;

namespace Sylvan.Data.Etl
{
	class SqlServerLoader : DataLoader
	{
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

				w.Write(col.ColumnName);
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

		public override void Load(DbDataReader data, string table, string database)
		{
			string connStr;
			if (database.Contains("=")) // full connection string
			{
				connStr = database;
			}
			else
			{
				// otherwise assume just a database name on local server
				var csb = new SqlConnectionStringBuilder() { DataSource = ".", InitialCatalog = database, IntegratedSecurity = true };
				connStr = csb.ConnectionString;
			}

			using (var conn = new SqlConnection(connStr))
			{
				conn.Open();

				var tbl = BuildTable(table, data.GetColumnSchema());
				var cmd = conn.CreateCommand();
				cmd.CommandText = tbl;
				try
				{
					cmd.ExecuteNonQuery();
				}
				catch (Exception e)
				{
					throw new InvalidOperationException($"Failed to create table {table}.", e);
				}

				using var tx = conn.BeginTransaction();
				var bc = new SqlBulkCopy(conn, 0, tx);
				bc.BulkCopyTimeout = 0;
				bc.EnableStreaming = true;
				bc.DestinationTableName = table;
				bc.BatchSize = 10000;
				bc.WriteToServer(data);
				tx.Commit();
			}
		}
	}
}