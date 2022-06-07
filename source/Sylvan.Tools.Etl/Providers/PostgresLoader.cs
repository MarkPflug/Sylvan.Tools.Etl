using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;

namespace Sylvan.Data.Etl
{
	class PostgresLoader : DataLoader
	{
		public override DbConnection GetConnection(string database)
		{
			string connStr;
			if (database.Contains("=")) // full connection string
			{
				connStr = database;
			}
			else
			{
				// otherwise assume just a database name on local server
				var csb = new NpgsqlConnectionStringBuilder
				{
					Host = "localhost",
					Database = database,
					IntegratedSecurity = true,
				};
				connStr = csb.ConnectionString;
			}

			var conn = new NpgsqlConnection(connStr);
			conn.Open();
			return conn;
		}
		static IdentifierStyle DbStyle = IdentifierStyle.Database;

		string BuildTable(string name, IEnumerable<DbColumn> cols)
		{
			var w = new StringWriter();

			w.WriteLine("create table " + DbStyle.Convert(name) + " (");

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

				var colName = DbStyle.Convert(col.ColumnName);
				w.Write(colName);
				w.Write(' ');
				switch (Type.GetTypeCode(col.DataType))
				{
					case TypeCode.Boolean:
						w.Write("boolean");
						break;
					case TypeCode.Int32:
						w.Write("integer");
						break;
					case TypeCode.Int64:
						w.Write("bigint");
						break;
					case TypeCode.String:
						w.Write("varchar");
						w.Write("(");
						w.Write(Math.Min(col.ColumnSize ?? 32, 256));
						w.Write(")");
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
						throw new NotSupportedException();
				}

				w.Write(' ');
				w.Write(col.DataType != typeof(string) && col.AllowDBNull == false ? " not null" : "null");
			}
			w.WriteLine();
			w.WriteLine(");");
			return w.ToString();
		}

		public override void Load(DbConnection connection, string table, DbDataReader data)
		{
			var sqlConn = (NpgsqlConnection)connection;
			var tbl = BuildTable(table, data.GetColumnSchema());
			var cmd = sqlConn.CreateCommand();
			cmd.CommandText = tbl;
			try
			{
				cmd.ExecuteNonQuery();
			}
			catch (Exception e)
			{
				throw new InvalidOperationException($"Failed to create table {table}.", e);
			}
			WriteData(sqlConn, table, data);
		}

		static void WriteData(NpgsqlConnection conn, string table, DbDataReader data)
		{
			var schema = data.GetColumnSchema();

			var sw = new StringWriter();
			sw.Write("copy ");
			sw.Write(DbStyle.Convert(table));
			sw.Write("(");
			for (int i = 0; i < schema.Count; i++)
			{
				if (i > 0)
					sw.Write(", ");

				var colSchema = schema[i];
				sw.Write(DbStyle.Convert(colSchema.ColumnName));
			}

			sw.Write(")");
			sw.Write("from stdin (format binary)");

			var cmd = sw.ToString();
			var bi = conn.BeginBinaryImport(cmd);

			while (data.Read())
			{
				bi.StartRow();
				for (var i = 0; i < data.FieldCount; i++)
				{
					var type = schema[i].DataType;
					var dbType = GetType(type);

					switch (dbType)
					{
						case NpgsqlDbType.Char:
							var str = data.GetString(i);
							bi.Write(str, dbType);
							break;
						case NpgsqlDbType.Integer:
							bi.Write(data.GetInt32(i), dbType);
							break;
						case NpgsqlDbType.Boolean:
							bi.Write(data.GetBoolean(i), dbType);
							break;
						case NpgsqlDbType.Double:
							bi.Write(data.GetDouble(i), dbType);
							break;
						case NpgsqlDbType.Money:
						case NpgsqlDbType.Numeric:
							bi.Write(data.GetDecimal(i), dbType);
							break;
						case NpgsqlDbType.Timestamp:
							bi.Write(data.GetDateTime(i), dbType);
							break;
						default:
							throw new NotSupportedException();
					}
				}
			}
			var count = bi.Complete();
		}

		static NpgsqlDbType GetType(Type type)
		{
			if (type == typeof(string))
				return NpgsqlDbType.Char;

			if (type == typeof(int))
				return NpgsqlDbType.Integer;

			if (type == typeof(bool))
				return NpgsqlDbType.Boolean;

			if (type == typeof(double))
				return NpgsqlDbType.Double;

			if (type == typeof(decimal))
				return NpgsqlDbType.Numeric;

			if (type == typeof(DateTime))
				return NpgsqlDbType.Timestamp;

			throw new NotSupportedException();
		}
	}
}
