using Microsoft.Extensions.Logging;
using System.Data.Common;
using System.Diagnostics;

namespace Sylvan.Data.Etl;

sealed class NullLogger : ILogger
{
	public static ILogger Instance = new NullLogger();

	private NullLogger() { }

	public IDisposable BeginScope<TState>(TState state)
	{
		return null!;
	}

	public bool IsEnabled(LogLevel logLevel)
	{
		return false;
	}

	public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
	{
	}
}

public class MigrateProcess
{
	readonly DbProvider source;
	readonly DbProvider target;
	readonly IMapping mapping;
	readonly ILogger log;

	public MigrateProcess(DbProvider source, DbProvider target, ILogger log)
	{
		this.source = source;
		this.target = target;
		this.log = log;
		this.mapping = Mapping.Identity;
	}

	public void Execute()
	{
		var style = new UnderscoreStyle(CasingStyle.LowerCase);

		using var sConn = source.GetConnection();
		using var tConn = target.GetConnection();

		Export(sConn, tConn);
	}

	void TerminalError(string msg, Exception ex)
	{
		Console.Error.WriteLine(msg);
		Environment.Exit(ex.HResult);
	}

	IEnumerable<TableInfo> GetTableInfo(DbConnection conn)
	{
		using var cmd = conn.CreateCommand();
		cmd.CommandText = "select table_schema, table_name from information_schema.tables where table_type = 'BASE TABLE' order by table_name";
		using var reader = cmd.ExecuteReader();
		while (reader.Read())
		{
			yield return new TableInfo(reader.GetString(0), reader.GetString(1));
		}
	}

	void Export(DbConnection conn, DbConnection dst)
	{
		var tables = GetTableInfo(conn).ToArray();
		var total = Stopwatch.StartNew();
		foreach (var table in tables)
		{
			Console.Write($"{table.TableName,-75}");
			var destTable = this.mapping.MapTable(table);
			if (destTable == null)
			{
				Console.WriteLine($" SKIPPED");
				continue;
			}
			var sw = Stopwatch.StartNew();
			using var cmd = conn.CreateCommand();
			var sel = GenerateSelect(conn, table);
			cmd.CommandTimeout = 0;
			cmd.CommandText = sel;
			using var reader = cmd.ExecuteReader();
			var str = BuildTable(table, reader.GetColumnSchema());
			using var dstCmd = dst.CreateCommand();
			dstCmd.CommandText = $"create schema if not exists \"{table.TableSchema}\"";
			dstCmd.ExecuteNonQuery();
			dstCmd.CommandText = str;
			dstCmd.ExecuteNonQuery();
			var count = WriteData(reader, target, table);

			Console.WriteLine($" {sw.Elapsed} {count}");
		}
		//Console.WriteLine("Creating primary keys");
		//{
		//	var w = new StringWriter();
		//	GeneratePKs(conn, w);
		//	var pks = w.ToString();
		//	var cmd = dst.CreateCommand();
		//	cmd.CommandText = pks;
		//	cmd.ExecuteNonQuery();
		//}

		//Console.WriteLine("Creating foreign keys");
		//{
		//	var w = new StringWriter();
		//	GenerateFKs(conn, w);
		//	var fks = w.ToString();
		//	var cmd = dst.CreateCommand();
		//	cmd.CommandText = fks;
		//	cmd.ExecuteNonQuery();
		//}
		
		Console.WriteLine($"{"TOTAL",-75} {total.Elapsed}");

		Console.WriteLine("Done");
	}

	string GenerateSelect(DbConnection conn, TableInfo table)
	{
		using var cmd = conn.CreateCommand();
		cmd.CommandText = $"select top 0 * from [{table.TableSchema}].[" + table.TableName + "]";
		using var reader = cmd.ExecuteReader();
		var cols = reader.GetColumnSchema();
		var sw = new StringWriter();
		sw.Write("select ");
		int i = 0;
		foreach (var col in cols)
		{
			var key = table.TableName + "." + col.ColumnName;
			var destCol = this.mapping.MapColumn(table, col);
			if (destCol == null)
			{
				continue;
			}

			if (i++ != 0)
			{
				sw.Write(',');
			}
			sw.Write('[');
			sw.Write(col.ColumnName);
			sw.Write(']');
		}
		sw.Write($" from [{table.TableSchema}].[" + table.TableName + "]");
		return sw.ToString();
	}

	static string BuildTable(TableInfo table, IEnumerable<DbColumn> cols)
	{
		var w = new StringWriter();

		w.WriteLine($"create table \"{table.TableSchema}\".\"" + table.TableName + "\" (");

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

			var colName = col.ColumnName;
			w.Write("\"");
			w.Write(colName);
			w.Write("\" ");
			var dataType = col.DataType;
			switch (Type.GetTypeCode(dataType))
			{
				case TypeCode.Boolean:
					w.Write("boolean");
					break;
				case TypeCode.Byte:
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
					if (dataType == typeof(byte[]))
					{
						w.Write("bytea");
						break;
					}
					if (dataType == typeof(Guid))
					{
						w.Write("UUID");
						break;
					}
					if (dataType == typeof(DateTimeOffset))
					{
						w.Write("timestamptz");
						break;
					}
					throw new NotSupportedException();
			}

			w.Write(' ');
			w.Write(col.DataType != typeof(string) && col.AllowDBNull == false ? " not null" : "null");
		}
		w.WriteLine();
		w.WriteLine(");");
		return w.ToString();
	}

	static long WriteData(DbDataReader data, DbProvider provider, TableInfo table)
	{
		return provider.LoadData(table.TableName, data);
	}

	static void GenerateFKs(DbConnection conn, TextWriter w)
	{
		using var cmd = conn.CreateCommand();
		cmd.CommandText = Sql.ForeignKeyInfo;
		using var reader = cmd.ExecuteReader();

		TableReference? r = null;

		while (reader.Read())
		{
			var fkSchema = reader.GetString(0);
			var fkName = reader.GetString(1);
			var srcTblSchema = reader.GetString(2);
			var srcTblName = reader.GetString(3);
			var srcColName = reader.GetString(4);
			var refTblSchema = reader.GetString(8);
			var refTblName = reader.GetString(9);
			var refColName = reader.GetString(10);

			if (r?.ReferenceName.Name != fkName)
			{

				r?.WriteConstraint(w);
				r = new TableReference(
					new QualifiedName(fkSchema, fkName),
					new QualifiedName(srcTblSchema, srcTblName),
					new QualifiedName(refTblSchema, refTblName)
				);
			}

			r!.AddColumnMapping(srcColName, refColName);
		}

		r?.WriteConstraint(w);
	}

	static void GeneratePKs(DbConnection conn, TextWriter w)
	{
		using var cmd = conn.CreateCommand();
		cmd.CommandText = Sql.PrimaryKeyInfo;
		using var reader = cmd.ExecuteReader();

		var columns = new List<string>();
		string? curPk = null;
		string? tableSchema = null;
		string? tableName = null;

		while (reader.Read())
		{
			var pkName = reader.GetString(0);

			if (curPk != null && curPk != pkName)
			{
				WritePK(w, curPk, tableSchema!, tableName!, columns);
				columns.Clear();
			}

			tableSchema = reader.GetString(1);
			tableName = reader.GetString(2);
			var columnName = reader.GetString(3);
			var ordinal = reader.GetInt32(4);

			curPk = pkName;
			columns.Add(columnName);
		}

		if (curPk != null)
		{
			WritePK(w, curPk, tableSchema!, tableName!, columns);
		}

		void WritePK(TextWriter writer, string pkName, string schema, string table, List<string> columns)
		{
			writer.Write("alter table \"" + schema + "\".\"" + table + "\"");
			writer.Write($" add constraint \"{pkName}\" primary key (");
			for (int i = 0; i < columns.Count; i++)
			{
				if (i > 0)
					writer.Write(",");
				writer.Write("\"");
				writer.Write(columns[i]);
				writer.Write("\"");
			}
			writer.WriteLine(");");
		}
	}

	class TableReference
	{
		public QualifiedName ReferenceName { get; }
		public QualifiedName SourceTable { get; }
		public QualifiedName TargetTable { get; }
		List<KeyValuePair<string, string>> cols;

		public TableReference(QualifiedName fkName, QualifiedName srcTable, QualifiedName tgtTable)
		{
			this.ReferenceName = fkName;
			this.SourceTable = srcTable;
			this.TargetTable = tgtTable;
			this.cols = new List<KeyValuePair<string, string>>();
		}

		public void AddColumnMapping(string source, string target)
		{
			this.cols.Add(new KeyValuePair<string, string>(source, target));
		}

		public void WriteConstraint(TextWriter w)

		{
			w.Write("alter table ");
			SourceTable.Write(w);
			w.Write(" add constraint ");
			w.Write(ReferenceName.Name);
			w.Write(" foreign key (");
			for (int i = 0; i < cols.Count; i++)
			{
				if (i != 0)
					w.Write(", ");
				w.Write('\"');
				w.Write(cols[i].Key);
				w.Write('\"');
			}
			w.Write(") references ");

			TargetTable.Write(w);

			w.Write(" (");
			for (int i = 0; i < cols.Count; i++)
			{
				if (i != 0)
					w.Write(", ");
				w.Write('\"');
				w.Write(cols[i].Value);
				w.Write('\"');
			}
			w.WriteLine(");");
		}
	}

	class QualifiedName : IEquatable<QualifiedName>
	{
		public static readonly QualifiedName Null = new QualifiedName(string.Empty, string.Empty);

		public QualifiedName(string schema, string name)
		{
			this.Schema = schema;
			this.Name = name;
		}

		public string Schema { get; }
		public string Name { get; }

		public override bool Equals(object? obj)
		{
			return base.Equals(obj);
		}

		public bool Equals(QualifiedName? name)
		{
			if (name == null) return false;

			var c = StringComparer.OrdinalIgnoreCase;
			return
				c.Equals(this.Schema, name.Schema) &&
				c.Equals(this.Name, name.Name);
		}

		public override int GetHashCode()
		{
			var c = StringComparer.OrdinalIgnoreCase;
			return
				HashCode.Combine(
					c.GetHashCode(this.Schema),
					c.GetHashCode(this.Name)
				);
		}

		public void Write(TextWriter w)
		{
			w.Write('\"');
			w.Write(this.Schema);
			w.Write('\"');
			w.Write('.');
			w.Write('\"');
			w.Write(this.Name);
			w.Write('\"');
		}
	}
}
