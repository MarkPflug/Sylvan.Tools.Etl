using Microsoft.Extensions.Logging;
using Npgsql;
using Sylvan.CodeGeneration;
using Sylvan.Data;
using Sylvan.Data.Etl;
using Sylvan.Data.Etl.Providers.Npgsql;
using Sylvan.Data.Etl.Providers.SqlServer;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using TestConsole;

static class Program
{
	const string Db = "t";

	static SqlConnectionStringBuilder SqlConnStr => new()
	{
		DataSource = ".",
		InitialCatalog = Db,
		IntegratedSecurity = true,
		TrustServerCertificate = true,
	};

	static NpgsqlConnectionStringBuilder NpgsqlConnStr => new()
	{
		Host = "localhost",
		Database = Db,
		//IntegratedSecurity = true,
	};

	public class Contact
	{
		public int Id { get; set; }
		public string? FirstName { get; set; }
		public string? LastName { get; set; }
		public int Age { get; set; }
	}

	public static IEnumerable<Contact> GetContacts(int count)
	{
		for (int i = 0; i < count; i++)
		{
			yield return new Contact { Id = i, FirstName = "Frist", LastName = "Lsat", Age = i % 100 };
		}
	}
	const int Count = 1000000;

	static void Main()
	{
		var nps = new NpgsqlProvider(NpgsqlConnStr.ConnectionString);
		var msq = new SqlServerProvider(SqlConnStr.ConnectionString);


		var sw = Stopwatch.StartNew();
		LoadData(nps, "public", "Contact", GetContacts(Count));
		sw.Stop();
		var a = sw.Elapsed;
		Console.WriteLine($"{sw.Elapsed}");

		sw = Stopwatch.StartNew();
		LoadData(msq, "dbo", "Contact", GetContacts(Count));
		sw.Stop();
		var b = sw.Elapsed;

		Console.WriteLine($"{sw.Elapsed}");
		Console.WriteLine($"{(double)b.Ticks / a.Ticks}");
	}


	static void LoadData<T>(DbProvider p, string s, string table, IEnumerable<T> data)
		where T : class
	{
		var conn = p.GetConnection();
		var schema = p.GetSchema(table);
		var reader = data.AsDataReader();

		var ti = new TableInfo(s, table);
		ti.Columns.Add(new Sylvan.Data.Etl.ColumnInfo("id", "int", DbType.Int32, false));
		ti.Columns.Add(new Sylvan.Data.Etl.ColumnInfo("firstname", "varchar", DbType.AnsiString, false));
		ti.Columns.Add(new Sylvan.Data.Etl.ColumnInfo("lastname", "varchar", DbType.AnsiString, false));
		ti.Columns.Add(new Sylvan.Data.Etl.ColumnInfo("age", "int", DbType.Int32, false));
		throw new NotImplementedException();
		//var mapping = MigrateProcess.MapTable(ti, Mapping.Identity);

		//p.LoadData(mapping, reader);
	}

	static void LogTest()
	{

		var c = new ConsoleLogger();

		c.LogCritical("CRITICAL");
		c.LogError("ERROR");
		c.LogWarning("WARNING");
		c.LogInformation("INFO");
		c.LogDebug("DEBUG");
		//TestMigrate();
		//var prov = (SqlServerProvider)GetProvider();
		//var m = prov.BuildMergeCommand("#load", "test", MergeAction.All);
	}

	static DbProvider GetProvider()
	{
		return new SqlServerProvider(SqlConnStr.ConnectionString);
	}

	static void Dev()
	{
		var provider = GetProvider();
		var tables = provider.GetTableInfos();

		var mapping = new NameStyleMapping(IdentifierStyle.Database);

		//foreach (var t in tables)
		//{
		//	var map = MapTable(t, mapping);
		//	var st = map.SourceTable;
		//	var tt = map.TargetTable;
		//	Console.WriteLine($"{st.TableSchema}.{st.TableName} => {tt?.TableSchema}.{tt?.TableName ?? "NON"}");
		//	foreach(var cm in map.ColumnMappings)
		//	{
		//		Console.WriteLine($"\t{cm.SourceColumn.Name} => {cm.TargetColumn?.Name ?? "NON"}");
		//	}
		//}
	}

	static void TestMigrate()
	{
		var log = new ConsoleLogger();

		var src = new SqlServerProvider(SqlConnStr.ConnectionString);

		var dcsb = new NpgsqlConnectionStringBuilder
		{
			Host = "localhost",
			//IntegratedSecurity = true,
		};

		var dstConn = new NpgsqlConnection(dcsb.ConnectionString);
		dstConn.Open();

		RecreateDb(dstConn, Db);

		dcsb.Database = Db;

		var dst = new NpgsqlProvider(dcsb.ConnectionString);

		var proc = new MigrateProcess(src, dst, log);

		proc.Execute();
	}

	static void RecreateDb(DbConnection conn, string db)
	{
		var cmd = conn.CreateCommand();
		cmd.CommandText = "drop database " + db;
		try
		{
			cmd.ExecuteNonQuery();
		}
		catch { }
		cmd.CommandText = "create database " + db;
		cmd.ExecuteNonQuery();
	}
}
