using Npgsql;
using Sylvan;
using Sylvan.Data.Etl;
using Sylvan.Data.Etl.Providers.Npgsql;
using Sylvan.Data.Etl.Providers.SqlServer;
using System.Data.Common;
using System.Data.SqlClient;
using TestConsole;

static class Program
{
	const string Db = "sc3";

	static SqlConnectionStringBuilder SqlConnStr => new SqlConnectionStringBuilder
	{
		DataSource = ".",
		InitialCatalog = Db,
		IntegratedSecurity = true,
	};

	static void Main()
	{
		TestMigrate();
	}

	static DbProvider GetProvider()
	{
		return new SqlServerProvider(SqlConnStr.ConnectionString);
	}

	static void Dev()
	{
		var provider = GetProvider();
		var tables = provider.GetTableInfo();

		var mapping = new NameStyleMapping(new UnderscoreStyle(CasingStyle.LowerCase));


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
			IntegratedSecurity = true,
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
