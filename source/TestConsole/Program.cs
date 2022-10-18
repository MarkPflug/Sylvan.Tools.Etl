using Npgsql;
using Sylvan.Data.Etl;
using Sylvan.Data.Etl.Providers.Npgsql;
using Sylvan.Data.Etl.Providers.SqlServer;
using System.Data.Common;
using System.Data.SqlClient;
using TestConsole;

const string Db = "sc3";

var log = new ConsoleLogger();

var scsb = new SqlConnectionStringBuilder
{
	DataSource = ".",
	InitialCatalog = Db,
	IntegratedSecurity = true,
};

var src = new SqlServerProvider(scsb.ConnectionString);

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