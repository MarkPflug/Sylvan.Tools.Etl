using Microsoft.Data.SqlClient;
using Npgsql;
using Sylvan.Data.Etl;
using Sylvan.Data.Etl.Providers.Npgsql;
using Sylvan.Data.Etl.Providers.SqlServer;
using System;

namespace Sylvan.Tools.Etl;

static class MigrateCommand
{
	internal static void Run()
	{
		var sqlConn = Environment.GetEnvironmentVariable("MSSqlConnStr");
		var pgConn = Environment.GetEnvironmentVariable("PostgreSqlConnStr");

		var csb = new SqlConnectionStringBuilder(sqlConn);
		var pgcb = new NpgsqlConnectionStringBuilder(pgConn);

		csb.InitialCatalog = "coi"; 
		pgcb.Database = csb.InitialCatalog;

		var srcDb = new SqlServerProvider(csb.ConnectionString);
		var dstDb = new NpgsqlProvider(pgcb.ConnectionString);

		var proc = new MigrateProcess(srcDb, dstDb);
		proc.Execute();
	}
}