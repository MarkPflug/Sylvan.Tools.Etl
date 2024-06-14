using Microsoft.Data.SqlClient;
using Npgsql;
using Sylvan.Data.Etl;
using Sylvan.Data.Etl.Providers.Npgsql;
using Sylvan.Data.Etl.Providers.SqlServer;
using System;

namespace Sylvan.Tools.DbMigrate;

static class MigrateCommand
{
	internal static void Run()
	{
		//string sqlHost = args[0];
		//string dbName = args[1];
		//string pgHost = args[2];
		//if (pgHost == ".") pgHost = "localhost";
		var pgHost = "localhost";

		var pgUsername = Environment.GetEnvironmentVariable("postgres_username");
		var pgPassword = Environment.GetEnvironmentVariable("postgres_password");

		var pgcb = new NpgsqlConnectionStringBuilder { Host = pgHost };

		if (string.IsNullOrEmpty(pgUsername))
		{
			//pgcb.IntegratedSecurity = true;
		}
		else
		{
			pgcb.Username = pgUsername;
			pgcb.Password = pgPassword;
		}

		var csb = new SqlConnectionStringBuilder()
		{
			DataSource = ".",
			InitialCatalog = "chs",
			TrustServerCertificate = true,
			IntegratedSecurity = true,
		};

		pgcb.Database = "chs";

		var srcDb = new SqlServerProvider(csb.ConnectionString);

		var dstDb = new NpgsqlProvider(pgcb.ConnectionString);

		var proc = new MigrateProcess(srcDb, dstDb);
		proc.Execute();
	}
}