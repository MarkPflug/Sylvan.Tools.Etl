using Npgsql;
using System;

namespace Sylvan.Tools.DbMigrate;

static class MigrateCommand
{
	static void Main(string[] args)
	{
		string sqlHost = args[0];
		string dbName = args[1];
		string pgHost = args[2];
		if (pgHost == ".") pgHost = "localhost";

		var pgUsername = Environment.GetEnvironmentVariable("postgres_username");
		var pgPassword = Environment.GetEnvironmentVariable("postgres_password");

		var pgcb = new NpgsqlConnectionStringBuilder { Host = pgHost };

		if (string.IsNullOrEmpty(pgUsername))
		{
			pgcb.IntegratedSecurity = true;
		}
		else
		{
			pgcb.Username = pgUsername;
			pgcb.Password = pgPassword;
		}


		var proc = new MigrateProcess();
	}
}
