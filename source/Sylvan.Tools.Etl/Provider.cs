using Sylvan.Data.Etl.Providers.Npgsql;
using Sylvan.Data.Etl.Providers.SqlServer;
using System;

namespace Sylvan.Data.Etl;

static class DbProviderFactory
{
	public static Func<string, DbProvider> GetProviderFactory(Provider p)
	{
		switch (p)
		{
			case Provider.SqlServer:
				return c => new SqlServerProvider(c);
			case Provider.Sqlite:
				return c => new SqliteProvider(c);
			case Provider.Postgres:
				return c => new NpgsqlProvider(c);
		}
		throw new NotSupportedException("Unknown provider '" + p + "'");
	}
}
