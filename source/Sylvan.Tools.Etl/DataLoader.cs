using System.Data.Common;

namespace Sylvan.Data.Etl
{
	abstract class DataLoader
	{
		public abstract DbConnection GetConnection(string database);

		public abstract void Load(DbConnection connection, string table, DbDataReader data);
	}
}
