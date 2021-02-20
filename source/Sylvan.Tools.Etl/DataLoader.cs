using System.Data.Common;

namespace Sylvan.Data.Etl
{
	abstract class DataLoader
	{
		public abstract void Load(IDbColumnSchemaGenerator schema, DbDataReader data, string table, string database);
	}
}
