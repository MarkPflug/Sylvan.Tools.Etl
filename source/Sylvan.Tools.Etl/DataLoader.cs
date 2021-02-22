using System.Data.Common;

namespace Sylvan.Data.Etl
{
	abstract class DataLoader
	{
		public abstract void Load(DbDataReader data, string table, string database);
	}
}
