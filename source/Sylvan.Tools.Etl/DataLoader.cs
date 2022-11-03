//using System.Collections.Generic;
//using System.Data.Common;

//namespace Sylvan.Data.Etl;

//abstract class DataLoader
//{
//	public abstract DbConnection GetConnection(string database);

//	public abstract void Load(DbConnection connection, string table, DbDataReader data);

//	public IEnumerable<DbColumn> GetSchema(DbConnection conn, string table)
//	{
//		try
//		{
//			var cmd = conn.CreateCommand();
//			cmd.CommandText = table;
//			cmd.CommandType = System.Data.CommandType.TableDirect;
//			var reader = cmd.ExecuteReader();
//			return reader.GetColumnSchema();
//		}
//		catch {
//			return null;
//		}
//	}
//}
