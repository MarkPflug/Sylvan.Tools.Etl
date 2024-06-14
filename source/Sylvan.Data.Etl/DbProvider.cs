using Sylvan.CodeGeneration;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data.Etl;

public abstract class DbProvider
{
	public virtual IEnumerable<string> IgnoreSchemas
	{
		get
		{
			yield break;
		}
	}

	public static readonly IdentifierStyle DefaultStyle = IdentifierStyle.Database;

	public virtual IdentifierStyle NameStyle => DefaultStyle;

	public abstract DbType? GetType(string typeName);

	public abstract DbConnection GetConnection();

	public void CreateDatabase(string name, bool deleteIfExists = false)
	{
		using var conn = GetConnection();

		using var cmd = conn.CreateCommand();
		if (deleteIfExists)
		{
			cmd.CommandText = "drop database if exists " + name;
			cmd.ExecuteNonQuery();
		}

		cmd.CommandText = "create database " + name;
		cmd.ExecuteNonQuery();
	}

	public abstract long LoadData(TableMapping tableName, DbDataReader data);


	public bool TableExists(string tableName)
	{
		using var conn = GetConnection();

		using var cmd = conn.CreateCommand();
		cmd.CommandText = $"SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{tableName}'";
		var c = Convert.ToInt32(cmd.ExecuteScalar());
		return c > 0;
	}

	public virtual IEnumerable<DbColumn>? GetSchema(string tableName)
	{
		if (TableExists(tableName))
		{
			using (var conn = GetConnection())
			{
				using var cmd = conn.CreateCommand();
				cmd.CommandText = $"select top 0 * from [{tableName}]";
				cmd.CommandType = CommandType.Text;
				using var reader = cmd.ExecuteReader();
				return reader.GetColumnSchema();
			}
		}
		return null;
	}

	public virtual TableInfo GetOrCreateTable(string tableName, IEnumerable<DbColumn> cols)
	{
		using var conn = GetConnection();
		using var cmd = conn.CreateCommand();

		if (TableExists(tableName))
		{
			throw new NotImplementedException();
		}
		else
		{
			return CreateTable(tableName, cols);
		}
	}

	public virtual TableInfo CreateTable(string tableName, IEnumerable<DbColumn> cols)
	{
		using var conn = GetConnection();
		using var cmd = conn.CreateCommand();
		var buildTableCmd = BuildTable(tableName, cols);
		cmd.CommandText = buildTableCmd;
		cmd.ExecuteNonQuery();
		return GetTableInfo(tableName);
	}

	protected virtual string BuildTable(string name, IEnumerable<DbColumn> cols)
	{
		throw new NotImplementedException();
	}

	public virtual TableInfo GetTableInfo(string name)
	{
		using var conn = GetConnection();
		using var cmd = conn.CreateCommand();

		TableInfo? table = null;

		cmd.CommandText = string.Format(Sql.TableColumn, "dbo", name);
		using var reader = cmd.ExecuteReader();

		while (reader.Read())
		{
			var schema = reader.GetString(0);
			var tableName = reader.GetString(1);

			if (table == null)
			{
				table = new TableInfo(schema, tableName);
			}

			var columnName = reader.GetString(2);
			var allowNull = StringComparer.OrdinalIgnoreCase.Equals(reader.GetString(4), "YES");
			var typeName = reader.GetString(5);
			var type = GetType(typeName);
			if (type != null)
			{
				int? textLength = reader.IsDBNull(6) ? null : reader.GetInt32(6);
				int? textOctetLength = reader.IsDBNull(7) ? null : reader.GetInt32(7);
				int? precision = reader.IsDBNull(8) ? null : reader.GetByte(8);
				int? scale = reader.IsDBNull(10) ? null : reader.GetInt32(10);
				var col = new ColumnInfo(columnName, typeName, type.Value, allowNull, textLength, precision, scale);

				table.Columns.Add(col);
			}
		}
		if (table == null) throw new Exception();
		return table;
	}

	public virtual IEnumerable<TableInfo> GetTableInfos()
	{
		using var conn = GetConnection();
		using var cmd = conn.CreateCommand();

		TableInfo? table = null;

		var tables = new List<TableInfo>();

		var ignoreSchemas = new HashSet<string>(this.IgnoreSchemas);

		{
			cmd.CommandText = Sql.TableColumn;
			using var reader = cmd.ExecuteReader();

			while (reader.Read())
			{
				var schema = reader.GetString(0);
				if (ignoreSchemas.Contains(schema))
					continue;
				var tableName = reader.GetString(1);

				if (table == null || table.TableSchema != schema || table.TableName != tableName)
				{
					if (table != null) tables.Add(table);
					table = new TableInfo(schema, tableName);
				}

				var columnName = reader.GetString(2);
				var allowNull = StringComparer.OrdinalIgnoreCase.Equals(reader.GetString(4), "YES");
				var typeName = reader.GetString(5);
				var type = GetType(typeName);
				if (type != null)
				{
					int? textLength = reader.IsDBNull(6) ? null : reader.GetInt32(6);
					int? textOctetLength = reader.IsDBNull(7) ? null : reader.GetInt32(7);
					int? precision = reader.IsDBNull(8) ? null : reader.GetByte(8);
					int? scale = reader.IsDBNull(10) ? null : reader.GetInt32(10);
					var col = new ColumnInfo(columnName, typeName, type.Value, allowNull, textLength, precision, scale);

					table.Columns.Add(col);
				}
			}

			if (table != null)
				tables.Add(table);
		}

		return tables;
	}
}

class DbProviderType
{
	public DbType DbType { get; }

	public bool HasLength { get; }

	public bool IsFixedLength { get; }

	public virtual string ProviderTypeName => DbType.ToString();

	public DbProviderType(DbType dbType, bool hasLength = false, bool isFixedLength = false)
	{
		this.DbType = dbType;
		this.HasLength = hasLength;
		this.IsFixedLength = isFixedLength;
	}
}

abstract class DbProviderType<T> : DbProviderType
{
	internal DbProviderType(T providerType, DbType type) : base(type)
	{
		this.ProviderType = providerType;
	}

	public T ProviderType { get; }

	public override string ProviderTypeName => this.ProviderType!.ToString()!;
}
