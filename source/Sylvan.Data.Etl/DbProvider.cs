using System.Data;
using System.Data.Common;

namespace Sylvan.Data.Etl;

public abstract class DbProvider
{
	public static readonly IdentifierStyle DefaultStyle = new UnderscoreStyle(CasingStyle.Unchanged);

	public virtual IdentifierStyle NameStyle => DefaultStyle;

	public abstract DbType GetType(string typeName);

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

	public abstract long LoadData(string tableName, DbDataReader data);

	public virtual IEnumerable<DbColumn> GetSchema(DbConnection conn, string tableName)
	{
		using var cmd = conn.CreateCommand();
		cmd.CommandText = tableName;
		cmd.CommandType = CommandType.TableDirect;
		using var reader = cmd.ExecuteReader();
		return reader.GetColumnSchema();
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
