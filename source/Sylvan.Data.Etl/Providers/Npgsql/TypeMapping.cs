using System;
using System.Data;
using NpgsqlTypes;

namespace Sylvan.Data.Etl;

static class TypeMapping
{
    public static DbType NpgsqlDbTypeToDbType(NpgsqlDbType npgsqlDbType)
        => npgsqlDbType switch
        {
            // Numeric types
            NpgsqlDbType.Smallint => DbType.Int16,
            NpgsqlDbType.Integer => DbType.Int32,
            NpgsqlDbType.Bigint => DbType.Int64,
            NpgsqlDbType.Real => DbType.Single,
            NpgsqlDbType.Double => DbType.Double,
            NpgsqlDbType.Numeric => DbType.Decimal,
            NpgsqlDbType.Money => DbType.Currency,

            // Text types
            NpgsqlDbType.Text => DbType.String,
            NpgsqlDbType.Xml => DbType.Xml,
            NpgsqlDbType.Varchar => DbType.String,
            NpgsqlDbType.Char => DbType.String,
            NpgsqlDbType.Name => DbType.String,
            NpgsqlDbType.Refcursor => DbType.String,
            NpgsqlDbType.Citext => DbType.String,
            NpgsqlDbType.Jsonb => DbType.Object,
            NpgsqlDbType.Json => DbType.Object,
            NpgsqlDbType.JsonPath => DbType.String,

            // Date/time types
            NpgsqlDbType.Timestamp => DbType.DateTime,
            NpgsqlDbType.TimestampTz => DbType.DateTimeOffset,
            NpgsqlDbType.Date => DbType.Date,
            NpgsqlDbType.Time => DbType.Time,

            // Misc data types
            NpgsqlDbType.Bytea => DbType.Binary,
            NpgsqlDbType.Boolean => DbType.Boolean,
            NpgsqlDbType.Uuid => DbType.Guid,

            NpgsqlDbType.Unknown => DbType.Object,

            _ => DbType.Object
        };

    public static NpgsqlDbType GetNpgsqlType(Type type)
    {
        if (type == typeof(string))
            return NpgsqlDbType.Char;

        if (type == typeof(byte))
            return NpgsqlDbType.Smallint;

        if (type == typeof(short))
            return NpgsqlDbType.Smallint;

        if (type == typeof(int))
            return NpgsqlDbType.Integer;

        if (type == typeof(long))
            return NpgsqlDbType.Bigint;

        if (type == typeof(bool))
            return NpgsqlDbType.Boolean;

        if (type == typeof(double))
            return NpgsqlDbType.Double;

        if (type == typeof(float))
            return NpgsqlDbType.Real;

        if (type == typeof(decimal))
            return NpgsqlDbType.Numeric;

        if (type == typeof(DateTime))
            return NpgsqlDbType.Timestamp;

        if (type == typeof(DateTimeOffset))
            return NpgsqlDbType.TimestampTz;

        if (type == typeof(byte[]))
            return NpgsqlDbType.Bytea;
        
        if (type == typeof(Guid))
            return NpgsqlDbType.Uuid;

        throw new NotSupportedException();
    }

    public static DbType GetDbType(Type type)
    {
        if (type == typeof(string))
            return DbType.String;

        if (type == typeof(byte))
            return DbType.Byte;

        if (type == typeof(short))
            return DbType.Int16;

        if (type == typeof(int))
            return DbType.Int32;

        if (type == typeof(long))
            return DbType.Int64;

        if (type == typeof(bool))
            return DbType.Boolean;

        if (type == typeof(double))
            return DbType.Double;

        if (type == typeof(float))
            return DbType.Single;

        if (type == typeof(decimal))
            return DbType.Decimal;

        if (type == typeof(DateTime))
            return DbType.DateTime;

        if (type == typeof(DateTimeOffset))
            return DbType.DateTimeOffset;

        if (type == typeof(byte[]))
            return DbType.Binary;

        if (type == typeof(Guid))
            return DbType.Guid;

        throw new NotSupportedException();
    }
}
