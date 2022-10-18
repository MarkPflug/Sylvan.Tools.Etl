//using Npgsql;

//var csb = new NpgsqlConnectionStringBuilder
//{
//	 Host = "localhost",
//	 IntegratedSecurity	= true,
//};
//var db = "test" + Guid.NewGuid().ToString("n");
//{
//	using var conn = new NpgsqlConnection(csb.ConnectionString);
//	conn.Open();
//	using var cmd = conn.CreateCommand();
//	cmd.CommandText = "create database " + db;

//	cmd.ExecuteNonQuery();
//}

//{
//	csb.Database = db;
//	using var conn = new NpgsqlConnection(csb.ConnectionString);
//	conn.Open();
//	using var cmd = conn.CreateCommand();
//	cmd.CommandText = "create table A (id int);";
//	cmd.ExecuteNonQuery();

//	using var import = conn.BeginBinaryImport(" copy a (id) from stdin (format binary);");
//	for (int i = 0; i < 100; i++)
//	{
//		import.StartRow();
//		import.Write(i);
//	}
//	import.Complete();
//}




