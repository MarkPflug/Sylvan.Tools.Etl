using Npgsql;
using Spectre.Console.Cli;
using Sylvan.Data.Etl;
using System;
using System.Diagnostics;
using System.IO;

namespace Sylvan.Tools.Etl
{
	class Program
	{
		static NpgsqlConnection GetConnection(string database)
		{
			string connStr;
			if (database.Contains("=")) // full connection string
			{
				connStr = database;
			}
			else
			{
				// otherwise assume just a database name on local server
				var csb = new NpgsqlConnectionStringBuilder
				{
					Host = "localhost",
					Database = database,
					IntegratedSecurity = true,
				};
				connStr = csb.ConnectionString;
			}

			var conn = new NpgsqlConnection(connStr);
			conn.Open();
			return conn;
		}

		static int Main(string[] args)
		{
			var result = 0;
			var sw = Stopwatch.StartNew();

			//var c = GetConnection("Test");

			//var w = c.BeginTextImport("copy \"1000000_sales_records\" (\"region\", \"country\", \"item_type\", \"sales_channel\", \"order_priority\", \"order_date\", \"order_id\", \"ship_date\", \"units_sold\", \"unit_price\", \"unit_cost\", \"total_revenue\", \"total_cost\", \"total_profit\") from stdin (format csv)");
			//var r = File.OpenText("excel/1000000_Sales_Records.csv");
			//string line;
			//r.ReadLine();
			//while ((line = r.ReadLine()) != null)
			//{
			//	w.WriteLine(line);
			//}
			//w.Dispose();
			//sw.Stop();
			//Console.WriteLine(sw.Elapsed.ToString());

			var app = new CommandApp();
			app.Configure(config =>
			{
				config.AddCommand<ImportCommand>("import");
				config.AddCommand<ExportCommand>("export");
				config.AddCommand<AnalyzeCommand>("analyze");
				config.AddCommand<SelectCommand>("select");
				config.Settings.PropagateExceptions = true;
			});
			result = app.Run(args);
			sw.Stop();
			Console.WriteLine(sw.Elapsed.ToString());
			return result;
		}
	}
}
