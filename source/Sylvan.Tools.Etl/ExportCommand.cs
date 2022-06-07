using Spectre.Console;
using Spectre.Console.Cli;
using Sylvan.Data.Csv;
using System;
using System.Data.Common;
using System.Diagnostics;
using System.IO;

namespace Sylvan.Data.Etl
{
	class ExportSettings : CommandSettings
	{
		[CommandArgument(0, "[Provider]")]
		public Provider Provider { get; set; }

		[CommandArgument(1, "[Database]")]
		public string Database { get; set; }

		[CommandArgument(2, "[Query]")]
		public string Query { get; set; }

		[CommandArgument(3, "[File]")]
		public string File { get; set; }

		[CommandOption("--schema <Schema>")]
		public string Schema { get; set; }
	}

	class ExportCommand : Command<ExportSettings>
	{
		DataLoader GetLoader(Provider provider)
		{
			switch (provider)
			{
				case Provider.SqlServer:
					return new SqlServerLoader();
				case Provider.Sqlite:
					return new SqliteLoader();
			}
			throw new NotSupportedException("Unknown provider '" + provider + "'");
		}

		public override ValidationResult Validate(CommandContext context, ExportSettings settings)
		{
			if (settings.Provider == 0)
			{
				return ValidationResult.Error("Provider required.");
			}
			if (settings.Database == null)
			{
				return ValidationResult.Error("Database required.");
			}
			if (settings.File == null)
			{
				return ValidationResult.Error("File required.");
			}
			return base.Validate(context, settings);
		}

		public override int Execute(
			CommandContext context,
			ExportSettings settings
		)
		{
			var database = settings.Database;
			var filename = settings.File;
			var loader = GetLoader(settings.Provider);

			using var conn = loader.GetConnection(database);
			var cmd = conn.CreateCommand();

			cmd.CommandText = settings.Query;

			var reader = cmd.ExecuteReader();
			var cs = reader.GetColumnSchema();
			var schema = new Schema(cs);
			var spec = schema.ToString().Replace(",", "," + Environment.NewLine);

			File.WriteAllText(filename + ".schema", spec);

			var sw = Stopwatch.StartNew();
			using var csvWriter = CsvDataWriter.Create(filename);
			csvWriter.Write(reader);
			sw.Stop();
			Console.WriteLine(sw.Elapsed.ToString());

			
			return 0;
		}
	}
}
