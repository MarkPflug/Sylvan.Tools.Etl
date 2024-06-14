using Spectre.Console;
using Spectre.Console.Cli;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace Sylvan.Data.Etl;

public enum Provider
{
	SqlServer = 1,
	Sqlite = 2,
	Postgres = 3,
}

sealed class ImportSettings : CommandSettings
{
	[CommandArgument(0, "[Provider]")]
	public Provider Provider { get; set; }

	[CommandArgument(1, "[Database]")]
	public string Database { get; set; }

	[CommandArgument(2, "[File]")]
	public string File { get; set; }

	[CommandOption("--schema <Schema>")]
	public string Schema { get; set; }

	[CommandOption("-t|--table <Table>")]
	public string Table { get; set; }

	[CommandOption("-s|--skip <SKIP>")]
	public int Skip { get; set; }

	[CommandOption("-q|--quote <quote>")]
	public string Quote { get; set; }

}

sealed class ImportCommand : Command<ImportSettings>
{
	public override ValidationResult Validate(CommandContext context, ImportSettings settings)
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
		ImportSettings settings
	)
	{
		Task RunImport(Action<double> progressCallback)
		{
			return Task.Run(() =>
			{
				var database = settings.Database;
				var filename = settings.File;
				var pf = DbProviderFactory.GetProviderFactory(settings.Provider);

				var tableSchema = settings.Schema ?? "dbo";
				var tableName = settings.Table ?? Path.GetFileNameWithoutExtension(filename);

				var provider = pf(database);
				var conn = provider.GetConnection();

				var schema = provider.GetSchema(tableName); // schema might be null

				var reader = DataReader.OpenReader(filename, prog: progressCallback, sg: schema);
				var colSchema = reader.GetColumnSchema();
				var ti = provider.GetOrCreateTable(tableName, colSchema);

				var mapping = MigrateProcess.MapTable(ti, Mapping.Identity);

				var v = new Validator();				

#pragma warning disable CS0618 // Type or member is obsolete
				reader = reader.ValidateSchema(v.Validate);
#pragma warning restore CS0618 // Type or member is obsolete
				provider.LoadData(mapping, reader);

				using var ow = Console.OpenStandardOutput();
				using var bw = new BufferedStream(ow, 0x1000);
				using var tw = new StreamWriter(bw);
				v.WriteErrors(reader, tw);
			});
		}



	Progress p = AnsiConsole.Progress();

	p.Columns(
			new ProgressColumn[] {
				new TaskDescriptionColumn(),
				new ProgressBarColumn(),
				new PercentageColumn(),
				new HybridTimeColumn(),
				new SpinnerColumn(),
			}
		);

		p.Start(ctx =>
			{
				var task = ctx.AddTask("Import", RunImport);
task.Wait();

			});

return 0;
	}

	class Validator
	{
		List<Error> validationErrors;

		public Validator()
		{
			this.validationErrors = new();
		}

		public void WriteErrors(DbDataReader r, TextWriter tw)
		{
			if (validationErrors.Count > 0)
			{
				tw.Write($"Validation Errors ({validationErrors.Count}):");
				foreach (var error in validationErrors)
				{
					var name = r.GetName(error.ColumnNumber);
					var type = r.GetFieldType(error.ColumnNumber);
					tw.WriteLine($"{error.RowNumber},{error.ColumnNumber} ({name}:{type.Name}) '{error.Value}'");
				}
			}
		}

		public class Error
		{

			public Error(int row, int col, string value)
			{
				this.RowNumber = row;
				this.ColumnNumber = col;
				this.Value = value;
			}
			public int RowNumber { get; }
			public int ColumnNumber { get; }
			public string Value { get; }
		}

		public bool Validate(DataValidationContext ctx)
		{
			foreach(var idx in ctx.GetErrors())
			{
				var error = new Error(ctx.RowNumber, idx, ctx.DataReader.GetString(idx));
				this.validationErrors.Add(error);
			}
			return false;
		}
	}
}
