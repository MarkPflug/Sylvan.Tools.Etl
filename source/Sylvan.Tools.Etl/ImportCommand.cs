using Spectre.Console;
using Spectre.Console.Cli;
using Sylvan.Data.Etl.Providers.Npgsql;
using Sylvan.Data.Etl.Providers.SqlServer;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Sylvan.Data.Etl
{
	public enum Provider
	{
		SqlServer = 1,
		Sqlite = 2,
		Postgres = 3,
	}

	class ImportSettings : CommandSettings
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

	class ImportCommand : Command<ImportSettings>
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
					var tableName = settings.Table ?? Path.GetFileNameWithoutExtension(filename);
					var provider = pf(database);
					var conn = provider.GetConnection();
					var schema = provider.GetSchema(tableName);
					var reader = DataReader.OpenReader(filename, prog: progressCallback, sg: schema);

					throw new NotImplementedException();
					//provider.LoadData(tableName, reader);
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
	}

	static class SpectreExtensions
	{
		public static Task AddTask(this ProgressContext ctx, string description, Func<Action<double>, Task> taskFactory, double maxValue = 1d)
		{
			var task = ctx.AddTask(description, new ProgressTaskSettings { MaxValue = maxValue });
			
			double last = 0d;

			Action<double> setProgress =
				progress =>
				{
					var delta = progress - last;
					task.Increment(delta);
					last = progress;
				};

			var tt = taskFactory(setProgress)
				// ensures that the task reaches "completed".
				.ContinueWith(
					t =>
					{
						if (t.IsCompletedSuccessfully)
						{
							task.Increment(maxValue);
						}
						else
						{
							throw t.Exception;
						}
					}
				);

			return tt;
		}
	}
}
