using Spectre.Console;
using Spectre.Console.Cli;
using Sylvan.Data.Csv;
using Sylvan.IO;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Etl
{
	public enum Provider
	{
		SqlServer = 1,
		Sqlite = 2,
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
	}

	class ImportCommand : Command<ImportSettings>
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
			double last = 0.0;
			double prog = 0.0;

			ProgressTask progress = null;
			var mre = new ManualResetEvent(false);

			void UpdateProgress(double p)
			{
				prog = p * 100d;
				mre.Set();
			}

			var task = Task.Run(() =>
			{
				CsvDataReader csv = null;

				var database = settings.Database;
				var filename = settings.File;
				var loader = GetLoader(settings.Provider);

				var tableName = settings.Table ?? Path.GetFileNameWithoutExtension(filename);

				Stream s = File.OpenRead(settings.File);
				s = s.WithReadProgress(UpdateProgress, 0.001);
				var tr = new StreamReader(s);
				for (int i = 0; i < settings.Skip; i++)
				{
					tr.ReadLine();
				}

				string schemaSpec = null;

				if (settings.Schema != null)
				{
					var schemaFile = settings.Schema;
					schemaSpec = File.ReadAllText(schemaFile);					
				}  
				else
				{
					var schemaFile = filename + ".schema";
					if (File.Exists(schemaFile))
					{
						schemaSpec = File.ReadAllText(schemaFile);
					}				
				}

				var explicitSchema = schemaSpec == null ? null : Schema.Parse(schemaSpec);

				var schema = explicitSchema == null ? CsvSchema.Nullable : new CsvSchema(explicitSchema);

				var opts =
					new CsvDataReaderOptions
					{
						BufferSize = 0x100000,
						Schema = schema,
					};

				csv = CsvDataReader.Create(tr, opts);

				loader.Load(csv, tableName, database);

				mre.Set();
			});
			// ensures that the progress loop finishes.
			task.ContinueWith(t => mre.Set());

			AnsiConsole.Progress()
				.Columns(new ProgressColumn[] {
					new TaskDescriptionColumn(),    // Task description
					new ProgressBarColumn(),        // Progress bar
					new PercentageColumn(),         // Percentage
					new RemainingTimeColumn(),      // Remaining time
					new SpinnerColumn(),
					}
				)
				.Start(ctx =>
				{
					progress = ctx.AddTask("Import");
					while (!task.IsCompleted)
					{
						mre.WaitOne();
						var inc = prog - last;
						last = prog;
						progress.Increment(inc);
						mre.Reset();
					}
					if (task.IsFaulted)
					{
						throw task.Exception;
					}
					else
					{
						// make sure it arrives at 100%
						if (last < 100d)
						{
							progress.Increment(100d - last);
						}
					}
				});

			return 0;
		}
	}
}
