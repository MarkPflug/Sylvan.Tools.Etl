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
	class SelectSettings : CommandSettings
	{
		[CommandArgument(0, "[File]")]
		public string File { get; set; }

		[CommandArgument(1, "[Output]")]
		public string Output { get; set; }

		[CommandArgument(2, "[Columns]")]
		public int[] Columns { get; set; }

		[CommandOption("-s|--skip <SKIP>")]
		public int Skip { get; set; }
	}

	class SelectCommand : Command<SelectSettings>
	{
		public override int Execute(
			CommandContext context,
			SelectSettings settings
		)
		{
			double last = 0.0;
			double prog = 0.0;

			ProgressTask task = null;
			var mre = new ManualResetEvent(false);
			bool done = false;
			Exception ex = null;
			void UpdateProgress(double p)
			{
				prog = p * 100d;
				mre.Set();
			}

			Task.Run(() =>
			{
				CsvDataReader csv = null;
				try
				{
					Stream s = File.OpenRead(settings.File);
					s = s.WithReadProgress(UpdateProgress, 0.001);
					var tr = new StreamReader(s);
					for (int i = 0; i < settings.Skip; i++)
					{
						tr.ReadLine();
					}
					var opts =
						new CsvDataReaderOptions
						{
							BufferSize = 0x100000,
						};
					csv = CsvDataReader.Create(tr, opts);
					var data = csv.Select(settings.Columns);

					var outWriter = new StreamWriter(settings.Output);
					var ww = CsvDataWriter.Create(outWriter);
					ww.Write(data);
				}
				catch (Exception exx)
				{
					var rn = csv?.RowNumber ?? -1;
					Console.WriteLine("Error around row: " + rn);
					ex = exx;
				}

				done = true;
				mre.Set();
				return 1;
			});

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
					task = ctx.AddTask("Select");
					while (!done)
					{
						mre.WaitOne();
						if (ex != null)
						{
							throw ex;
						}
						var inc = prog - last;
						last = prog;
						task.Increment(inc);
						mre.Reset();
					}
				});

			return 1;
		}
	}
}
