using Spectre.Console.Cli;
using Sylvan.Data.Csv;
using System;
using System.IO;

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
			var filename = settings.File;

			Stream iStream = filename == "."
				? Console.OpenStandardInput()
				: File.OpenRead(settings.File);

			var tr = new StreamReader(iStream);
			for (int i = 0; i < settings.Skip; i++)
			{
				tr.ReadLine();
			}
			var opts =
				new CsvDataReaderOptions
				{
					BufferSize = 0x100000,
				};
			var csv = CsvDataReader.Create(tr, opts);
			var data = csv.Select(settings.Columns);

			var oStream =
				settings.Output == "."
				? Console.OpenStandardOutput()
				: File.Create(settings.Output);

			var tw = new StreamWriter(oStream);
			var ww = CsvDataWriter.Create(tw);
			ww.Write(data);

			return 0;
		}
	}
}
