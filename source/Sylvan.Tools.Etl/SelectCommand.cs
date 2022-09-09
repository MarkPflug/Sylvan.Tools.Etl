using Spectre.Console.Cli;
using Sylvan.Data.Csv;
using System;
using System.IO;

namespace Sylvan.Data.Etl;

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
		using var reader = DataReader.OpenReader(filename);

		var data = reader.Select(settings.Columns);

		var oStream =
			settings.Output == "."
			? Console.OpenStandardOutput()
			: File.Create(settings.Output);

		using var tw = new StreamWriter(oStream, Console.OutputEncoding, 0x1000);
		using var ww = CsvDataWriter.Create(tw);
		ww.Write(data);
		oStream.Flush();

		return 0;
	}
}
