using Spectre.Console.Cli;
using Sylvan.Data.Csv;
using System;
using System.IO;

namespace Sylvan.Data.Etl
{
	class AnalyzeSettings : CommandSettings
	{
		[CommandArgument(0, "[File]")]
		public string File { get; set; }

		[CommandArgument(1, "[Lines]")]
		public int Lines { get; set; }

		[CommandOption("-s|--skip <SKIP>")]
		public int Skip { get; set; }

		public AnalyzeSettings()
		{
			this.Lines = 100000;
		}
	}

	class AnalyzeCommand : Command<AnalyzeSettings>
	{
		public override int Execute(CommandContext context, AnalyzeSettings settings)
		{
			var filename = settings.File;
			var output = filename + ".schema";

			var tr = new StreamReader(filename);
			for (int i = 0; i < settings.Skip; i++)
			{
				tr.ReadLine();
			}

			var csv = CsvDataReader.Create(tr);
			var a = new SchemaAnalyzer(new SchemaAnalyzerOptions { AnalyzeRowCount = settings.Lines });
			var re = a.Analyze(csv);
			var schema = re.GetSchema();

			File.WriteAllText(output, schema.ToString().Replace(",", "," + Environment.NewLine));
			return 0;
		}
	}
}
