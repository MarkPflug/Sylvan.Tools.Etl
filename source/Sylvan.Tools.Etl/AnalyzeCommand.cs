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

		[CommandArgument(1, "[Schema]")]
		public string Schema { get; set; }

		[CommandOption("-l|--lines <Count>")]
		public int Lines { get; set; }

		[CommandOption("-s|--skip <Skip>")]
		public int Skip { get; set; }

		public AnalyzeSettings()
		{
			this.Lines = 10000;
		}
	}

	class AnalyzeCommand : Command<AnalyzeSettings>
	{
		public override int Execute(CommandContext context, AnalyzeSettings settings)
		{
			var filename = settings.File;
			var output = settings.Schema ?? filename + ".schema";

			Stream oStream =
				output == "."
				? Console.OpenStandardOutput()
				: File.Create(output);

			
			var reader = DataReader.OpenReader(filename, false);
			for (int i = 0; i < settings.Skip; i++)
			{
				if(!reader.Read())
				{
					throw new Exception();
				}
			}

			var a = new SchemaAnalyzer(new SchemaAnalyzerOptions { AnalyzeRowCount = settings.Lines });
			var re = a.Analyze(reader);
			var sb = re.GetSchemaBuilder();
			
			foreach(var col in sb)
			{
				// TODO: think more about how to handle columns size
				col.ColumnSize = null;
			}
			var schema = sb.Build();

			using var tw = new StreamWriter(oStream);
			tw.Write(schema.ToString().Replace(",", "," + Environment.NewLine));
			return 0;
		}
	}
}
