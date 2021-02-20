using Spectre.Console.Cli;
using Sylvan.Data.Etl;

namespace Sylvan.Tools.Etl
{
	class Program
	{
		static int Main(string[] args)
		{
			var app = new CommandApp();
			app.Configure(config =>
			{
				config.AddCommand<ImportCommand>("import");
				config.AddCommand<AnalyzeCommand>("analyze");
				config.AddCommand<SelectCommand>("select");
			});

			return app.Run(args);
		}
	}
}
