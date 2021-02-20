using Spectre.Console.Cli;
using Sylvan.Data.Etl;

namespace Sylvan.Tools.Etl
{
    class Program
    {
        static int Main(string[] args)
        {
            //var s = new SqliteConnection("Data Source=export.db");
            //s.Open();
            //var cmd = s.CreateCommand();
            //cmd.CommandText = "select * from Test";
            //var reader = cmd.ExecuteReader();
            //while (reader.Read())
            //{
            //    var a = reader.GetString(0);
            //    var b = reader.GetString(1);
            //}

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
