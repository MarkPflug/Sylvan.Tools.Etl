using Spectre.Console;
using Spectre.Console.Cli;
using Sylvan.Data.Csv;
using Sylvan.IO;
using System;
using System.Data.Common;
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
		public string Provider { get; set; }

		[CommandArgument(1, "[Database]")]
        public string Database { get; set; }

        [CommandArgument(2, "[File]")]
        public string File { get; set; }

		[CommandOption("-t|--table <Table>")]
		public string Table { get; set; }

		[CommandOption("-s|--skip <SKIP>")]
        public int Skip { get; set; }
    }

    class ImportCommand : Command<ImportSettings>
    {
        DataLoader GetLoader(string provider)
        {
            if (!Enum.TryParse<Provider>(provider, out var p))
            {
                throw new ArgumentException($"Unknown provider {provider}", nameof(provider));
            }

            switch (p)
            {
                case Provider.SqlServer:
                    return new SqlServerLoader();
                case Provider.Sqlite:
                    return new SqliteFastLoader();
            }
            throw new NotSupportedException();
        }

        IDbColumnSchemaGenerator GetSchema(string filename)
        {
            var schemaFile = filename + ".schema";
            if (File.Exists(schemaFile))
            {
                return Schema.Parse(File.ReadAllText(schemaFile));
            }
            else
            {
                using var csv = CsvDataReader.Create(filename);
                var a = new SchemaAnalyzer(new SchemaAnalyzerOptions { AnalyzeRowCount = 1000000 });
                var re = a.Analyze(csv);
                var schema = re.GetSchema();
                // var tableCmdText = loader.BuildTable(tableName, schema);
                csv.Dispose();
                return schema;
            }
        }

        public override int Execute(
            CommandContext context,
            ImportSettings settings
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

                    var schema = GetSchema(filename);
                    var opts = 
                        new CsvDataReaderOptions { 
                            BufferSize = 0x100000, 
                            Schema = new CsvSchema(schema.GetColumnSchema()),
                        };
                    csv = CsvDataReader.Create(tr, opts);
                    loader.Load(schema, csv, tableName, database);
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
                    task = ctx.AddTask("Import");
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
