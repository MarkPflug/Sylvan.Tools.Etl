using Sylvan.Data.Csv;
using Sylvan.Data.Etl;
using System.Diagnostics;

var sw = Stopwatch.StartNew();

var loader = new SqliteProvider("Data Source=mydata.db");
//File.Delete("asdf");

var csv = CsvDataReader.Create("/data/csv/yellow_tripdata_2020-01.csv");

loader.LoadData("data", csv);
var ww = CsvDataWriter.Create("dumperoo.csv");
var conn = loader.GetConnection();
var cmd = conn.CreateCommand();
cmd.CommandText = "select * from data";
var r = cmd.ExecuteReader();
ww.Write(r);

sw.Stop();
Console.WriteLine("Done " + sw.Elapsed);
