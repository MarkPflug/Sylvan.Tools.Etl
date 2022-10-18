using Sylvan.Data.Csv;
using Sylvan.Data.Excel;
using Sylvan.IO;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Sylvan.Data.Etl;

static class DataReader
{
	internal static DbDataReader OpenReader(string filename, bool useSchema = true, Action<double> prog = null, IEnumerable<DbColumn> sg = null)
	{
		var ext = Path.GetExtension(filename).ToLowerInvariant();
		prog = prog ?? (d => { });

		switch (ext)
		{
			case ".csv":
				return OpenCsv(filename, useSchema, sg, prog);
			case ".dbf":
				return OpenXBase(filename);
			case ".xls":
			case ".xlsb":
			case ".xlsx":
				return OpenExcel(filename, useSchema, prog);
			case ".zip":
				return OpenZip(filename);
			default:
				throw new NotSupportedException();
		}
	}

	static DbDataReader OpenZip(string filename)
	{
		var s = File.OpenRead(filename);
		var za = new ZipArchive(s);

		var fn = Path.GetFileNameWithoutExtension(filename);
		var entryName = fn + ".dbf";
		var entry = za.GetEntry(entryName);
		if (entry != null)
		{
			var es = entry.Open();
			return XBase.XBaseDataReader.Create(es);
		}
		throw new NotSupportedException();
	}

	static DbDataReader OpenCsv(string filename, bool useSchema, IEnumerable<DbColumn> sg, Action<double> prog)
	{
		Stream iStream = filename == "."
			? Console.OpenStandardInput()
			: new ProgressStream(File.OpenRead(filename), prog);

		var tr = new StreamReader(iStream);
		CsvSchema csvSchema = null;
		if (sg != null)
		{
			csvSchema = new CsvSchema(sg);
		}
		if (useSchema)
		{
			var schema = GetSchema(filename);
			csvSchema = schema == null ? null : new CsvSchema(schema);
		}
		var opts =
			new CsvDataReaderOptions
			{
				Schema = csvSchema,
				BufferSize = 0x20000,
				OwnsReader = true,
				CsvStyle = CsvStyle.Escaped,
				Escape = '\0',
			};

		return CsvDataReader.Create(tr, opts);
	}

	static DbDataReader OpenExcel(string filename, bool useSchema, Action<double> prog)
	{
		IExcelSchemaProvider excelSchema = ExcelSchema.Default;
		if (useSchema)
		{
			var schema = GetSchema(filename);
			if (schema != null)
			{
				excelSchema = new ExcelSchema(true, schema);

			}
		}

		var opts =
			new ExcelDataReaderOptions()
			{
				GetErrorAsNull = true,
				Schema = excelSchema,
			};
		var edr = ExcelDataReader.Create(filename, opts);
		var c = edr.RowCount;

		if (c > 0)
		{
			Action<long> cb = row =>
			{
				prog((double)row / (double)c);
			};

			return new ProgressDataReader(edr, cb);
		}
		else
		{
			// can't provide progress, so don't bother wrapping
			return edr;
		}
	}

	static Schema GetSchema(string file)
	{
		var schemaFile = file + ".schema";
		if (File.Exists(schemaFile))
			return Schema.Parse(File.ReadAllText(schemaFile));
		return null;
	}

	static Schema GetSchema(DbDataReader reader)
	{
		var a = new SchemaAnalyzer();

		var result = a.Analyze(reader);
		var sb = result.GetSchemaBuilder();
		var s = sb.Build();
		return s;
	}

	static DbDataReader OpenXBase(string filename)
	{
		Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
		var s = File.OpenRead(filename);
		return XBase.XBaseDataReader.Create(s);
	}
}
