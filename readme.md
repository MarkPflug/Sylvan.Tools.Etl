# <img src="Sylvan.png" height="48" alt="Sylvan Logo"/> Sylvan.Tools.Etl

Sylvan.Tools.Etl is a .NET global tool to help with common ETL (extract transform load) operations.

## Installation

Requires [.NET SDK](https://dotnet.microsoft.com/download). Installs as a .NET global tool.

```
dotnet tool install -g Sylvan.Tools.Etl
```

### Analyze

Analyzes a CSV file and creates a .schema file describing the data.

`setl analyze data.csv`

Creates a file named data.csv.schema.

### Import

Imports a CSV file into a database. Currently supports either SqlServer or Sqlite.

`setl import SqlServer MyDb data.csv ImportedData`

Will analyze the CSV contents to determine schema, or use a .schema file if one exists.

### Export

Exports a database query to a CSV file.

`setl export SqlServer MyDb "select * from ImportedData where " data.csv`

Will export the results of the query to a file named data.csv and create an accompanying .schema file.

### Select

Selects columns from a CSV file.

`setl select data.csv dump.csv 0 1 2 12`
