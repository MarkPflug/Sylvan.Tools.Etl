# <img src="Sylvan.png" height="48" alt="Sylvan Logo"/> Sylvan.Tools.Etl

Sylvan.Tools.Etl is a .NET global tool for doing ETL (extract transform load) operations.

## Installation

Requires [.NET SDK](https://dotnet.microsoft.com/download). Installs as a global tool.

```
dotnet tool install -g Sylvan.Tools.Etl
```

### Analyze

Analyzes a CSV file and creates a .schema file describing the data.

`setl analyze data.csv`

### Import

Imports a CSV file into a database. Currently supports either SqlServer or Sqlite.

`setl import SqlServer MyDb data.csv ImportedData`

### Select

Selects columns from a CSV file.

`setl select data.csv dump.csv 0 1 2 12`
