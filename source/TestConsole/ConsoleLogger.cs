using Microsoft.Extensions.Logging;

namespace TestConsole;

sealed class ConsoleLogger : ILogger
{
	public IDisposable? BeginScope<TState>(TState state) where TState : notnull
	{
		return null;
	}

	public bool IsEnabled(LogLevel logLevel)
	{
		return true;
	}

	public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
	{
		var str = formatter(state, exception);
		SetColor(logLevel);
		Console.Write(DateTime.Now.ToString("HH:mm:ss"));
		Console.Write(" ");
		Console.Write(ShortCode(logLevel));
		Console.Write(" ");
		Console.WriteLine(str);
	}

	static char ShortCode(LogLevel l)
	{
		return Code(l)[0];
	}

	static string Code(LogLevel l)
	{
		return l switch
		{
			LogLevel.Debug => "DBG",
			LogLevel.Information => "INF",
			LogLevel.Warning => "WRN",
			LogLevel.Error => "ERR",
			LogLevel.Critical => "CRT",
			_ => "???"
		};
	}

	static void SetColor(LogLevel level)
	{
		Console.ResetColor();
		switch (level)
		{
			case LogLevel.Critical:
				Console.ForegroundColor = ConsoleColor.White;
				Console.BackgroundColor = ConsoleColor.DarkRed;
				break;
			case LogLevel.Error:
				Console.ForegroundColor = ConsoleColor.Red;
				break;
			case LogLevel.Warning:
				Console.ForegroundColor = ConsoleColor.Yellow;
				break;
			case LogLevel.Information:
				Console.ForegroundColor = ConsoleColor.White;
				break;
			case LogLevel.Debug:
				Console.ForegroundColor = ConsoleColor.DarkGray;
				break;
		}
	}
}