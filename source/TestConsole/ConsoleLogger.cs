using Microsoft.Extensions.Logging;

namespace TestConsole;

class ConsoleLogger : ILogger
{
	public IDisposable BeginScope<TState>(TState state)
	{
		return null!;
	}

	public bool IsEnabled(LogLevel logLevel)
	{
		return true;
	}

	public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
	{
		var str = formatter(state, exception);
		Console.ForegroundColor = GetColor(logLevel);
		Console.WriteLine(str);
	}

	static ConsoleColor GetColor(LogLevel level)
	{
		switch (level)
		{
			case LogLevel.Critical:
				return ConsoleColor.Magenta;
			case LogLevel.Error:
				return ConsoleColor.Red;
			case LogLevel.Warning:
				return ConsoleColor.Yellow;
			case LogLevel.Information:
				return ConsoleColor.White;
			case LogLevel.Debug:
				return ConsoleColor.Gray;
		}
		return ConsoleColor.White;
	}
}