using Spectre.Console;
using System;
using System.Threading.Tasks;

namespace Sylvan.Data.Etl;

static class SpectreExtensions
{
	public static Task AddTask(this ProgressContext ctx, string description, Func<Action<double>, Task> taskFactory, double maxValue = 1d)
	{
		var task = ctx.AddTask(description, new ProgressTaskSettings { MaxValue = maxValue });

		double last = 0d;

		Action<double> setProgress =
			progress =>
			{
				var delta = progress - last;
				task.Increment(delta);
				last = progress;
			};

		var tt = taskFactory(setProgress)
			// ensures that the task reaches "completed".
			.ContinueWith(
				t =>
				{
					if (t.IsCompletedSuccessfully)
					{
						task.Increment(maxValue);
					}
					else
					{
						throw t.Exception;
					}
				}
			);

		return tt;
	}
}