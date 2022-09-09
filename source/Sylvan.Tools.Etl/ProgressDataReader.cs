using System;
using System.Data.Common;

namespace Sylvan.Data;

sealed class ProgressDataReader : DataReaderAdapter
{
	Action<long> progressCallback;

	long row = 0;

	public ProgressDataReader(DbDataReader dr, Action<long> progressCallback) : base(dr)
	{
		this.progressCallback = progressCallback;
	}

	public override bool Read()
	{
		progressCallback(row++);
		return base.Read();
	}
}
