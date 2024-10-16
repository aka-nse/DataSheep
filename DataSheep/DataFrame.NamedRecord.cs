using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

partial class DataFrame
{
    public static DataFrame<TRecord> Create<TRecord>(int initialCapacity = 256)
        where TRecord : IDataRecord<TRecord>
        => new NamedRecordDataFrame<TRecord>(initialCapacity);

    public static DataFrame<TRecord> Create<TRecord>(IReadOnlyList<TRecord> records)
        where TRecord : IDataRecord<TRecord>
        => new NamedRecordDataFrame<TRecord>(records);

    public static DataFrame<TRecord> Create<TRecord>(ReadOnlySpan<TRecord> records)
        where TRecord : IDataRecord<TRecord>
        => new NamedRecordDataFrame<TRecord>(records);
}


file class NamedRecordDataFrame<TRecord> : DataFrame<TRecord>
    where TRecord : IDataRecord<TRecord>
{
    public NamedRecordDataFrame(int initialCapacity)
        : base(TRecord.CreateSeriesPrefab(initialCapacity))
    {
    }

    public NamedRecordDataFrame(IReadOnlyList<TRecord> records)
        : base(TRecord.CreateSeriesPrefab(records.Count))
    {
        using var temp = TemporaryBuffer.Create(records);
        AddRange(temp.Span);
    }

    public NamedRecordDataFrame(ReadOnlySpan<TRecord> records)
        : base(TRecord.CreateSeriesPrefab(records.Length))
    {
        AddRange(records);
    }

    protected override void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination)
        => TRecord.ReadFromSeries(series, rowIndex, destination);

    protected override void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<TRecord> source)
        => TRecord.WriteToSeries(series, rowIndex, source);
}
