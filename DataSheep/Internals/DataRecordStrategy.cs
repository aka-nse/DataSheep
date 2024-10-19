using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

internal readonly struct DataRecordStrategy<TRecord> : IRecordStrategy<TRecord>
    where TRecord : IDataRecord<TRecord>
{
    public static IReadOnlyList<string> Columns => TRecord.Columns;

    public static int ColumnCount
        => TRecord.Columns.Count;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => TRecord.CreateSeriesInitial(initialCapacity, columnNames);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination)
        => TRecord.ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<TRecord> source)
        => TRecord.WriteToSeries(series, rowIndex, source);
}