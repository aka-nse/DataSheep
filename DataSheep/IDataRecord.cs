using System.Runtime.CompilerServices;

namespace DataSheep;

public interface IDataRecord<TSelf> : ITuple
    where TSelf : IDataRecord<TSelf>
{
    public static abstract IReadOnlyList<string> Columns { get; }
    public static abstract ISeries[] CreateSeriesPrefab(int initialCapacity);
    public static abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TSelf> destination);
    public static abstract void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<TSelf> source);
}
