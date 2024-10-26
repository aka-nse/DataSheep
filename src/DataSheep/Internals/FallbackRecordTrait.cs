
namespace DataSheep;

internal class FallbackRecordTrait<T> : IRecordTrait<T>
{
    public IReadOnlyList<string> DefaultColumnNames => [typeof(T).Name];

    public IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T>(typeof(T).Name, initialCapacity),
        ];

    public void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<T> destination)
        => destination[rowIndex] = series[0].As<T>()[rowIndex];

    public void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<T> source)
        => series[0].As<T>()[rowIndex] = source[rowIndex];
}
