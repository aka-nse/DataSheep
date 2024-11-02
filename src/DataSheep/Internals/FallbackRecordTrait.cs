
namespace DataSheep;

internal class FallbackRecordTrait<T> : IRecordTrait<T>
{
    public IReadOnlyList<string> DefaultColumnNames => [typeof(T).Name];

    public IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex == 0
            ? new MutableSeries<T>(typeof(T).Name, initialCapacity)
            : throw new ArgumentOutOfRangeException(nameof(columnIndex));

    public void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<T> destination)
    {
        series[0].GetValues(rowIndex, destination);
    }

    public void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<T> source)
    {
        series[0].SetValues(rowIndex, source);
    }
}
