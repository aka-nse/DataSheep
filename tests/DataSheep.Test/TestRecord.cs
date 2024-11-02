using Xunit;

namespace DataSheep;

internal interface IDataRecordEx<TRecord> : IDataRecord<TRecord>
    where TRecord : IDataRecordEx<TRecord>, IDataRecord<TRecord>
{
    public static abstract TRecord Create(int a, int b, string c);
    public void Deconstruct(out int a, out int b, out string c);
}

internal class TraitImpl<TRecord> where TRecord : IDataRecordEx<TRecord>
{
    public IReadOnlyList<string> DefaultColumnNames
        => ["A", "B", "C"];

    public IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex switch {
            0 => new MutableSeries<int>(columnName, initialCapacity),
            1 => new MutableSeries<int>(columnName, initialCapacity),
            2 => new MutableSeries<string>(columnName, initialCapacity),
            _ => throw new ArgumentOutOfRangeException(nameof(columnIndex)),
        };

    public void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination)
    {
        var a = series[0];
        var b = series[1];
        var c = series[2];
        for(var j = 0; j < destination.Length; ++j)
        {
            var row = rowIndex + j;
            destination[j] = TRecord.Create(a.GetValue<int>(row), b.GetValue<int>(row), c.GetValue<string>(row));
        }
    }

    public void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<TRecord> source)
    {
        var sa = series[0];
        var sb = series[1];
        var sc = series[2];
        for(var j = 0; j < source.Length; ++j)
        {
            var row = rowIndex + j;
            var (a, b, c) = source[j];
            sa.SetValue(row, a);
            sb.SetValue(row, b);
            sc.SetValue(row, c);
        }
    }
}

internal record class TestClassRecord(int A, int B, string C) : IDataRecordEx<TestClassRecord>
{
    public class TraitImpl : TraitImpl<TestClassRecord>, IRecordTrait<TestClassRecord>;

    public static IRecordTrait<TestClassRecord> Trait { get; } = new TraitImpl();

    public static TestClassRecord Create(int a, int b, string c) => new(a, b, c);
}

internal record struct TestStructRecord(int A, int B, string C) : IDataRecordEx<TestStructRecord>
{
    public class TraitImpl : TraitImpl<TestStructRecord>, IRecordTrait<TestStructRecord>;

    public static IRecordTrait<TestStructRecord> Trait { get; } = new TraitImpl();

    public static TestStructRecord Create(int a, int b, string c) => new(a, b, c);
}
