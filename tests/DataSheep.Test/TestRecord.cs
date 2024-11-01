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

    public IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
                new ArraySeries<int>(columnNames[0], initialCapacity),
                new ArraySeries<int>(columnNames[1], initialCapacity),
                new ArraySeries<string>(columnNames[2], initialCapacity),
        ];

    public void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination)
    {
        var a = series[0].As<int>();
        var b = series[1].As<int>();
        var c = series[2].As<string>();
        for(var j = 0; j < destination.Length; ++j)
        {
            var row = rowIndex + j;
            destination[j] = TRecord.Create(a[row], b[row], c[row]);
        }
    }

    public void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<TRecord> source)
    {
        var a = series[0].As<int>();
        var b = series[1].As<int>();
        var c = series[2].As<string>();
        for(var j = 0; j < source.Length; ++j)
        {
            var row = rowIndex + j;
            (a[row], b[row], c[row]) = source[j];
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
