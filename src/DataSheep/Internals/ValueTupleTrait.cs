using System.Collections;
using System.Runtime.CompilerServices;

namespace DataSheep;

internal static class ValueTupleTrait
{
    public static readonly string[] DefaultColumnNames = ["Column 1", "Column 2", "Column 3", "Column 4", "Column 5", "Column 6", "Column 7",];
}

internal interface IValueTupleTrait<T> : IRecordTrait<T>
    where T : struct,
        IComparable,
        IComparable<T>,
        IEquatable<T>,
        IStructuralComparable,
        IStructuralEquatable,
        ITuple
{
    public new static abstract int ColumnCount { get; }
    public new static abstract IReadOnlyList<string> DefaultColumnNames { get; }
    public new static abstract IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames);
    public new static abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<T> destination);
    public new static abstract void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<T> source);
}

internal readonly struct ValueTupleTrait<T1> : IValueTupleTrait<ValueTuple<T1>>
{
    public static int ColumnCount => 1;
    int IRecordTrait.ColumnCount => 1;

    public static IReadOnlyList<string> DefaultColumnNames { get; } = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 1);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
        ];
    IMutableSeries[] IRecordTrait<ValueTuple<T1>>.CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => CreateSeriesPrefab(initialCapacity, columnNames);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1>> destination)
    {
        var ser1 = series[0].As<T1>();
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = new(ser1[j]);
        }
    }
    void IRecordTrait<ValueTuple<T1>>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1>> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1>> source)
    {
        var ser1 = series[0].As<T1>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1[j] = source[i].Item1;
        }
    }
    void IRecordTrait<ValueTuple<T1>>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1>> source)
        => WriteToSeries(series, rowIndex, source);
}

// ValueTupleStrategy`2 -- ValueTupleStrategy`7 are defined in ValueTupleStrategy.g.cs

internal readonly struct ValueTupleTrait<T1, T2, T3, T4, T5, T6, T7, TRest, TRestTrait> : IValueTupleTrait<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>
    where TRest : struct,
        IComparable,
        IComparable<TRest>,
        IEquatable<TRest>,
        IStructuralComparable,
        IStructuralEquatable,
        ITuple
    where TRestTrait : struct, IValueTupleTrait<TRest>
{
    public static int ColumnCount => 7 + TRestTrait.ColumnCount;
    int IRecordTrait.ColumnCount => ColumnCount;

    public static IReadOnlyList<string> DefaultColumnNames { get; }
        = Enumerable
            .Range(1, 7 + TRestTrait.ColumnCount)
            .Select(x => $"Column {x}")
            .ToArray();
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames
        => DefaultColumnNames;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
            new ArraySeries<T5>(columnNames[4], initialCapacity),
            new ArraySeries<T6>(columnNames[5], initialCapacity),
            new ArraySeries<T7>(columnNames[6], initialCapacity),
            .. TRestTrait.CreateSeriesPrefab(initialCapacity, columnNames.Slice(7)),
            ];
    IMutableSeries[] IRecordTrait<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>.CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => CreateSeriesPrefab(initialCapacity, columnNames);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> destination)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        var ser5 = series[4].As<T5>();
        var ser6 = series[5].As<T6>();
        var ser7 = series[6].As<T7>();
        using var tempRest = new TemporaryBuffer<TRest>(destination.Length);
        TRestTrait.ReadFromSeries(series[7..], rowIndex, tempRest.Span);
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = new(ser1[j], ser2[j], ser3[j], ser4[j], ser5[j], ser6[j], ser7[j], tempRest.Span[i]);
        }
    }
    void IRecordTrait<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> source)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        var ser5 = series[4].As<T5>();
        var ser6 = series[5].As<T6>();
        var ser7 = series[6].As<T7>();
        using var tempRest = new TemporaryBuffer<TRest>(source.Length);
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1[j] = source[i].Item1;
            ser2[j] = source[i].Item2;
            ser3[j] = source[i].Item3;
            ser4[j] = source[i].Item4;
            ser5[j] = source[i].Item5;
            ser6[j] = source[i].Item6;
            ser7[j] = source[i].Item7;
            tempRest.Span[i] = source[i].Rest;
        }
        TRestTrait.WriteToSeries(series[7..], rowIndex, tempRest.Span);
    }
    void IRecordTrait<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> source)
        => WriteToSeries(series, rowIndex, source);
}
