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
    public new static abstract IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName);
    public new static abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<T> destination);
    public new static abstract void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<T> source);
}

internal readonly struct ValueTupleTrait<T1> : IValueTupleTrait<ValueTuple<T1>>
{
    public static int ColumnCount => 1;
    int IRecordTrait.ColumnCount => 1;

    public static IReadOnlyList<string> DefaultColumnNames { get; } = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 1);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex == 0
            ? new MutableSeries<T1>(columnName, initialCapacity)
            : throw new ArgumentOutOfRangeException(nameof(columnIndex));
    IMutableSeries IRecordTrait.CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => CreateSeries(columnIndex, initialCapacity, columnName);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1>> destination)
    {
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = new(series[0].GetValue<T1>(j));
        }
    }
    void IRecordTrait<ValueTuple<T1>>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1>> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1>> source)
    {
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            series[0].SetValue(j, source[i].Item1);
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

    public static IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex switch
        {
            0 => new MutableSeries<T1>(columnName, initialCapacity),
            1 => new MutableSeries<T2>(columnName, initialCapacity),
            2 => new MutableSeries<T3>(columnName, initialCapacity),
            3 => new MutableSeries<T4>(columnName, initialCapacity),
            4 => new MutableSeries<T5>(columnName, initialCapacity),
            5 => new MutableSeries<T6>(columnName, initialCapacity),
            6 => new MutableSeries<T7>(columnName, initialCapacity),
            _ =>  TRestTrait.CreateSeries(columnIndex - 7, initialCapacity, columnName),
        };
    IMutableSeries IRecordTrait.CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => CreateSeries(columnIndex, initialCapacity, columnName);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> destination)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        var ser5 = series[4];
        var ser6 = series[5];
        var ser7 = series[6];
        using var tempRest = new TemporaryBuffer<TRest>(destination.Length);
        TRestTrait.ReadFromSeries(series[7..], rowIndex, tempRest.Span);
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = new(ser1.GetValue<T1>(j), ser2.GetValue<T2>(j), ser3.GetValue<T3>(j), ser4.GetValue<T4>(j), ser5.GetValue<T5>(j), ser6.GetValue<T6>(j), ser7.GetValue<T7>(j), tempRest.Span[i]);
        }
    }
    void IRecordTrait<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> source)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        var ser5 = series[4];
        var ser6 = series[5];
        var ser7 = series[6];
        using var tempRest = new TemporaryBuffer<TRest>(source.Length);
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1.SetValue(j, source[i].Item1);
            ser2.SetValue(j, source[i].Item2);
            ser3.SetValue(j, source[i].Item3);
            ser4.SetValue(j, source[i].Item4);
            ser5.SetValue(j, source[i].Item5);
            ser6.SetValue(j, source[i].Item6);
            ser7.SetValue(j, source[i].Item7);
            tempRest.Span[i] = source[i].Rest;
        }
        TRestTrait.WriteToSeries(series[7..], rowIndex, tempRest.Span);
    }
    void IRecordTrait<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> source)
        => WriteToSeries(series, rowIndex, source);
}
