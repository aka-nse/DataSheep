using System.Collections;
using System.Runtime.CompilerServices;

namespace DataSheep;

internal readonly struct ValueTupleStrategy<T1> : IRecordStrategy<ValueTuple<T1>>
{
    public static int ColumnCount => 1;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new MutableSeries<T1>(columnNames[0], initialCapacity),
        ];

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<ValueTuple<T1>> destination)
    {
        var ser1 = series[0].As<T1>();
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = new(ser1[j]);
        }
    }

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1>> source)
    {
        var ser1 = series[0].As<T1>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1[j] = source[i].Item1;
        }
    }
}

// ValueTupleStrategy`2 -- ValueTupleStrategy`7 are defined in ValueTupleStrategy.g.cs

internal readonly struct ValueTupleStrategy<T1, T2, T3, T4, T5, T6, T7, TRest, TRestStrategy> : IRecordStrategy<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>
    where TRest : struct,
        IComparable,
        IComparable<TRest>,
        IEquatable<TRest>,
        IStructuralComparable,
        IStructuralEquatable,
        ITuple
    where TRestStrategy : IRecordStrategy<TRest>
{
    public static int ColumnCount => 7 + TRestStrategy.ColumnCount;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new MutableSeries<T1>(columnNames[0], initialCapacity),
            new MutableSeries<T2>(columnNames[1], initialCapacity),
            new MutableSeries<T3>(columnNames[2], initialCapacity),
            new MutableSeries<T4>(columnNames[3], initialCapacity),
            new MutableSeries<T5>(columnNames[4], initialCapacity),
            new MutableSeries<T6>(columnNames[5], initialCapacity),
            new MutableSeries<T7>(columnNames[6], initialCapacity),
            .. TRestStrategy.CreateSeriesPrefab(initialCapacity, columnNames.Slice(7)),
            ];

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
        TRestStrategy.ReadFromSeries(series[7..], rowIndex, tempRest.Span);
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = new(ser1[j], ser2[j], ser3[j], ser4[j], ser5[j], ser6[j], ser7[j], tempRest.Span[i]);
        }
    }

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
        TRestStrategy.WriteToSeries(series[7..], rowIndex, tempRest.Span);
    }
}
