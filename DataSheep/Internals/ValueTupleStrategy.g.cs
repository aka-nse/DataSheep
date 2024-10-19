namespace DataSheep;

internal readonly struct ValueTupleStrategy<T1, T2> : IRecordStrategy<(T1, T2)>
{
    public static int ColumnCount => 2;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new MutableSeries<T1>(columnNames[0], initialCapacity),
            new MutableSeries<T2>(columnNames[1], initialCapacity),
        ];

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2)> destination)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1[j], ser2[j]);
        }
    }

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2)> source)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            (ser1[j], ser2[j]) = source[i];
        }
    }
}

internal readonly struct ValueTupleStrategy<T1, T2, T3> : IRecordStrategy<(T1, T2, T3)>
{
    public static int ColumnCount => 3;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new MutableSeries<T1>(columnNames[0], initialCapacity),
            new MutableSeries<T2>(columnNames[1], initialCapacity),
            new MutableSeries<T3>(columnNames[2], initialCapacity),
        ];

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3)> destination)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1[j], ser2[j], ser3[j]);
        }
    }

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3)> source)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            (ser1[j], ser2[j], ser3[j]) = source[i];
        }
    }
}

internal readonly struct ValueTupleStrategy<T1, T2, T3, T4> : IRecordStrategy<(T1, T2, T3, T4)>
{
    public static int ColumnCount => 4;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new MutableSeries<T1>(columnNames[0], initialCapacity),
            new MutableSeries<T2>(columnNames[1], initialCapacity),
            new MutableSeries<T3>(columnNames[2], initialCapacity),
            new MutableSeries<T4>(columnNames[3], initialCapacity),
        ];

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4)> destination)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1[j], ser2[j], ser3[j], ser4[j]);
        }
    }

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4)> source)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            (ser1[j], ser2[j], ser3[j], ser4[j]) = source[i];
        }
    }
}

internal readonly struct ValueTupleStrategy<T1, T2, T3, T4, T5> : IRecordStrategy<(T1, T2, T3, T4, T5)>
{
    public static int ColumnCount => 5;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new MutableSeries<T1>(columnNames[0], initialCapacity),
            new MutableSeries<T2>(columnNames[1], initialCapacity),
            new MutableSeries<T3>(columnNames[2], initialCapacity),
            new MutableSeries<T4>(columnNames[3], initialCapacity),
            new MutableSeries<T5>(columnNames[4], initialCapacity),
        ];

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5)> destination)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        var ser5 = series[4].As<T5>();
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1[j], ser2[j], ser3[j], ser4[j], ser5[j]);
        }
    }

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5)> source)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        var ser5 = series[4].As<T5>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            (ser1[j], ser2[j], ser3[j], ser4[j], ser5[j]) = source[i];
        }
    }
}

internal readonly struct ValueTupleStrategy<T1, T2, T3, T4, T5, T6> : IRecordStrategy<(T1, T2, T3, T4, T5, T6)>
{
    public static int ColumnCount => 6;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new MutableSeries<T1>(columnNames[0], initialCapacity),
            new MutableSeries<T2>(columnNames[1], initialCapacity),
            new MutableSeries<T3>(columnNames[2], initialCapacity),
            new MutableSeries<T4>(columnNames[3], initialCapacity),
            new MutableSeries<T5>(columnNames[4], initialCapacity),
            new MutableSeries<T6>(columnNames[5], initialCapacity),
        ];

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5, T6)> destination)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        var ser5 = series[4].As<T5>();
        var ser6 = series[5].As<T6>();
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1[j], ser2[j], ser3[j], ser4[j], ser5[j], ser6[j]);
        }
    }

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6)> source)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        var ser5 = series[4].As<T5>();
        var ser6 = series[5].As<T6>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            (ser1[j], ser2[j], ser3[j], ser4[j], ser5[j], ser6[j]) = source[i];
        }
    }
}

internal readonly struct ValueTupleStrategy<T1, T2, T3, T4, T5, T6, T7> : IRecordStrategy<(T1, T2, T3, T4, T5, T6, T7)>
{
    public static int ColumnCount => 7;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new MutableSeries<T1>(columnNames[0], initialCapacity),
            new MutableSeries<T2>(columnNames[1], initialCapacity),
            new MutableSeries<T3>(columnNames[2], initialCapacity),
            new MutableSeries<T4>(columnNames[3], initialCapacity),
            new MutableSeries<T5>(columnNames[4], initialCapacity),
            new MutableSeries<T6>(columnNames[5], initialCapacity),
            new MutableSeries<T7>(columnNames[6], initialCapacity),
        ];

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5, T6, T7)> destination)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        var ser5 = series[4].As<T5>();
        var ser6 = series[5].As<T6>();
        var ser7 = series[6].As<T7>();
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1[j], ser2[j], ser3[j], ser4[j], ser5[j], ser6[j], ser7[j]);
        }
    }

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6, T7)> source)
    {
        var ser1 = series[0].As<T1>();
        var ser2 = series[1].As<T2>();
        var ser3 = series[2].As<T3>();
        var ser4 = series[3].As<T4>();
        var ser5 = series[4].As<T5>();
        var ser6 = series[5].As<T6>();
        var ser7 = series[6].As<T7>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            (ser1[j], ser2[j], ser3[j], ser4[j], ser5[j], ser6[j], ser7[j]) = source[i];
        }
    }
}

