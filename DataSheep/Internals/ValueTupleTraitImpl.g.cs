namespace DataSheep;

internal readonly struct ValueTupleTrait<T1, T2> : IValueTupleTrait<(T1, T2)>
{
    public static int ColumnCount => 2;
    int IRecordTrait.ColumnCount => 2;

    public static IReadOnlyList<string> DefaultColumnNames { get; }
    = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 2);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
        ];
    IMutableSeries[] IRecordTrait<(T1, T2)>.CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => CreateSeriesPrefab(initialCapacity, columnNames);

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
    void IRecordTrait<(T1, T2)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2)> destination)
        => ReadFromSeries(series, rowIndex, destination);

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
    void IRecordTrait<(T1, T2)>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2)> source)
        => WriteToSeries(series, rowIndex, source);
}

internal readonly struct ValueTupleTrait<T1, T2, T3> : IValueTupleTrait<(T1, T2, T3)>
{
    public static int ColumnCount => 3;
    int IRecordTrait.ColumnCount => 3;

    public static IReadOnlyList<string> DefaultColumnNames { get; }
    = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 3);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
        ];
    IMutableSeries[] IRecordTrait<(T1, T2, T3)>.CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => CreateSeriesPrefab(initialCapacity, columnNames);

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
    void IRecordTrait<(T1, T2, T3)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3)> destination)
        => ReadFromSeries(series, rowIndex, destination);

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
    void IRecordTrait<(T1, T2, T3)>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3)> source)
        => WriteToSeries(series, rowIndex, source);
}

internal readonly struct ValueTupleTrait<T1, T2, T3, T4> : IValueTupleTrait<(T1, T2, T3, T4)>
{
    public static int ColumnCount => 4;
    int IRecordTrait.ColumnCount => 4;

    public static IReadOnlyList<string> DefaultColumnNames { get; }
    = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 4);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
        ];
    IMutableSeries[] IRecordTrait<(T1, T2, T3, T4)>.CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => CreateSeriesPrefab(initialCapacity, columnNames);

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
    void IRecordTrait<(T1, T2, T3, T4)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4)> destination)
        => ReadFromSeries(series, rowIndex, destination);

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
    void IRecordTrait<(T1, T2, T3, T4)>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4)> source)
        => WriteToSeries(series, rowIndex, source);
}

internal readonly struct ValueTupleTrait<T1, T2, T3, T4, T5> : IValueTupleTrait<(T1, T2, T3, T4, T5)>
{
    public static int ColumnCount => 5;
    int IRecordTrait.ColumnCount => 5;

    public static IReadOnlyList<string> DefaultColumnNames { get; }
    = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 5);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
            new ArraySeries<T5>(columnNames[4], initialCapacity),
        ];
    IMutableSeries[] IRecordTrait<(T1, T2, T3, T4, T5)>.CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => CreateSeriesPrefab(initialCapacity, columnNames);

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
    void IRecordTrait<(T1, T2, T3, T4, T5)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5)> destination)
        => ReadFromSeries(series, rowIndex, destination);

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
    void IRecordTrait<(T1, T2, T3, T4, T5)>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5)> source)
        => WriteToSeries(series, rowIndex, source);
}

internal readonly struct ValueTupleTrait<T1, T2, T3, T4, T5, T6> : IValueTupleTrait<(T1, T2, T3, T4, T5, T6)>
{
    public static int ColumnCount => 6;
    int IRecordTrait.ColumnCount => 6;

    public static IReadOnlyList<string> DefaultColumnNames { get; }
    = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 6);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
            new ArraySeries<T5>(columnNames[4], initialCapacity),
            new ArraySeries<T6>(columnNames[5], initialCapacity),
        ];
    IMutableSeries[] IRecordTrait<(T1, T2, T3, T4, T5, T6)>.CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => CreateSeriesPrefab(initialCapacity, columnNames);

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
    void IRecordTrait<(T1, T2, T3, T4, T5, T6)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5, T6)> destination)
        => ReadFromSeries(series, rowIndex, destination);

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
    void IRecordTrait<(T1, T2, T3, T4, T5, T6)>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6)> source)
        => WriteToSeries(series, rowIndex, source);
}

internal readonly struct ValueTupleTrait<T1, T2, T3, T4, T5, T6, T7> : IValueTupleTrait<(T1, T2, T3, T4, T5, T6, T7)>
{
    public static int ColumnCount => 7;
    int IRecordTrait.ColumnCount => 7;

    public static IReadOnlyList<string> DefaultColumnNames { get; }
    = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 7);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
            new ArraySeries<T5>(columnNames[4], initialCapacity),
            new ArraySeries<T6>(columnNames[5], initialCapacity),
            new ArraySeries<T7>(columnNames[6], initialCapacity),
        ];
    IMutableSeries[] IRecordTrait<(T1, T2, T3, T4, T5, T6, T7)>.CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => CreateSeriesPrefab(initialCapacity, columnNames);

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
    void IRecordTrait<(T1, T2, T3, T4, T5, T6, T7)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5, T6, T7)> destination)
        => ReadFromSeries(series, rowIndex, destination);

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
    void IRecordTrait<(T1, T2, T3, T4, T5, T6, T7)>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6, T7)> source)
        => WriteToSeries(series, rowIndex, source);
}

