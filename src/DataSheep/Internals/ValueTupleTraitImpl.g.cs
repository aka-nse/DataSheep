namespace DataSheep;

internal readonly struct ValueTupleTrait<T1, T2> : IValueTupleTrait<(T1, T2)>
{
    public static int ColumnCount => 2;
    int IRecordTrait.ColumnCount => 2;

    public static IReadOnlyList<string> DefaultColumnNames { get; }
    = new ArraySegment<string>(ValueTupleTrait.DefaultColumnNames, 0, 2);
    IReadOnlyList<string> IRecordTrait.DefaultColumnNames => DefaultColumnNames;

    public static IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex switch {
            0 => new MutableSeries<T1>(columnName, initialCapacity),
            1 => new MutableSeries<T2>(columnName, initialCapacity),
            _ => throw new ArgumentOutOfRangeException(nameof(columnIndex)),
        };
    IMutableSeries IRecordTrait.CreateSeries(int columnIndex,int initialCapacity, string columnName)
        => CreateSeries(columnIndex, initialCapacity, columnName);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2)> destination)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1.GetValue<T1>(j), ser2.GetValue<T2>(j));
        }
    }
    void IRecordTrait<(T1, T2)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2)> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2)> source)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1.SetValue<T1>(j, source[i].Item1);
            ser2.SetValue<T2>(j, source[i].Item2);
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

    public static IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex switch {
            0 => new MutableSeries<T1>(columnName, initialCapacity),
            1 => new MutableSeries<T2>(columnName, initialCapacity),
            2 => new MutableSeries<T3>(columnName, initialCapacity),
            _ => throw new ArgumentOutOfRangeException(nameof(columnIndex)),
        };
    IMutableSeries IRecordTrait.CreateSeries(int columnIndex,int initialCapacity, string columnName)
        => CreateSeries(columnIndex, initialCapacity, columnName);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3)> destination)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1.GetValue<T1>(j), ser2.GetValue<T2>(j), ser3.GetValue<T3>(j));
        }
    }
    void IRecordTrait<(T1, T2, T3)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3)> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3)> source)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1.SetValue<T1>(j, source[i].Item1);
            ser2.SetValue<T2>(j, source[i].Item2);
            ser3.SetValue<T3>(j, source[i].Item3);
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

    public static IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex switch {
            0 => new MutableSeries<T1>(columnName, initialCapacity),
            1 => new MutableSeries<T2>(columnName, initialCapacity),
            2 => new MutableSeries<T3>(columnName, initialCapacity),
            3 => new MutableSeries<T4>(columnName, initialCapacity),
            _ => throw new ArgumentOutOfRangeException(nameof(columnIndex)),
        };
    IMutableSeries IRecordTrait.CreateSeries(int columnIndex,int initialCapacity, string columnName)
        => CreateSeries(columnIndex, initialCapacity, columnName);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4)> destination)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1.GetValue<T1>(j), ser2.GetValue<T2>(j), ser3.GetValue<T3>(j), ser4.GetValue<T4>(j));
        }
    }
    void IRecordTrait<(T1, T2, T3, T4)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4)> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4)> source)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1.SetValue<T1>(j, source[i].Item1);
            ser2.SetValue<T2>(j, source[i].Item2);
            ser3.SetValue<T3>(j, source[i].Item3);
            ser4.SetValue<T4>(j, source[i].Item4);
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

    public static IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex switch {
            0 => new MutableSeries<T1>(columnName, initialCapacity),
            1 => new MutableSeries<T2>(columnName, initialCapacity),
            2 => new MutableSeries<T3>(columnName, initialCapacity),
            3 => new MutableSeries<T4>(columnName, initialCapacity),
            4 => new MutableSeries<T5>(columnName, initialCapacity),
            _ => throw new ArgumentOutOfRangeException(nameof(columnIndex)),
        };
    IMutableSeries IRecordTrait.CreateSeries(int columnIndex,int initialCapacity, string columnName)
        => CreateSeries(columnIndex, initialCapacity, columnName);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5)> destination)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        var ser5 = series[4];
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1.GetValue<T1>(j), ser2.GetValue<T2>(j), ser3.GetValue<T3>(j), ser4.GetValue<T4>(j), ser5.GetValue<T5>(j));
        }
    }
    void IRecordTrait<(T1, T2, T3, T4, T5)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5)> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5)> source)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        var ser5 = series[4];
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1.SetValue<T1>(j, source[i].Item1);
            ser2.SetValue<T2>(j, source[i].Item2);
            ser3.SetValue<T3>(j, source[i].Item3);
            ser4.SetValue<T4>(j, source[i].Item4);
            ser5.SetValue<T5>(j, source[i].Item5);
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

    public static IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex switch {
            0 => new MutableSeries<T1>(columnName, initialCapacity),
            1 => new MutableSeries<T2>(columnName, initialCapacity),
            2 => new MutableSeries<T3>(columnName, initialCapacity),
            3 => new MutableSeries<T4>(columnName, initialCapacity),
            4 => new MutableSeries<T5>(columnName, initialCapacity),
            5 => new MutableSeries<T6>(columnName, initialCapacity),
            _ => throw new ArgumentOutOfRangeException(nameof(columnIndex)),
        };
    IMutableSeries IRecordTrait.CreateSeries(int columnIndex,int initialCapacity, string columnName)
        => CreateSeries(columnIndex, initialCapacity, columnName);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5, T6)> destination)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        var ser5 = series[4];
        var ser6 = series[5];
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1.GetValue<T1>(j), ser2.GetValue<T2>(j), ser3.GetValue<T3>(j), ser4.GetValue<T4>(j), ser5.GetValue<T5>(j), ser6.GetValue<T6>(j));
        }
    }
    void IRecordTrait<(T1, T2, T3, T4, T5, T6)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5, T6)> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6)> source)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        var ser5 = series[4];
        var ser6 = series[5];
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1.SetValue<T1>(j, source[i].Item1);
            ser2.SetValue<T2>(j, source[i].Item2);
            ser3.SetValue<T3>(j, source[i].Item3);
            ser4.SetValue<T4>(j, source[i].Item4);
            ser5.SetValue<T5>(j, source[i].Item5);
            ser6.SetValue<T6>(j, source[i].Item6);
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

    public static IMutableSeries CreateSeries(int columnIndex, int initialCapacity, string columnName)
        => columnIndex switch {
            0 => new MutableSeries<T1>(columnName, initialCapacity),
            1 => new MutableSeries<T2>(columnName, initialCapacity),
            2 => new MutableSeries<T3>(columnName, initialCapacity),
            3 => new MutableSeries<T4>(columnName, initialCapacity),
            4 => new MutableSeries<T5>(columnName, initialCapacity),
            5 => new MutableSeries<T6>(columnName, initialCapacity),
            6 => new MutableSeries<T7>(columnName, initialCapacity),
            _ => throw new ArgumentOutOfRangeException(nameof(columnIndex)),
        };
    IMutableSeries IRecordTrait.CreateSeries(int columnIndex,int initialCapacity, string columnName)
        => CreateSeries(columnIndex, initialCapacity, columnName);

    public static void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5, T6, T7)> destination)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        var ser5 = series[4];
        var ser6 = series[5];
        var ser7 = series[6];
        for(var i = 0; i < destination.Length; i++)
        {
            var j = rowIndex + i;
            destination[i] = (ser1.GetValue<T1>(j), ser2.GetValue<T2>(j), ser3.GetValue<T3>(j), ser4.GetValue<T4>(j), ser5.GetValue<T5>(j), ser6.GetValue<T6>(j), ser7.GetValue<T7>(j));
        }
    }
    void IRecordTrait<(T1, T2, T3, T4, T5, T6, T7)>.ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<(T1, T2, T3, T4, T5, T6, T7)> destination)
        => ReadFromSeries(series, rowIndex, destination);

    public static void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6, T7)> source)
    {
        var ser1 = series[0];
        var ser2 = series[1];
        var ser3 = series[2];
        var ser4 = series[3];
        var ser5 = series[4];
        var ser6 = series[5];
        var ser7 = series[6];
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1.SetValue<T1>(j, source[i].Item1);
            ser2.SetValue<T2>(j, source[i].Item2);
            ser3.SetValue<T3>(j, source[i].Item3);
            ser4.SetValue<T4>(j, source[i].Item4);
            ser5.SetValue<T5>(j, source[i].Item5);
            ser6.SetValue<T6>(j, source[i].Item6);
            ser7.SetValue<T7>(j, source[i].Item7);
        }
    }
    void IRecordTrait<(T1, T2, T3, T4, T5, T6, T7)>.WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6, T7)> source)
        => WriteToSeries(series, rowIndex, source);
}

