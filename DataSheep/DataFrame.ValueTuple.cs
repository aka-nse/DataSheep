using System.Collections;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DataSheep;

partial class DataFrame
{
    public static DataFrame<TTuple> Create<TTuple>(int initialCapacity = 256, params string[] columnNames)
        where TTuple : struct,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            IStructuralComparable,
            IStructuralEquatable,
            ITuple
        => ValueTupleStrategy<TTuple>.Create(initialCapacity, columnNames);

}

internal static class ValueTupleStrategy<TTuple>
    where TTuple : struct,
        IComparable,
        IComparable<TTuple>,
        IEquatable<TTuple>,
        IStructuralComparable,
        IStructuralEquatable,
        ITuple
{
    public static Func<int, IReadOnlyList<string>, DataFrame<TTuple>> Create { get; }

    static ValueTupleStrategy()
    {
        static Type getStrategyType(Type tupleType)
        {
            if(!tupleType.IsGenericType)
            {
                throw new InvalidCastException();
            }
            var genericDef = tupleType.GetGenericTypeDefinition();
            if(genericDef.Module != typeof(ValueTuple<>).Module)
            {
                throw new InvalidCastException();
            }
            if(genericDef.FullName?.Split('`')[0] != typeof(ValueTuple<>).FullName?.Split('`')[0])
            {
                throw new InvalidCastException();
            }
            var typeArgs = tupleType.GetGenericArguments();
            return typeArgs.Length switch
            {
                1 => typeof(Strategy<>).MakeGenericType(typeArgs),
                2 => typeof(Strategy<,>).MakeGenericType(typeArgs),
                3 => typeof(Strategy<,,>).MakeGenericType(typeArgs),
                4 => typeof(Strategy<,,,>).MakeGenericType(typeArgs),
                5 => typeof(Strategy<,,,,>).MakeGenericType(typeArgs),
                6 => typeof(Strategy<,,,,,>).MakeGenericType(typeArgs),
                7 => typeof(Strategy<,,,,,,>).MakeGenericType(typeArgs),
                8 => typeof(Strategy<,,,,,,,,>).MakeGenericType([.. typeArgs, getStrategyType(typeArgs[^1])]),
                _ => throw new InvalidCastException(),
            };
        }

        var strategyType = getStrategyType(typeof(TTuple));
        var dataFrameType = typeof(ValueTupleDataFrame<,>).MakeGenericType([typeof(TTuple), strategyType]);
        var ctor = dataFrameType.GetConstructor([typeof(int), typeof(IReadOnlyList<string>)])!;
        var initialCapacity = Expression.Parameter(typeof(int), "initialCapacity");
        var columnNames = Expression.Parameter(typeof(IReadOnlyList<string>), "columnNames");
        Create = Expression.Lambda<Func<int, IReadOnlyList<string>, DataFrame<TTuple>>>(
            Expression.New(ctor, [initialCapacity, columnNames]),
            [initialCapacity, columnNames]
            ).Compile();
    }
}


internal interface IValueTupleStrategy<TTuple>
    where TTuple : struct,
        IComparable,
        IComparable<TTuple>,
        IEquatable<TTuple>,
        IStructuralComparable,
        IStructuralEquatable,
        ITuple
{
    public static abstract int ColumnCount { get; }
    public static abstract ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames);
    public static abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TTuple> destination);
    public static abstract void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<TTuple> source);
}

file readonly struct Strategy<T1> : IValueTupleStrategy<ValueTuple<T1>>
{
    public static int ColumnCount => 1;

    public static ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
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

    public static void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1>> source)
    {
        var ser1 = series[0].As<T1>();
        for(var i = 0; i < source.Length; i++)
        {
            var j = rowIndex + i;
            ser1[j] = source[i].Item1;
        }
    }
}

file readonly struct Strategy<T1, T2> : IValueTupleStrategy<(T1, T2)>
{
    public static int ColumnCount => 2;

    public static ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
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

    public static void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<(T1, T2)> source)
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

file readonly struct Strategy<T1, T2, T3> : IValueTupleStrategy<(T1, T2, T3)>
{
    public static int ColumnCount => 3;

    public static ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
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

    public static void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3)> source)
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

file readonly struct Strategy<T1, T2, T3, T4> : IValueTupleStrategy<(T1, T2, T3, T4)>
{
    public static int ColumnCount => 4;

    public static ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
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

    public static void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4)> source)
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

file readonly struct Strategy<T1, T2, T3, T4, T5> : IValueTupleStrategy<(T1, T2, T3, T4, T5)>
{
    public static int ColumnCount => 5;

    public static ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
            new ArraySeries<T5>(columnNames[4], initialCapacity),
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

    public static void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5)> source)
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

file readonly struct Strategy<T1, T2, T3, T4, T5, T6> : IValueTupleStrategy<(T1, T2, T3, T4, T5, T6)>
{
    public static int ColumnCount => 6;

    public static ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
            new ArraySeries<T5>(columnNames[4], initialCapacity),
            new ArraySeries<T6>(columnNames[5], initialCapacity),
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

    public static void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6)> source)
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

file readonly struct Strategy<T1, T2, T3, T4, T5, T6, T7> : IValueTupleStrategy<(T1, T2, T3, T4, T5, T6, T7)>
{
    public static int ColumnCount => 7;

    public static ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
            new ArraySeries<T5>(columnNames[4], initialCapacity),
            new ArraySeries<T6>(columnNames[5], initialCapacity),
            new ArraySeries<T7>(columnNames[6], initialCapacity),
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

    public static void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<(T1, T2, T3, T4, T5, T6, T7)> source)
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

file readonly struct Strategy<T1, T2, T3, T4, T5, T6, T7, TRest, TRestStrategy> : IValueTupleStrategy<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>
    where TRest : struct,
        IComparable,
        IComparable<TRest>,
        IEquatable<TRest>,
        IStructuralComparable,
        IStructuralEquatable,
        ITuple
    where TRestStrategy : IValueTupleStrategy<TRest>
{
    public static int ColumnCount => 7 + TRestStrategy.ColumnCount;

    public static ISeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames)
        => [
            new ArraySeries<T1>(columnNames[0], initialCapacity),
            new ArraySeries<T2>(columnNames[1], initialCapacity),
            new ArraySeries<T3>(columnNames[2], initialCapacity),
            new ArraySeries<T4>(columnNames[3], initialCapacity),
            new ArraySeries<T5>(columnNames[4], initialCapacity),
            new ArraySeries<T6>(columnNames[5], initialCapacity),
            new ArraySeries<T7>(columnNames[6], initialCapacity),
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

    public static void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> source)
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

internal class ValueTupleDataFrame<TTuple, TStrategy>(int initialCapacity, IReadOnlyList<string> columnNames)
    : DataFrame<TTuple>(TStrategy.CreateSeriesPrefab(initialCapacity, PadWithDefault(columnNames)))
    where TTuple : struct,
        IComparable,
        IComparable<TTuple>,
        IEquatable<TTuple>,
        IStructuralComparable,
        IStructuralEquatable,
        ITuple
    where TStrategy : struct, IValueTupleStrategy<TTuple>
{
    protected override void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TTuple> destination)
        => TStrategy.ReadFromSeries(series, rowIndex, destination);

    protected override void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<TTuple> source)
        => TStrategy.WriteToSeries(series, rowIndex, source);

    private static IReadOnlyList<string> PadWithDefault(IReadOnlyList<string> columnNames)
    {
        if(columnNames.Count >= TStrategy.ColumnCount)
        {
            return columnNames;
        }
        return [
            ..columnNames,
            ..Enumerable
                .Range(columnNames.Count + 1, TStrategy.ColumnCount - columnNames.Count)
                .Select(x => $"Column {x}"),
            ];
    }
}
