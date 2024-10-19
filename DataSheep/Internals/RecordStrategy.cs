using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DataSheep;

internal interface IRecordStrategy<TRecord>
    where TRecord : ITuple
{
    public static abstract int ColumnCount { get; }
    public static abstract IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames);
    public static abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination);
    public static abstract void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<TRecord> source);
}


internal static class RecordStrategy
{
    public static MutableDataFrame<TRecord> CreateMutableDataFrame<TRecord>(IMutableSeries[] series)
        where TRecord : ITuple
        => Cache<TRecord>.CreateWithColumns(series);


    public static MutableDataFrame<TRecord> CreateMutableDataFrame<TRecord>(int initialCapacity, IReadOnlyList<string> columnNames)
        where TRecord : ITuple
        => Cache<TRecord>.CreatePlane(initialCapacity, columnNames);

    /// <summary>
    /// Determines <paramref name="type"/> is an implementation of <see cref="IDataRecord{TSelf}"/>.
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public static bool IsDataRecordType(Type type)
        => type
        .GetInterfaces()
        .Any(ifType => ReflectionHelpers.IsClosedGenericOf(ifType, typeof(IDataRecord<>)));

    public static Type? GetValueTupleStrategyType(Type tupleType)
    {
        if(!tupleType.IsGenericType)
        {
            return null;
        }
        var genericDef = tupleType.GetGenericTypeDefinition();
        if(genericDef.Module != typeof(ValueTuple<>).Module)
        {
            return null;
        }
        if(genericDef.FullName?.Split('`')[0] != typeof(ValueTuple<>).FullName?.Split('`')[0])
        {
            return null;
        }
        var typeArgs = tupleType.GetGenericArguments();
        return typeArgs.Length switch
        {
            1 => typeof(ValueTupleStrategy<>).MakeGenericType(typeArgs),
            2 => typeof(ValueTupleStrategy<,>).MakeGenericType(typeArgs),
            3 => typeof(ValueTupleStrategy<,,>).MakeGenericType(typeArgs),
            4 => typeof(ValueTupleStrategy<,,,>).MakeGenericType(typeArgs),
            5 => typeof(ValueTupleStrategy<,,,,>).MakeGenericType(typeArgs),
            6 => typeof(ValueTupleStrategy<,,,,,>).MakeGenericType(typeArgs),
            7 => typeof(ValueTupleStrategy<,,,,,,>).MakeGenericType(typeArgs),
            8 when GetValueTupleStrategyType(typeArgs[^1]) is { } rest => typeof(ValueTupleStrategy<,,,,,,,,>).MakeGenericType([.. typeArgs, rest]),
            _ => null,
        };
    }
}

file static class Cache<TRecord>
    where TRecord : ITuple
{
    public static Func<IMutableSeries[], MutableDataFrame<TRecord>> CreateWithColumns { get; }
    public static Func<int, IReadOnlyList<string>, MutableDataFrame<TRecord>> CreatePlane { get; }

    static Cache()
    {
        static Type? getDataFrameType()
        {
            Type strategyType;
            if(RecordStrategy.IsDataRecordType(typeof(TRecord)))
            {
                strategyType = typeof(DataRecordStrategy<>).MakeGenericType(typeof(TRecord));
            }
            else if(RecordStrategy.GetValueTupleStrategyType(typeof(TRecord)) is Type _strategyType)
            {
                strategyType = _strategyType;
            }
            else
            {
                return null;
            }
            return typeof(MutableDataFrame<,>).MakeGenericType([typeof(TRecord), strategyType]);
        }

        static ConstructorInfo getColumnsConstructor(Type dfType)
        {
            return dfType.GetConstructor([typeof(IMutableSeries[])])!;
        }

        static ConstructorInfo getPlaneConstructor(Type dfType)
        {
            return dfType.GetConstructor([typeof(int), typeof(IReadOnlyList<string>)])!;
        }

        if(getDataFrameType() is { } dfType)
        {
            {
                var ctor = getColumnsConstructor(dfType);
                var series = Expression.Parameter(typeof(IMutableSeries[]), "series");
                CreateWithColumns = Expression.Lambda<Func<IMutableSeries[], MutableDataFrame<TRecord>>>(
                    Expression.New(ctor, [series]),
                    [series]
                    ).Compile();
            }
            {
                var ctor = getPlaneConstructor(dfType);
                var initialCapacity = Expression.Parameter(typeof(int), "initialCapacity");
                var columnNames = Expression.Parameter(typeof(IReadOnlyList<string>), "columnNames");
                CreatePlane = Expression.Lambda<Func<int, IReadOnlyList<string>, MutableDataFrame<TRecord>>>(
                    Expression.New(ctor, [initialCapacity, columnNames]),
                    [initialCapacity, columnNames]
                    ).Compile();
            }
        }
        else
        {
            CreateWithColumns = series => throw new NotImplementedException();
            CreatePlane = (rowCount, columns) => throw new NotSupportedException();
        }
    }
}
