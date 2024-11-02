using System.Collections;
using System.Runtime.CompilerServices;

namespace DataSheep;

partial class DataFrame
{
    private static DataFrame<TRecord>.Mutable CreateMutableInternal<TRecord>(IRecordTrait<TRecord> trait, int initialCapacity, IReadOnlyDictionary<int, string> columnNames)
    {
        var names = new ColumnNameList(columnNames, trait.DefaultColumnNames);
        var series = new IMutableSeries[trait.ColumnCount];
        for(var i = 0; i < series.Length; ++i)
        {
            series[i] = trait.CreateSeries(i, initialCapacity, names[i]);
        }
        return new(trait, series);
    }

    #region IDataRecord

    /// <summary>
    /// Creates a new immutable data frame for the specified record type whose traits is defined explicitly.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    /// <param name="records"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TRecord>.Immutable Create<TRecord>(ReadOnlySpan<TRecord> records, DataRecordOptions? options = null)
        where TRecord : IDataRecord<TRecord>
        => CreateMutable(records, options).MoveToImmutable();

    /// <summary>
    /// Creates a new immutable data frame for the specified record type whose traits is defined explicitly.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    /// <param name="records"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TRecord>.Immutable Create<TRecord>(IReadOnlyList<TRecord> records, DataRecordOptions? options = null)
        where TRecord : IDataRecord<TRecord>
        => CreateMutable(records, options).MoveToImmutable();

    /// <summary>
    /// Creates a new mutable data frame for the specified record type whose traits is defined explicitly.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    /// <param name="initialCapacity"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TRecord>.Mutable CreateMutable<TRecord>(int initialCapacity, DataRecordOptions? options = null)
        where TRecord : IDataRecord<TRecord>
        => CreateMutableInternal(TRecord.Trait, initialCapacity, (options ?? DataRecordOptions.Default).ColumnNames);

    /// <summary>
    /// Creates a new mutable data frame for the specified record type whose traits is defined explicitly.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    /// <param name="records"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TRecord>.Mutable CreateMutable<TRecord>(ReadOnlySpan<TRecord> records, DataRecordOptions? options = null)
        where TRecord : IDataRecord<TRecord>
    {
        var df = CreateMutable<TRecord>(records.Length, options);
        df.AddRecords(records);
        return df;
    }

    /// <summary>
    /// Creates a new mutable data frame for the specified record type whose traits is defined explicitly.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    /// <param name="records"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TRecord>.Mutable CreateMutable<TRecord>(IReadOnlyList<TRecord> records, DataRecordOptions? options = null)
        where TRecord : IDataRecord<TRecord>
    {
        var df = CreateMutable<TRecord>(records.Count, options);
        using var buffer = new TemporaryBuffer<TRecord>(records.Count);
        for(var i = 0; i < records.Count; ++i)
        {
            buffer.Span[i] = records[i];
        }
        df.AddRecords(buffer.Span);
        return df;
    }

    #endregion

    #region ValueTuple

    /// <summary>
    /// Creates a new immutable data frame for the specified value tuple record.
    /// </summary>
    /// <typeparam name="TTuple"></typeparam>
    /// <param name="records"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TTuple>.Immutable Create<TTuple>(ReadOnlySpan<TTuple> records, ValueTupleOptions? options = null)
        where TTuple : struct,
            IStructuralComparable,
            IStructuralEquatable,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            ITuple
        => CreateMutable(records, options).MoveToImmutable();

    /// <summary>
    /// Creates a new immutable data frame for the specified value tuple record.
    /// </summary>
    /// <typeparam name="TTuple"></typeparam>
    /// <param name="records"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TTuple>.Immutable Create<TTuple>(IReadOnlyList<TTuple> records, ValueTupleOptions? options = null)
        where TTuple : struct,
            IStructuralComparable,
            IStructuralEquatable,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            ITuple
        => CreateMutable(records, options).MoveToImmutable();

    /// <summary>
    /// Creates a new mutable data frame for the specified value tuple record.
    /// </summary>
    /// <typeparam name="TTuple"></typeparam>
    /// <param name="initialCapacity"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TTuple>.Mutable CreateMutable<TTuple>(int initialCapacity, ValueTupleOptions? options = null)
        where TTuple : struct,
            IStructuralComparable,
            IStructuralEquatable,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            ITuple
        => CreateMutableInternal(RecordTrait.Cache<TTuple>.Trait, initialCapacity, (options ?? ValueTupleOptions.Default).ColumnNames);

    /// <summary>
    /// Creates a new mutable data frame for the specified value tuple record.
    /// </summary>
    /// <typeparam name="TTuple"></typeparam>
    /// <param name="records"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TTuple>.Mutable CreateMutable<TTuple>(ReadOnlySpan<TTuple> records, ValueTupleOptions? options = null)
        where TTuple : struct,
            IStructuralComparable,
            IStructuralEquatable,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            ITuple
    {
        var df = CreateMutable<TTuple>(records.Length, options);
        df.AddRecords(records);
        return df;
    }

    /// <summary>
    /// Creates a new mutable data frame for the specified value tuple record.
    /// </summary>
    /// <typeparam name="TTuple"></typeparam>
    /// <param name="records"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static DataFrame<TTuple>.Mutable CreateMutable<TTuple>(IReadOnlyList<TTuple> records, ValueTupleOptions? options = null)
        where TTuple : struct,
            IStructuralComparable,
            IStructuralEquatable,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            ITuple
    {
        var df = CreateMutable<TTuple>(records.Count, options);
        using var buffer = new TemporaryBuffer<TTuple>(records.Count);
        for(var i = 0; i < records.Count; ++i)
        {
            buffer.Span[i] = records[i];
        }
        df.AddRecords(buffer.Span);
        return df;
    }

    #endregion
}

/// <summary>
/// Provies options to be used with creation of <see cref="DataFrame"/>.
/// </summary>
/// <param name="ColumnNames"></param>
public record class DataRecordOptions(
    IReadOnlyDictionary<int, string> ColumnNames)
{
    /// <summary>
    /// Gets a default instance of <see cref="DataRecordOptions"/>.
    /// </summary>
    public static DataRecordOptions Default { get; }
        = new([]);

    /// <summary>
    /// Creates a new instance of <see cref="DataRecordOptions"/>.
    /// </summary>
    /// <param name="ColumnNames"></param>
    public DataRecordOptions(IReadOnlyList<string> ColumnNames)
        : this(DataFrame.OptionsHelper.MapColumnNamesFromList(ColumnNames))
    {
    }
}

/// <summary>
/// Provies options to be used with creation of <see cref="DataFrame"/>.
/// </summary>
/// <param name="ColumnNames"></param>
public record class ValueTupleOptions(
    IReadOnlyDictionary<int, string> ColumnNames)
{
    /// <summary>
    /// Gets a default instance of <see cref="ValueTupleOptions"/>.
    /// </summary>
    public static ValueTupleOptions Default { get; }
        = new([]);

    /// <summary>
    /// Creates a new instance of <see cref="ValueTupleOptions"/>.
    /// </summary>
    /// <param name="ColumnNames"></param>
    public ValueTupleOptions(IReadOnlyList<string> ColumnNames)
        : this(DataFrame.OptionsHelper.MapColumnNamesFromList(ColumnNames))
    {
    }
}
