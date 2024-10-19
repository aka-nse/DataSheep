using System.Runtime.CompilerServices;

namespace DataSheep;

/// <summary>
/// Defines the column mapping for DataFrame{TRecord}.
/// </summary>
/// <typeparam name="TSelf"></typeparam>
public interface IDataRecord<TSelf> : ITuple
    where TSelf : IDataRecord<TSelf>
{
    /// <summary>
    /// Gets column names.
    /// </summary>
    public static abstract IReadOnlyList<string> Columns { get; }

    /// <summary>
    /// Creates a new series set which can be mapped to/from this type.
    /// </summary>
    /// <param name="initialCapacity"></param>
    /// <param name="columnNames"></param>
    /// <returns></returns>
    public static abstract IMutableSeries[] CreateSeriesInitial(int initialCapacity, IReadOnlyList<string> columnNames);

    /// <summary>
    /// Transforms a column-oriented storage for this type to row-oriented materialized sequence.
    /// </summary>
    /// <param name="series"></param>
    /// <param name="rowIndex"></param>
    /// <param name="destination"></param>
    public static abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TSelf> destination);

    /// <summary>
    /// Transforms a row-oriented materialized sequence to a column-oriented storage for this type.
    /// </summary>
    /// <param name="series"></param>
    /// <param name="rowIndex"></param>
    /// <param name="source"></param>
    public static abstract void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<TSelf> source);
}
