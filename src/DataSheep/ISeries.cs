namespace DataSheep;

/// <summary>
/// Provides a generall surface of column for <see cref="DataFrame{TRecord}"/>.
/// </summary>
public interface ISeries
{
    /// <summary> Gets the column name. </summary>
    public string ColumnName { get; }

    /// <summary> Gets a number of elements. </summary>
    public int Count { get; }

    /// <summary> Casts down into concrete series with type argument. </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    /// <exception cref="InvalidCastException" />

    /// <summary> Clones data into new series. </summary>
    /// <returns></returns>
    public IMutableSeries Clone();

    /// <summary> Gets the element at the specified row index. </summary>
    /// <param name="rowIndex"></param>
    /// <returns></returns>
    /// <exception cref="InvalidCastException" />
    public T GetValue<T>(int rowIndex);

    /// <summary>
    /// Gets bulkly the elements at the specified range.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="destination"></param>
    /// <exception cref="InvalidCastException" />
    public void GetValues<T>(int rowIndex, Span<T> destination);
}
