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
    public ISeries<T> As<T>();

    /// <summary> Clones data into new series. </summary>
    /// <returns></returns>
    public IMutableSeries Clone();
}

/// <summary>
/// Provides a concrete surface of column for <see cref="DataFrame{TRecord}"/>.
/// </summary>
/// <typeparam name="T"></typeparam>
public interface ISeries<T> : ISeries
{
    /// <summary> Gets the element at the specified row index. </summary>
    /// <param name="rowIndex"></param>
    /// <returns></returns>
    public T this[int rowIndex] { get; }

    /// <summary>
    /// Gets bulkly the elements at the specified range.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="destination"></param>
    public void GetValues(int rowIndex, Span<T> destination);

    /// <inheritdoc/>
    public new MutableSeries<T> Clone();
}
