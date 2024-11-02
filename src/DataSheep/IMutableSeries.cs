namespace DataSheep;

/// <summary>
/// Implementation of <see cref="ISeries"/> which has data entity which can mutate.
/// </summary>
public interface IMutableSeries : ISeries
{
    /// <summary> Sets the element at the specified row index. </summary>
    /// <param name="rowIndex"></param>
    /// <returns></returns>
    /// <exception cref="InvalidCastException" />
    public void SetValue<T>(int rowIndex, T value);

    /// <summary>
    /// Sets bulkly the elements at the specified range.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="source"></param>
    /// <exception cref="InvalidCastException" />
    public void SetValues<T>(int rowIndex, ReadOnlySpan<T> source);

    /// <summary>
    /// Expands this series.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="expandCount"></param>
    public void Expand(int rowIndex, int expandCount);

    /// <summary>
    /// Shrinks this series.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="shrinkCount"></param>
    public void Shrink(int rowIndex, int shrinkCount);

    /// <summary>
    /// Removes all elements in this series.
    /// </summary>
    public void Clear();
}
