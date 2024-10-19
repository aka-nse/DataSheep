namespace DataSheep;

/// <summary>
/// Implementation of <see cref="ISeries"/> which has data entity which can mutate.
/// </summary>
public interface IMutableSeries : ISeries
{
    /// <summary> Gets or sets the element at the specified row index. </summary>
    /// <param name="rowIndex"></param>
    /// <returns></returns>
    /// <exception cref="InvalidCastException" />
    public object? this[int rowIndex] { get; set; }

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

    /// <summary> Casts down into concrete series with type argument. </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public new MutableSeries<T> As<T>();
}
