namespace DataSheep;

public interface ISeries
{
    public string ColumnName { get; }
    public int Count { get; }

    public void Expand(int rowIndex, int expandCount);

    public void Shrink(int rowIndex, int shrinkCount);

    public void Clear();

    public ISeries<T> As<T>();
}

public interface ISeries<T> : ISeries
{
    public T this[int rowIndex] { get; set; }

    public void Add(T item);

    public void AddRange(ReadOnlySpan<T> items);

    public void Insert(int rowIndex, T item);

    public void InsertRange(int rowIndex, ReadOnlySpan<T> items);

    public void RemoveAt(int rowIndex);

    public void RemoveRange(int rowIndex, int count);

    public void GetValues(int rowIndex, Span<T> destination);

    public void SetValues(int rowIndex, ReadOnlySpan<T> source);

    /// <summary>
    /// Forces to create a copy of this instance.
    /// </summary>
    /// <returns></returns>
    public ArraySeries<T> Copy();
}

