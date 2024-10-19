using System.Numerics;

namespace DataSheep;

/// <summary>
/// Implementation of <see cref="ISeries"/> which has data entity which can mutate.
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class MutableSeries<T>
    : IMutableSeries, ISeries<T>
{
    private T[] _array;

    /// <inheritdoc/>
    public string ColumnName { get; }

    /// <inheritdoc/>
    public int Count => _count;
    private int _count = 0;

    /// <summary>
    /// Gets the maximum amount this series can be extended without reallocation.
    /// </summary>
    public int Capacity => _array.Length;

    /// <summary> Gets or sets the element at the specified row index. </summary>
    /// <param name="rowIndex"></param>
    /// <returns></returns>
    public T this[int rowIndex]
    {
        get => (uint)rowIndex < (uint)Count ? _array[rowIndex] : throw new IndexOutOfRangeException();
        set => _array[rowIndex] = (uint)rowIndex < (uint)Count ? value : throw new IndexOutOfRangeException();
    }
    object? IMutableSeries.this[int rowIndex]
    {
        get => this[rowIndex];
        set => this[rowIndex] = value is T tvalue ? tvalue : throw new InvalidCastException();
    }

    internal MutableSeries(string columnName, int initialMinimumCapacity)
    {
        ColumnName = columnName;
        _array = new T[Math.Max(256, BitOperations.RoundUpToPowerOf2((uint)initialMinimumCapacity))];
    }

    internal MutableSeries(string columnName, ReadOnlySpan<T> initialValues)
    {
        ColumnName = columnName;
        _array = new T[Math.Max(256, BitOperations.RoundUpToPowerOf2((uint)initialValues.Length))];
        initialValues.CopyTo(_array);
        _count = initialValues.Length;
    }

    /// <inheritdoc/>
    public MutableSeries<T1> As<T1>()
        => this is MutableSeries<T1> typed
            ? typed
            : throw new InvalidCastException();

    /// <inheritdoc/>
    ISeries<T1> ISeries.As<T1>() => As<T1>();

    /// <inheritdoc/>
    public void Expand(int rowIndex, int expandCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(expandCount, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan((uint)rowIndex, (uint)Count);
        var newCount = Count + expandCount;
        ExtendBufferIfNeed(newCount);
        if(rowIndex < Count)
        {
            _array.AsSpan(rowIndex, Count - rowIndex).CopyTo(_array.AsSpan(rowIndex + expandCount));
        }
        _count = newCount;
    }

    /// <inheritdoc/>
    public void Shrink(int rowIndex, int shrinkCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(rowIndex, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan((uint)shrinkCount, (uint)Count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(rowIndex + shrinkCount, Count);
        var newCount = Count - shrinkCount;
        if(rowIndex + shrinkCount < Count)
        {
            _array.AsSpan(rowIndex + shrinkCount, Count - rowIndex - shrinkCount).CopyTo(_array.AsSpan(rowIndex));
        }
        _count = newCount;
    }

    /// <summary>
    /// Adds a new element at the last.
    /// </summary>
    /// <param name="item"></param>
    public void Add(T item)
        => Insert(Count, item);

    /// <summary>
    /// Adds new elements at the last.
    /// </summary>
    /// <param name="items"></param>
    public void AddRange(ReadOnlySpan<T> items)
        => InsertRange(Count, items);

    /// <summary>
    /// Inserts a new element at the specified position.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="item"></param>
    public void Insert(int rowIndex, T item)
    {
        Expand(rowIndex, 1);
        _array[rowIndex] = item;
    }

    /// <summary>
    /// Inserts new elements at the specified position.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="items"></param>
    public void InsertRange(int rowIndex, ReadOnlySpan<T> items)
    {
        Expand(rowIndex, items.Length);
        items.CopyTo(_array.AsSpan(rowIndex));
    }

    /// <summary>
    /// Removes an element at the specified position.
    /// </summary>
    /// <param name="rowIndex"></param>
    public void RemoveAt(int rowIndex)
        => Shrink(rowIndex, 1);

    /// <summary>
    /// Removes elements at the specified range.
    /// </summary>
    /// <param name="rowIndex"></param>
    public void RemoveRange(int rowIndex, int count)
        => Shrink(rowIndex, count);

    /// <inheritdoc />
    public void Clear()
    {
        _array.AsSpan().Clear();
        _count = 0;
    }

    /// <inheritdoc />
    public void GetValues(int rowIndex, Span<T> destination)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(rowIndex, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(rowIndex + destination.Length, Count);
        _array.AsSpan(rowIndex, destination.Length).CopyTo(destination);
    }

    /// <summary>
    /// Sets bulkly the elements at the specified range.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="source"></param>
    public void SetValues(int rowIndex, ReadOnlySpan<T> source)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(rowIndex, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(rowIndex + source.Length, Count);
        source.CopyTo(_array.AsSpan(rowIndex, source.Length));
    }

    /// <inheritdoc />
    public MutableSeries<T> Clone()
        => new(ColumnName, _array);

    IMutableSeries ISeries.Clone()
        => Clone();

    private void ExtendBufferIfNeed(int newCount)
    {
        if(_array.Length < newCount)
        {
            var newArray = new T[BitOperations.RoundUpToPowerOf2((uint)newCount)];
            _array.CopyTo(newArray.AsSpan());
            _array = newArray;
        }
    }
}
