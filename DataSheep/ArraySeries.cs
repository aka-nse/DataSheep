using System.Numerics;

namespace DataSheep;

public sealed class ArraySeries<T>
    : ISeries<T>
{
    private T[] _array;

    public string ColumnName { get; }

    public int Capacity => _array.Length;
    public int Count { get; private set; } = 0;

    public T this[int rowIndex]
    {
        get => (uint)rowIndex < (uint)Count ? _array[rowIndex] : throw new IndexOutOfRangeException();
        set => _array[rowIndex] = (uint)rowIndex < (uint)Count ? value : throw new IndexOutOfRangeException();
    }

    public ArraySeries(string columnName, int initialMinimumCapacity)
    {
        ColumnName = columnName;
        _array = new T[Math.Max(256, BitOperations.RoundUpToPowerOf2((uint)initialMinimumCapacity))];
    }

    public ArraySeries(string columnName, ReadOnlySpan<T> initialValues)
    {
        ColumnName = columnName;
        _array = new T[Math.Max(256, BitOperations.RoundUpToPowerOf2((uint)initialValues.Length))];
        initialValues.CopyTo(_array);
        Count = initialValues.Length;
    }

    public ArraySeries<T1> As<T1>()
        => this is ArraySeries<T1> typed ? typed : throw new InvalidCastException();

    ISeries<T1> ISeries.As<T1>() => As<T1>();

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
        Count = newCount;
    }

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
        Count = newCount;
    }

    public void Add(T item)
        => Insert(Count, item);

    public void AddRange(ReadOnlySpan<T> items)
        => InsertRange(Count, items);

    public void Insert(int rowIndex, T item)
    {
        Expand(rowIndex, 1);
        _array[rowIndex] = item;
    }

    public void InsertRange(int rowIndex, ReadOnlySpan<T> items)
    {
        Expand(rowIndex, items.Length);
        items.CopyTo(_array.AsSpan(rowIndex));
    }

    public void RemoveAt(int rowIndex)
        => Shrink(rowIndex, 1);

    public void RemoveRange(int rowIndex, int count)
        => Shrink(rowIndex, count);

    public void Clear()
    {
        _array.AsSpan().Clear();
        Count = 0;
    }

    public void GetValues(int rowIndex, Span<T> destination)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(rowIndex, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(rowIndex + destination.Length, Count);
        _array.AsSpan(rowIndex, destination.Length).CopyTo(destination);
    }

    public void SetValues(int rowIndex, ReadOnlySpan<T> source)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(rowIndex, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(rowIndex + source.Length, Count);
        source.CopyTo(_array.AsSpan(rowIndex, source.Length));
    }

    public ArraySeries<T> Copy()
        => new(ColumnName, _array);

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
