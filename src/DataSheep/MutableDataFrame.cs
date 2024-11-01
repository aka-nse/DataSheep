using System.Collections;

namespace DataSheep;

/// <summary>
/// Provides utilities for <see cref="MutableDataFrame{TRecord}"/>.
/// </summary>
public static partial class MutableDataFrame
{
}

/// <summary>
/// The mutable table whose each row can be mapped with <typeparamref name="TRecord"/>.
/// </summary>
/// <typeparam name="TRecord"></typeparam>
/// <param name="trait"></param>
/// <param name="series"></param>
public sealed class MutableDataFrame<TRecord>(IRecordTrait<TRecord> trait, IMutableSeries[] series)
    : IDataFrame<TRecord>
{
    private readonly TRecord[] _buffer = new TRecord[1];
    private int _generation;

    public int RowCount => Series[0].Count;

    internal IMutableSeries[] Series
        => _series ?? throw new ObjectDisposedException("");
    internal IMutableSeries[]? _series = series;

    public TRecord this[int rowIndex]
    {
        get
        {
            trait.ReadFromSeries(Series, rowIndex, _buffer);
            return _buffer[0];
        }
        set
        {
            _buffer[0] = value;
            trait.WriteToSeries(Series, rowIndex, _buffer);
            ++_generation;
        }
    }

    public IEnumerable<TRecord> AsEnumerable()
        => new Enumerable(this);

    private IMutableSeries[] CopySeries()
    {
        var newSeries = new IMutableSeries[Series.Length];
        for(var j = 0; j < newSeries.Length; ++j)
        {
            newSeries[j] = Series[j].Clone();
        }
        return newSeries;
    }

    public DataFrame<TRecord>.Mutable CopyAsMutable()
        => new(trait, CopySeries());


    public DataFrame<TRecord>.Immutable CopyAsImmutable()
        => new(trait, CopySeries());

    public DataFrame<TRecord>.Immutable MoveAsImmutable()
        => new(trait, Series);

    public void GetRecords(int rowIndex, Span<TRecord> destination)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rowIndex, nameof(rowIndex));
        ArgumentOutOfRangeException.ThrowIfLessThan(destination.Length, RowCount - rowIndex, nameof(destination));
        trait.ReadFromSeries(Series, rowIndex, destination);
    }

    public TRecord[] GetRecords()
    {
        var records = new TRecord[RowCount];
        GetRecords(0, records);
        return records;
    }

    public void SetRecords(int rowIndex, ReadOnlySpan<TRecord> source)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rowIndex, nameof(rowIndex));
        ArgumentOutOfRangeException.ThrowIfLessThan(source.Length, RowCount - rowIndex, nameof(source));
        trait.WriteToSeries(Series, rowIndex, source);
        ++_generation;
    }

    public void AddRecord(TRecord record)
        => InsertRecord(RowCount, record);

    public void AddRecords(ReadOnlySpan<TRecord> records)
        => InsertRecords(RowCount, records);

    public void InsertRecord(int rowIndex, TRecord record)
    {
        _buffer[0] = record;
        InsertRecords(rowIndex, _buffer);
    }

    public void InsertRecords(int rowIndex, ReadOnlySpan<TRecord> records)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan((uint)rowIndex, (uint)RowCount, nameof(rowIndex));
        foreach(var series in Series)
        {
            series.Expand(rowIndex, records.Length);
        }
        trait.WriteToSeries(Series, rowIndex, records);
        ++_generation;
    }

    public void RemoveRecord(int rowIndex)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)rowIndex, (uint)RowCount, nameof(rowIndex));
        foreach(var series in Series)
        {
            series.Shrink(rowIndex, 1);
        }
        ++_generation;
    }

    public void RemoveRecords(int rowIndex, int rowCount)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)rowIndex, (uint)RowCount, nameof(rowIndex));
        ArgumentOutOfRangeException.ThrowIfNegative(rowCount, nameof(rowCount));
        foreach(var series in Series)
        {
            series.Shrink(rowIndex, rowCount);
        }
        ++_generation;
    }

    public void Clear()
    {
        foreach(var series in Series)
        {
            series.Clear();
        }
        ++_generation;
    }

    private class Enumerable : IEnumerable<TRecord>, IEnumerator<TRecord>
    {
        private Enumerable? _nextEnumerator;
        private readonly MutableDataFrame<TRecord> _owner;
        private int _generation;
        private int _rowIndex = -1;

        public Enumerable(MutableDataFrame<TRecord> owner)
        {
            _owner = owner;
            _nextEnumerator = this;
        }

        public TRecord Current => (uint)_rowIndex < (uint)_owner.RowCount
            ? _owner[_rowIndex]
            : throw new InvalidOperationException();
        object IEnumerator.Current => Current!;


        public bool MoveNext()
        {
            if(_generation != _owner._generation)
            {
                throw new InvalidOperationException();
            }
            ++_rowIndex;
            return _rowIndex < _owner.RowCount;
        }

        public void Dispose() { }

        public void Reset() => _rowIndex = -1;

        public IEnumerator<TRecord> GetEnumerator()
        {
            if(Interlocked.Exchange(ref _nextEnumerator, null) is { } nextEnumerator)
            {
                nextEnumerator._generation = _owner._generation;
                return nextEnumerator;
            }
            return new Enumerable(_owner) { _generation = _owner._generation };
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}