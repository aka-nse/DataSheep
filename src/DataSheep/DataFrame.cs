using System.Collections;

namespace DataSheep;

/// <summary>
/// Provides utilities for <see cref="DataFrame{TRecord}"/>.
/// </summary>
public static partial class DataFrame
{
}

/// <summary>
/// The immutable table whose each row can be mapped with <typeparamref name="TRecord"/>.
/// </summary>
/// <typeparam name="TRecord"></typeparam>
/// <param name="trait"></param>
/// <param name="series"></param>
public sealed class DataFrame<TRecord>(IRecordTrait<TRecord> trait, ISeries[] series)
    : IDataFrame<TRecord>
{
    private readonly TRecord[] _buffer = new TRecord[1];

    public int RowCount => Series[0].Count;

    internal ISeries[] Series { get; } = series;

    public TRecord this[int rowIndex]
    {
        get
        {
            trait.ReadFromSeries(Series, rowIndex, _buffer);
            return _buffer[0];
        }
    }

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

    public IEnumerable<TRecord> AsEnumerable()
        => new Enumerable(this);

    public MutableDataFrame<TRecord> CopyAsMutable()
    {
        var newSeries = new IMutableSeries[Series.Length];
        for(var j = 0; j < newSeries.Length; ++j)
        {
            newSeries[j] = Series[j].Clone();
        }
        return new(trait, newSeries);
    }

    public DataFrame<TRecord> CopyAsImmutable()
        => this;

    public DataFrame<TRecord> MoveAsImmutable()
        => this;


    private class Enumerable : IEnumerable<TRecord>, IEnumerator<TRecord>
    {
        private Enumerable? _nextEnumerator;
        private readonly DataFrame<TRecord> _owner;
        private int _rowIndex = -1;

        public Enumerable(DataFrame<TRecord> owner)
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
            ++_rowIndex;
            return _rowIndex < _owner.RowCount;
        }

        public void Dispose() { }

        public void Reset() => _rowIndex = -1;

        public IEnumerator<TRecord> GetEnumerator()
        {
            if(Interlocked.Exchange(ref _nextEnumerator, null) is { } nextEnumerator)
            {
                return nextEnumerator;
            }
            return new Enumerable(_owner);
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}