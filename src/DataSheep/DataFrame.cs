using System.Collections;

namespace DataSheep;

/// <summary>
/// Provides utilities for <see cref="DataFrame{TRecord}"/>.
/// </summary>
public static partial class DataFrame
{
}

/// <summary>
/// The readonly table whose each row can be mapped with <typeparamref name="TRecord"/>.
/// </summary>
/// <typeparam name="TRecord"></typeparam>
/// <param name="trait"></param>
/// <param name="series"></param>
public partial class DataFrame<TRecord>(IRecordTrait<TRecord> trait, ISeries[] series)
    : IDataFrame<TRecord>
{
    private readonly TRecord[] _buffer = new TRecord[1];

    protected IRecordTrait<TRecord> Trait { get; } = trait;

    /// <inheritdoc/>
    public int RowCount => Series[0].Count;

    internal ISeries[] Series { get; } = series;

    /// <inheritdoc/>
    public TRecord this[int rowIndex]
    {
        get
        {
            Trait.ReadFromSeries(Series, rowIndex, _buffer);
            return _buffer[0];
        }
    }

    /// <inheritdoc/>
    public void GetRecords(int rowIndex, Span<TRecord> destination)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rowIndex, nameof(rowIndex));
        ArgumentOutOfRangeException.ThrowIfLessThan(destination.Length, RowCount - rowIndex, nameof(destination));
        Trait.ReadFromSeries(Series, rowIndex, destination);
    }

    /// <inheritdoc/>
    public TRecord[] GetRecords()
    {
        var records = new TRecord[RowCount];
        GetRecords(0, records);
        return records;
    }

    /// <inheritdoc/>
    public virtual IEnumerable<TRecord> AsEnumerable()
        => new Enumerable(this);

    /// <inheritdoc/>
    public Mutable CopyAsMutable()
    {
        var newSeries = new IMutableSeries[Series.Length];
        for(var j = 0; j < newSeries.Length; ++j)
        {
            newSeries[j] = Series[j].Clone();
        }
        return new(Trait, newSeries);
    }

    /// <inheritdoc/>
    public virtual Immutable CopyAsImmutable()
    {
        var newSeries = new IMutableSeries[Series.Length];
        for(var j = 0; j < newSeries.Length; ++j)
        {
            newSeries[j] = Series[j].Clone();
        }
        return new(Trait, newSeries);
    }


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