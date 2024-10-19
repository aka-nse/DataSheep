using System.Collections;
using System.Runtime.CompilerServices;

namespace DataSheep;

/// <summary>
/// The mirror type of <see cref="DataFrame"/> which can mutate.
/// </summary>
public abstract partial class MutableDataFrame
{
    /// <summary> Gets column names. </summary>
    public abstract IReadOnlyList<string> ColumnNames { get; }

    /// <summary> Gets a number of rows. </summary>
    public abstract int RowCount { get; }

    /// <summary> Gets an iterable object of the rows. </summary>
    /// <returns></returns>
    public abstract IEnumerable AsEnumerable();
}

/// <summary>
/// The mirror type of <see cref="DataFrame{TRecord}"/> which can mutate.
/// </summary>
public abstract class MutableDataFrame<TRecord> : MutableDataFrame
    where TRecord : ITuple
{
    private sealed class ColumnNameList(MutableDataFrame<TRecord> owner) : IReadOnlyList<string>
    {
        private readonly MutableDataFrame<TRecord> _owner = owner;

        public int Count => _owner._series.Length;

        public string this[int index] => _owner._series[index].ColumnName;

        public IEnumerator<string> GetEnumerator()
        {
            foreach(var series in _owner._series)
            {
                yield return series.ColumnName;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }

    private sealed class Enumerable(MutableDataFrame<TRecord> owner) : IEnumerable<TRecord>
    {
        private readonly MutableDataFrame<TRecord> _owner = owner;

        public IEnumerator<TRecord> GetEnumerator()
        {
            var rowCount = _owner.RowCount;
            var generation = _owner._generation;
            for(var i = 0; i < rowCount; ++i)
            {
                if(generation != _owner._generation)
                {
                    throw new InvalidOperationException();
                }
                yield return _owner[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private uint _generation;
    private IMutableSeries[] _series;

    private readonly TRecord[] _temporaryBuffer = new TRecord[1];

    /// <inheritdoc/>
    public sealed override IReadOnlyList<string> ColumnNames { get; }

    /// <inheritdoc/>
    public sealed override int RowCount => _series.FirstOrDefault()?.Count ?? 0;

    /// <summary>
    /// Gets or sets the row at the specified row index.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <returns></returns>
    /// <exception cref="IndexOutOfRangeException"></exception>
    public TRecord this[int rowIndex]
    {
        get
        {
            if((uint)rowIndex >= (uint)RowCount)
            {
                throw new IndexOutOfRangeException();
            }
            ReadFromSeries(_series, rowIndex, _temporaryBuffer);
            return _temporaryBuffer[0];
        }
        set
        {
            if((uint)rowIndex >= (uint)RowCount)
            {
                throw new IndexOutOfRangeException();
            }
            _temporaryBuffer[0] = value;
            WriteToSeries(_series, rowIndex, _temporaryBuffer);
            ++_generation;
        }
    }

    private protected MutableDataFrame(IMutableSeries[] series)
    {
        _series = series;
        ColumnNames = new ColumnNameList(this);
    }

    private protected abstract DataFrame<TRecord> ToImmutable(ISeries[] series);

    /// <summary>
    /// Destroys this instance and gets immutable version of this.
    /// </summary>
    /// <returns></returns>
    public DataFrame<TRecord> MoveToImmutable()
    {
        var series = _series;
        _series = null!;
        return ToImmutable(series);
    }

    /// <summary>
    /// Gets immutable version of this without destruction.
    /// </summary>
    /// <returns></returns>
    public DataFrame<TRecord> CopyToImmutable()
    {
        var series = new ISeries[_series.Length];
        for(var i = 0; i < series.Length; ++i)
        {
            series[i] = _series[i].Clone();
        }
        return ToImmutable(series);
    }

    protected abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination);

    protected abstract void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<TRecord> source);

    /// <summary>
    /// Adds a new record at the end.
    /// </summary>
    /// <param name="record"></param>
    public void Add(TRecord record)
        => Insert(RowCount, record);

    /// <summary>
    /// Adds new records at the end.
    /// </summary>
    /// <param name="records"></param>
    public void AddRange(ReadOnlySpan<TRecord> records)
        => InsertRange(RowCount, records);

    /// <summary>
    /// Inserts a new record at the specified position.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="record"></param>
    public void Insert(int rowIndex, TRecord record)
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Expand(rowIndex, 1);
        }
        _temporaryBuffer[0] = record;
        WriteToSeries(_series, rowIndex, _temporaryBuffer);
        ++_generation;
    }

    /// <summary>
    /// Inserts new records at the specified position.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="records"></param>
    public void InsertRange(int rowIndex, ReadOnlySpan<TRecord> records)
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Expand(rowIndex, records.Length);
        }
        WriteToSeries(_series, rowIndex, records);
        ++_generation;
    }

    /// <summary>
    /// Removes a record at the specified position.
    /// </summary>
    /// <param name="rowIndex"></param>
    public void RemoveAt(int rowIndex)
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Shrink(rowIndex, 1);
        }
        ++_generation;
    }

    /// <summary>
    /// Removes records at the specified range.
    /// </summary>
    /// <param name="rowIndex"></param>
    public void RemoveRange(int rowIndex, int count)
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Shrink(rowIndex, count);
        }
        ++_generation;
    }

    /// <summary>
    /// Removes all records.
    /// </summary>
    public void Clear()
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Clear();
        }
        ++_generation;
    }

    /// <inheritdoc/>
    public override IEnumerable<TRecord> AsEnumerable()
        => new Enumerable(this);
}

internal class MutableDataFrame<TRecord, TStrategy>(IMutableSeries[] series)
    : MutableDataFrame<TRecord>(series)
    where TRecord : ITuple
    where TStrategy : struct, IRecordStrategy<TRecord>
{
    public MutableDataFrame(int initialCapacity, IReadOnlyList<string> columnNames)
        : this(TStrategy.CreateSeriesPrefab(initialCapacity, PadWithDefault(columnNames)))
    {
    }

    private protected override DataFrame<TRecord> ToImmutable(ISeries[] series)
        => new DataFrame<TRecord, TStrategy>(series);

    protected override void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination)
        => TStrategy.ReadFromSeries(series, rowIndex, destination);

    protected override void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<TRecord> source)
        => TStrategy.WriteToSeries(series, rowIndex, source);

    private static IReadOnlyList<string> PadWithDefault(IReadOnlyList<string> columnNames)
    {
        if(columnNames.Count >= TStrategy.ColumnCount)
        {
            return columnNames;
        }
        return [
            ..columnNames,
            ..Enumerable
                .Range(columnNames.Count + 1, TStrategy.ColumnCount - columnNames.Count)
                .Select(x => $"Column {x}"),
            ];
    }
}
