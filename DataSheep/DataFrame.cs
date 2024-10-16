using System.Collections;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace DataSheep;

public abstract partial class DataFrame
{
    public abstract IReadOnlyList<string> ColumnNames { get; }
    public abstract int RowCount { get; }
    public abstract IEnumerable AsEnumerable();
}

public abstract class DataFrame<TRecord> : DataFrame
    where TRecord : ITuple
{
    private sealed class ColumnNameList(DataFrame<TRecord> owner) : IReadOnlyList<string>
    {
        private readonly DataFrame<TRecord> _owner = owner;

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

    private sealed class Enumerable(DataFrame<TRecord> owner) : IEnumerable<TRecord>
    {
        private readonly DataFrame<TRecord> _owner = owner;

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
    private readonly ISeries[] _series;

    private readonly TRecord[] _temporaryBuffer = new TRecord[1];

    public sealed override IReadOnlyList<string> ColumnNames { get; }

    public sealed override int RowCount => _series.FirstOrDefault()?.Count ?? 0;

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

    private protected DataFrame(ISeries[] series)
    {
        _series = series;
        ColumnNames = new ColumnNameList(this);
    }

    protected abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination);

    protected abstract void WriteToSeries(ReadOnlySpan<ISeries> series, int rowIndex, ReadOnlySpan<TRecord> source);

    public void Add(TRecord record)
        => Insert(RowCount, record);

    public void AddRange(ReadOnlySpan<TRecord> records)
        => InsertRange(RowCount, records);

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

    public void InsertRange(int rowIndex, ReadOnlySpan<TRecord> records)
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Expand(rowIndex, records.Length);
        }
        WriteToSeries(_series, rowIndex, records);
        ++_generation;
    }

    public void RemoveAt(int rowIndex)
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Shrink(rowIndex, 1);
        }
        ++_generation;
    }

    public void RemoveRange(int rowIndex, int count)
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Shrink(rowIndex, count);
        }
        ++_generation;
    }

    public void Clear()
    {
        for(var i = 0; i < _series.Length; ++i)
        {
            _series[i].Clear();
        }
        ++_generation;
    }

    public override IEnumerable<TRecord> AsEnumerable()
        => new Enumerable(this);
}