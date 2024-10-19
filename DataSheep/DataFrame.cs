using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

/// <summary>
/// Base type of <see cref="DataFrame{TRecord}"/>.
/// </summary>
public abstract partial class DataFrame
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
/// Provides a immutable data table for the specified type which is internally transformed into columnar oriented.
/// </summary>
/// <typeparam name="TRecord">
/// The type to present one row.
/// </typeparam>
public abstract class DataFrame<TRecord> : DataFrame
    where TRecord : ITuple
{
    private sealed class ColumnNameList(DataFrame<TRecord> owner) : IReadOnlyList<string>
    {
        private readonly DataFrame<TRecord> _owner = owner;

        public int Count => _owner.Series.Length;

        public string this[int index] => _owner.Series[index].ColumnName;

        public IEnumerator<string> GetEnumerator()
        {
            foreach(var series in _owner.Series)
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
            for(var i = 0; i < rowCount; ++i)
            {
                yield return _owner[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    internal ISeries[] Series { get; }

    private readonly TRecord[] _temporaryBuffer = new TRecord[1];

    /// <inheritdoc/>
    public sealed override IReadOnlyList<string> ColumnNames { get; }

    /// <inheritdoc/>
    public sealed override int RowCount => Series.FirstOrDefault()?.Count ?? 0;

    /// <summary>
    /// Gets the row at the specified row index.
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
            ReadFromSeries(Series, rowIndex, _temporaryBuffer);
            return _temporaryBuffer[0];
        }
    }

    private protected DataFrame(ISeries[] series)
    {
        Series = series;
        ColumnNames = new ColumnNameList(this);
    }

    /// <summary>
    /// Gets mutable version of this without destruction.
    /// </summary>
    /// <returns></returns>
    public MutableDataFrame<TRecord> CopyToMutable()
    {
        var series = new IMutableSeries[Series.Length];
        for(var i = 0; i < series.Length; ++i)
        {
            series[i] = Series[i].Clone();
        }
        return RecordStrategy.CreateMutableDataFrame<TRecord>(series);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="series"></param>
    /// <param name="rowIndex"></param>
    /// <param name="destination"></param>
    protected abstract void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination);

    /// <inheritdoc/>
    public override IEnumerable<TRecord> AsEnumerable()
        => new Enumerable(this);
}

internal class DataFrame<TRecord, TStrategy>(ISeries[] series)
    : DataFrame<TRecord>(series)
    where TRecord : ITuple
    where TStrategy : struct, IRecordStrategy<TRecord>
{
    /// <inheritdoc />
    protected override void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination)
        => TStrategy.ReadFromSeries(series, rowIndex, destination);
}
