using System;
using System.Runtime.CompilerServices;

namespace DataSheep.Linq;

partial class QueryableDataFrame
{
    public static DataFrame<TRecord> Where<TRecord>(
            this DataFrame<TRecord> df,
            Func<TRecord, bool> predicate,
            OverloadResolver.DataRecord resolver = default)
        where TRecord : ITuple
    {
        ArgumentNullException.ThrowIfNull(df, nameof(df));
        ArgumentNullException.ThrowIfNull(predicate, nameof(predicate));
        return new WhereDataTable<TRecord>(df, predicate);
    }
}

file class WhereDataTable<TRecord>(DataFrame<TRecord> source, Func<TRecord, bool> predicate)
    : DataFrame<TRecord>(CreateSeries(source, predicate))
    where TRecord : ITuple
{
    static ISeries[] CreateSeries(DataFrame<TRecord> df, Func<TRecord, bool> predicate)
    {
        var indexMap = new WhereIndexMap<TRecord>(df, predicate);
        var series = new ISeries[df.Series.Length];
        for(var i = 0; i <series.Length; ++i)
        {
            series[i] = new IndexMapSeries(indexMap, series[i]);
        }
        return series;
    }

    private readonly DataFrame<TRecord> _source = source;

    protected override void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<TRecord> destination)
    {
        throw new NotImplementedException();
    }
}

file class WhereIndexMap<TRecord>(DataFrame<TRecord> source, Func<TRecord, bool> predicate)
    : IndexMap
    where TRecord : ITuple
{
    private readonly DataFrame<TRecord> _source = source;
    private readonly Func<TRecord, bool> _predicate = predicate;
    private readonly List<int> _indices = new();
    private int _cursor = -1;

    public override int RowCount
    {
        get
        {
            while(MoveNext()) { }
            return _indices.Count;
        }
    }

    public override int this[int rowIndex]
    {
        get
        {
            var x = GetMappedIndex(rowIndex);
            return x >= 0
                ? x
                : throw new ArgumentOutOfRangeException(nameof(rowIndex));
        }
    }

    private int GetMappedIndex(int rowIndex)
    {
        do
        {
            if(rowIndex < _indices.Count)
            {
                return _indices[rowIndex];
            }
        } while(MoveNext());
        return -1;
    }

    private bool MoveNext()
    {
        if(_cursor >= _indices.Count)
        {
            return false;
        }
        while(true)
        {
            ++_cursor;
            if(_cursor >= _indices.Count)
            {
                return false;
            }
            if(_predicate(_source[_cursor]))
            {
                _indices.Add(_cursor);
                return true;
            }
        }
    }
}
