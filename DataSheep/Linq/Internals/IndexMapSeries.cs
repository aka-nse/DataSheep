using System.Collections.Immutable;

namespace DataSheep.Linq;

internal abstract class IndexMap
{
    public abstract int RowCount { get; }
    public abstract int this[int rowIndex] { get; }
}

internal class IndexMapSeries(IndexMap indexMap, ISeries source)
    : ISeries
{
    private sealed class TypeBounded<T>(IndexMapSeries owner) : ISeries<T>
    {
        private readonly IndexMapSeries _owner = owner;

        private ISeries<T> Source => (ISeries<T>)_owner._source;

        public T this[int rowIndex] => throw new NotImplementedException();

        public string ColumnName => ((ISeries)_owner).ColumnName;

        public int Count => ((ISeries)_owner).Count;

        public ISeries<T1> As<T1>()
            => this as ISeries<T1> ?? throw new InvalidCastException();

        public void GetValues(int rowIndex, Span<T> destination)
        {
            for(var i = 0; i < destination.Length; ++i)
            {
                destination[i] = Source[_owner._indexMap[rowIndex + i]];
            }
        }

        public MutableSeries<T> Clone()
        {
            var retval = new MutableSeries<T>(ColumnName, Count);
            for(var i = 0; i < _owner._indexMap.RowCount; ++i)
            {
                retval.Add(Source[_owner._indexMap[i]]);
            }
            return retval;
        }
        IMutableSeries ISeries.Clone() => Clone();
    }

    private readonly IndexMap _indexMap = indexMap;
    private readonly ISeries _source = source;
    private ISeries? _typeBounded;

    public int Count => _indexMap.RowCount;
    public string ColumnName => _source.ColumnName;

    public ISeries<T> As<T>()
    {
        if(_typeBounded is null)
        {
            var retval = _source is ISeries<T> typeBounded
                ? typeBounded
                : throw new InvalidCastException();
            _typeBounded = retval;
            return retval;
        }
        return _typeBounded as ISeries<T>
            ?? throw new InvalidCastException();
    }

    public IMutableSeries Clone() => Clone();
}
