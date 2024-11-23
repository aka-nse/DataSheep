using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

public class LazySeries(IRecordTrait trait, int columnIndex, ILazyEvaluator predicate)
{
    internal class Chunk
    {
        private readonly IMutableSeries _series;

        // For each `series[0]` -- `series[255]`,  presents whether the element has been evaluated or not.
        private FixedBitArray256 _isRowEvaluated;

        public int GlobalStart { get; }

        public Chunk(int start, IMutableSeries series)
        {
            GlobalStart = start;
            _series = series;
            _series.Expand(0, Math.Max(0, 256 - _series.Count));
        }

        /// <returns>
        /// If not null, it should be insert into next of this.
        /// </returns>
        public int GetValues<T>(ILazyEvaluator predicate, int localStart, Span<T> destination)
        {
            if(destination.IsEmpty)
            {
                return 0;
            }
            if((uint)_series.Count <= (uint)localStart)
            {
                return 0;
            }
            var localEnd = localStart + destination.Length;
            if(_series.Count <= localEnd)
            {
                localEnd = _series.Count;
                destination = destination[..(localEnd - localStart)];
            }
            if(!_isRowEvaluated.All(localStart..localEnd))
            {
                FillValues(predicate, localStart, destination);
            }
            if(_series.Count <= localEnd)
            {
                localEnd = _series.Count;
                destination = destination[..(localEnd - localStart)];
            }
            _series.GetValues(localStart, destination);
            return localEnd - localStart;
        }

        private void FillValues<T>(ILazyEvaluator predicate, int localStart, Span<T> destination)
        {
            var bulkSearchCursor = localStart;
            var localEnd = localStart + destination.Length;
            while(true)
            {
                var range = _isRowEvaluated.GetNextFalseChunk(ref bulkSearchCursor);
                var s = range.Start.Value;
                if(_series.Count <= s)
                {
                    break;
                }
                var e = range.End.Value;
                var l = Math.Min(e, localEnd) - s;
                if(l <= 0)
                {
                    break;
                }
                var chunkBuffer = destination.Slice(s - localStart, l);
                var evaluatedCount = predicate.EvaluateValues(s, chunkBuffer);
                _series.SetValues<T>(s, chunkBuffer[..evaluatedCount]);
                _isRowEvaluated.BulkSet(s..(s+l), true);
                if(evaluatedCount < chunkBuffer.Length)
                {
                    var tail = s + evaluatedCount;
                    _series.Shrink(tail, _series.Count - tail);
                    break;
                }
            }
        }
    }

    private readonly int _columnIndex = columnIndex;
    private readonly IRecordTrait _trait = trait;
    private readonly ILazyEvaluator _predicate = predicate;
    private readonly List<Chunk> _chunks = [];

    public void GetValues<T>(int startRowIndex, Span<T> destination)
    {
        var endRowIndex = startRowIndex + destination.Length;
        throw new NotImplementedException();
    }
}

public interface ILazyEvaluator
{
    /// <summary>
    /// Evaluates the values of the specified range and stores them in the destination.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="startRowIndex"></param>
    /// <param name="destination"></param>
    /// <returns>
    /// [<c>0 - destination.Length</c>] The number of elements that have been evaluated actually.
    /// If less than <c>destination.Length</c>, it means this evaluator reached tail of data.
    /// </returns>
    /// <exception cref="InvalidCastException"></exception>
    public int EvaluateValues<T>(int startRowIndex, Span<T> destination)
    {
        if(this is not ILazyEvaluator<T> @this)
        {
            throw new InvalidCastException();
        }
        return @this.EvaluateValues(startRowIndex, destination);
    }
}

public interface ILazyEvaluator<T> : ILazyEvaluator
{
    public int EvaluateValues(int startRowIndex, Span<T> destination);
}