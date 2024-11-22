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

        public int GlobalStart { get; }

        // For each `series[0]` -- `series[255]`,  presents whether the element has been evaluated or not.
        private FixedBitArray256 _IsRowEvaluated;

        public Chunk(int start, IMutableSeries series)
        {
            GlobalStart = start;
            _series = series;
            _series.Expand(0, Math.Max(0, 256 - _series.Count));
        }

        /// <returns>
        /// If not null, it should be insert into next of this.
        /// </returns>
        public void GetValues<T>(ILazyEvaluator predicate, int localStart, Span<T> destination)
        {
            var localEnd = localStart + destination.Length;
            if(!_IsRowEvaluated.All(localStart..localEnd))
            {
                FillValues(predicate, localStart, destination);
            }
            _series.GetValues(localStart, destination);
            return;
        }

        private void FillValues<T>(ILazyEvaluator predicate, int localStart, Span<T> destination)
        {
            var bulkSearchCursor = localStart;
            var localEnd = localStart + destination.Length;
            while(true)
            {
                var range = _IsRowEvaluated.GetNextFalseChunk(ref bulkSearchCursor);
                var s = range.Start.Value;
                var e = range.End.Value;
                var l = Math.Min(e, localEnd) - s;
                if(l <= 0)
                {
                    break;
                }
                var chunkBuffer = destination.Slice(s - localStart, l);
                predicate.EvaluateValues(s, chunkBuffer);
                _series.SetValues<T>(s, chunkBuffer);
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
    public void EvaluateValues<T>(int startRowIndex, Span<T> destination)
    {
        if(this is not ILazyEvaluator<T> @this)
        {
            throw new InvalidCastException();
        }
        @this.EvaluateValues(startRowIndex, destination);
    }
}

public interface ILazyEvaluator<T> : ILazyEvaluator
{
    public void EvaluateValues(int startRowIndex, Span<T> destination);
}