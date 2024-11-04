using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

public class LazySeries(IRecordTrait trait, int columnIndex, ILazyEvaluator predicate)
{
    internal class NodeItem(int start, IMutableSeries series)
    {
        // For each `Series[0]` -- `Series[255]`,  presents whether the element has been evaluated or not.
        // Since chunks starting more than 256 positions apart will build a new linked list node,
        // this flag considers only the minimum capacity of the ArraySeries.
        private FixedBitArray256 _IsRowEvaluated;

        public readonly int Start = start;

        /// <returns>
        /// If not null, it should be insert into next of this.
        /// </returns>
        public NodeItem? GetValues<T>(LazySeries owner, int localRowIndex, Span<T> destination)
        {
            FillReadyRegion<T>(localRowIndex, destination.Length, owner._predicate);

            var remain = localRowIndex + destination.Length - series.Count;
            if(remain <= 0)
            {
                // all data is already evaluated
                series.GetValues(localRowIndex, destination);
                return null;
            }
            else if(localRowIndex <= 256)
            {
                // need to evaluate new data, and it should be appended into current node
                var appendStart = series.Count;
                using var tmpBuff = new TemporaryBuffer<T>(remain);
                var buffer = tmpBuff.Span;
                owner._predicate.EvaluateValues(localRowIndex + series.Count, buffer);
                series.Expand(appendStart, remain);
                series.SetValues<T>(appendStart, buffer);
                return null;
            }
            else
            {
                // need to evaluate new data, and new node should be introduce for the data
                var newSeries = owner._trait.CreateSeries(owner._columnIndex, remain, "");
                using var tmpBuff = new TemporaryBuffer<T>(remain);
                var buffer = tmpBuff.Span;
                owner._predicate.EvaluateValues(localRowIndex + series.Count, buffer);
                newSeries.Expand(0, remain);
                newSeries.SetValues<T>(0, buffer);
                return new NodeItem(Start + localRowIndex, newSeries);
            }
        }

        private void FillReadyRegion<T>(int localRowIndex, int destLength, ILazyEvaluator predicate)
        {
            var readySize = Math.Max(0, Math.Min(256 - localRowIndex, destLength));
            if(readySize > 0 && !_IsRowEvaluated.All(localRowIndex, readySize))
            {
                // should update _IsRowEvaluated
                using var tmpBuff = new TemporaryBuffer<T>(256);
                var buffer = tmpBuff.Span;
                var start = localRowIndex;
                var remain = readySize;
                var count = 0;
                while(true)
                {
                    (start, count) = _IsRowEvaluated.GetNextFalseChunk(start, remain);
                    if(count == 0)
                    {
                        break;
                    }
                    predicate.EvaluateValues<T>(Start + start, buffer[..count]);
                    series.SetValues<T>(start, buffer[..count]);
                    _IsRowEvaluated.BulkSet(start, count, true);
                    start += count;
                    remain -= count;
                }
            }
        }
    }

    private readonly int _columnIndex = columnIndex;
    private readonly IRecordTrait _trait = trait;
    private readonly ILazyEvaluator _predicate = predicate;
    private readonly LinkedList<NodeItem> _chunks = new ();

    public void GetValues<T>(int rowIndex, Span<T> destination)
    {
        var endRowIndex = rowIndex + destination.Length;
        LinkedListNode<NodeItem> startNode = null!;
        LinkedListNode<NodeItem> endNode = null!;
        for(var node = _chunks.First; node is { }; node = node.Next)
        {
            var start = node.Value.Start;
            var end = node.Next is { } next ? next.Value.Start : int.MaxValue;
            if(start <= rowIndex && rowIndex < end)
            {
                startNode = node;
            }
            if(start < endRowIndex && endRowIndex <= end)
            {
                endNode = node;
            }
            if(startNode is { } && endNode is { })
            {
                break;
            }
        }
        startNode ??= _chunks.First!;
        endNode ??= _chunks.Last!;
        for(var node = startNode; ; )
        {
            var destStart = Math.Max(0, rowIndex - node.Value.Start);
            var seriesStart = Math.Max(0, node.Value.Start - rowIndex);
            var count = Math.Min(destination.Length - destStart, node.Next is { } next ? next.Value.Start : int.MaxValue);
            var nextNodeItem = node.Value.GetValues(this, seriesStart, destination.Slice(destStart, count));
            if(nextNodeItem is { })
            {
                node = _chunks.AddAfter(node, nextNodeItem);
            }
            if(node == endNode)
            {
                break;
            }
            node = node.Next!;
        }
    }
}

public interface ILazyEvaluator
{
    public void EvaluateValues<T>(int rowIndex, Span<T> destination);
}