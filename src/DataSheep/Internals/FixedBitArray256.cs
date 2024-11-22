using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;


internal interface IFixedBitArray256
{
    public bool this[int index] { get; set; }
    public bool All(Range range);
    public Range GetNextFalseChunk(ref int cursor);
    public void BulkSet(Range range, bool flag);
}


internal unsafe struct FixedBitArray256 : IFixedBitArray256
{
    private fixed ulong _values[4];

    [UnscopedRef]
    internal ref ulong Get(int i) => ref _values[i];

    public bool this[int index]
    {
        get => ((_values[index >> 6] >> (index & 63)) & 1) == 1;
        set => _values[index >> 6] =
            value
            ? ((1uL << (index & 63)) | _values[index >> 6])
            : (~(1uL << (index & 63)) & _values[index >> 6]);
    }


    public bool All(Range range)
    {
        var (start, length) = range.GetOffsetAndLength(256);
        var end = start + length;
        for(var n = start >> 6; n < 4; ++n)
        {
            var head = 64 * n;
            var localStart = Math.Max(start - head, 0);
            var localEnd = Math.Min(end - head, 64);
            if(localEnd - localStart <= 0)
            {
                continue;
            }
            if((_values[n] | ~makeBitMask(localStart, localEnd)) != ulong.MaxValue)
            {
                return false;
            }
        }
        return true;
    }

    public Range GetNextFalseChunk(ref int cursor)
    {
        var start = 256;
        var end = 256;
        var n = cursor >> 6;
        for(; n < 4; ++n)
        {
            var head = 64 * n;
            var localStart = Math.Max(cursor - head, 0);
            if(localStart >= 64)
            {
                continue;
            }
            var masked = ~(_values[n] | ~makeBitMask(localStart, 64));
            var skippedTrueCount = BitOperations.TrailingZeroCount(masked);
            if(skippedTrueCount == 64)
            {
                continue;
            }
            start = (n << 6) + skippedTrueCount;
            break;
        }
        for(; n < 4; ++n)
        {
            var head = 64 * n;
            var localStart = Math.Max(start - head, 0);
            var masked = _values[n] & makeBitMask(localStart, 64);
            var takeFalseCount = BitOperations.TrailingZeroCount(masked);
            end = (n << 6) + takeFalseCount;
            if(takeFalseCount < 64)
            {
                break;
            }
        }
        cursor = end;
        return new Range(start, end);
    }

    public void BulkSet(Range range, bool flag)
    {
        var (start, length) = range.GetOffsetAndLength(256);
        var end = start + length;
        var startN = start >> 6;
        var endN = (end - 1) >> 6;
        if(flag)
        {
            for(var n = startN; n <= endN; ++n)
            {
                var head = 64 * n;
                var localStart = Math.Max(start - head, 0);
                var localEnd = Math.Min(end - head, 64);
                _values[n] |= makeBitMask(localStart, localEnd);
            }
        }
        else
        {
            for(var n = startN; n <= endN; ++n)
            {
                var head = 64 * n;
                var localStart = Math.Max(start - head, 0);
                var localEnd = Math.Min(end - head, 64);
                _values[n] &= ~makeBitMask(localStart, localEnd);
            }
        }
    }

    public override string ToString()
    {
        var buffer = (stackalloc char[256]);
        for(var i = 0; i < 256; ++i)
        {
            buffer[i] = this[i] ? '1' : '0';
        }
        return buffer.ToString();
    }

    private static ulong makeBitMask(int start, int end)
        => ((ulong.MaxValue >> start) << (start + 64 - end)) >> (64 - end);
}
