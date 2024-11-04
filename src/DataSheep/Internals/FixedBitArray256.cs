using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

internal unsafe struct FixedBitArray256
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


    public bool All(int start, int count)
    {
#warning reference implement
        for(var i = 0; i < count; ++i)
        {
            if(!this[start + i])
            {
                return false;
            }
        }
        return true;
    }

    public (int start, int count) GetNextFalseChunk(int start, int maxCount)
    {
#warning reference implement
        var tail = start + maxCount;
        for(; start < 256; ++start)
        {
            if(!this[start])
            {
                break;
            }
        }
        var end = start;
        for(; end < tail; ++end)
        {
            if(this[end])
            {
                break;
            }
        }
        return (start, end - start);
    }

    public void BulkSet(int start, int count, bool flag)
    {
#warning reference implement
        var end = start + count;
        for(var i = start; i < end; ++i)
        {
            this[i] = flag;
        }
    }
}
