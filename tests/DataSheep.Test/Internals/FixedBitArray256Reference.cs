using System.Collections;

namespace DataSheep;

internal class FixedBitArray256Reference : IFixedBitArray256
{
    private readonly BitArray _bitArray = new(256);

    public bool this[int index]
    {
        get => _bitArray[index];
        set => _bitArray[index] = value;
    }

    public bool All(Range range)
    {
        var (start, length) = range.GetOffsetAndLength(256);
        var end = start + length;
        for(var i = start; i < end; ++i)
        {
            if(!_bitArray[i])
            {
                return false;
            }
        }
        return true;
    }

    public Range GetNextFalseChunk(ref int cursor)
    {
        var start = cursor;
        for(; start < 256; ++start)
        {
            if(!_bitArray[start])
            {
                break;
            }
        }
        for(cursor = start; cursor < 256; ++cursor)
        {
            if(_bitArray[cursor])
            {
                break;
            }
        }
        return start..cursor;
    }

    public void BulkSet(Range range, bool flag)
    {
        var (start, length) = range.GetOffsetAndLength(256);
        var end = start + length;
        for(var i = start; i < end; ++i)
        {
            _bitArray[i] = flag;
        }
    }

    public override string ToString()
    {
        var buffer = (stackalloc char[256]);
        for(var i = 0; i < 256; ++i)
        {
            buffer[i] = _bitArray[i] ? '1' : '0';
        }
        return buffer.ToString();
    }

}