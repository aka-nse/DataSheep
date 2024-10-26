using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataSheep;

internal static class TemporaryBuffer
{
    public static TemporaryBuffer<T> Create<T>(IReadOnlyList<T> values)
    {
        var buffer = new TemporaryBuffer<T>(values.Count);
        for(var i = 0; i < buffer.Span.Length; ++i)
        {
            buffer.Span[i] = values[i];
        }
        return buffer;
    }
}


internal ref struct TemporaryBuffer<T>
{
    private readonly int _count;
    private T[] _array;
    public readonly Span<T> Span => _array.AsSpan(0, _count);
    public readonly Memory<T> Memory => _array.AsMemory(0, _count);

    public TemporaryBuffer(int count)
    {
        _count = count;
        _array = ArrayPool<T>.Shared.Rent(count);
    }

    public void Dispose()
    {
        if(Interlocked.Exchange(ref _array!, null) is { } buffer)
        {
            ArrayPool<T>.Shared.Return(buffer);
        }
    }
}
