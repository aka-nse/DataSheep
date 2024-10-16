using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

internal class ReadOnlyListSpan<T>(IReadOnlyList<T> source, int start, int count) : IReadOnlyList<T>
{
    private readonly IReadOnlyList<T> _source
        = source ?? throw new ArgumentNullException(nameof(source));

    private readonly int _start
        = (uint)start < (uint)source.Count
        ? start
        : throw new ArgumentOutOfRangeException(nameof(start));

    private readonly int _count
        = (count >= 0 && start + count <= source.Count)
        ? count
        : throw new ArgumentException();

    public T this[int index]
        => (uint)index < (uint)_count
        ? _source[index + _start]
        : throw new ArgumentOutOfRangeException(nameof(index));

    public int Count => _count;

    public IEnumerator<T> GetEnumerator()
    {
        for(var i = 0; i < _count; ++i)
        {
            yield return this[i];
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

internal static class ReadOnlyListSpan
{
    public static IReadOnlyList<T> Slice<T>(this IReadOnlyList<T> list, int start)
        => Slice(list, start, list.Count - start);

    public static IReadOnlyList<T> Slice<T>(this IReadOnlyList<T> list, int start, int count)
        => new ReadOnlyListSpan<T>(list, start, count);
}