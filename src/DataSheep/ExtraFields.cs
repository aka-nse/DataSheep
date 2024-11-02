using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

/// <summary>
/// Presents container for fields in remaining columns.
/// </summary>
public class ExtraFields
    : ITuple
{
    private readonly object?[] _fields;

    /// <inheritdoc />
    public int Length => _fields.Length;

    /// <summary> Returns the value of the specified element. </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public object? this[int index] => _fields[index];

    /// <summary></summary>
    /// <param name="length"></param>
    public ExtraFields(int length)
    {
        _fields = new object?[length];
    }

    /// <summary></summary>
    /// <param name="fields"></param>
    public ExtraFields(ReadOnlySpan<object?> fields)
    {
        _fields = fields.ToArray();
    }
}

/// <summary>
/// Presents container for fields in remaining columns, which can be parsed as the specified type.
/// </summary>
public class ExtraFields<T>
    : ITuple
{
    private readonly T[] _fields;

    /// <inheritdoc />
    public int Length => _fields.Length;

    /// <summary> Returns the value of the specified element. </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public T this[int index] => _fields[index];
    object? ITuple.this[int index] => _fields[index];

    /// <summary></summary>
    /// <param name="length"></param>
    public ExtraFields(int length)
    {
        _fields = new T[length];
    }

    /// <summary></summary>
    /// <param name="fields"></param>
    public ExtraFields(ReadOnlySpan<T> fields)
    {
        _fields = fields.ToArray();
    }
}

