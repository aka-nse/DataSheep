using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

/// <summary>
/// Provides overload selector trait types.
/// </summary>
public static class OverloadResolver
{
    /// <summary>
    /// When optional of this type is put on last argument, its overload shall be resolved as <see cref="IDataRecord{TSelf}"/> pattern.
    /// </summary>
    public readonly struct DataRecord;

    /// <summary>
    /// When optional of this type is put on last argument, its overload shall be resolved as value tuple pattern.
    /// </summary>
    public readonly struct ValueTuple;
}
