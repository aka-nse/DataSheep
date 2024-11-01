using System;
using System.Collections.Generic;
using System.Text;

namespace DataSheep.Generators;

internal static class InternalHelpers
{
    public static string CommaJoined(int start, int count, Func<int, string> predicate)
        => string.Join(", ", Enumerable.Range(start, count).Select(predicate));
}
