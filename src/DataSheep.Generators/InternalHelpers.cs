using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.CompilerServices;

namespace DataSheep.Generators;

internal static class InternalHelpers
{
    public static string CommaJoined(int start, int count, Func<int, string> predicate)
        => string.Join(", ", Enumerable.Range(start, count).Select(predicate));

    public static string CommaJoined<T>(this IEnumerable<T> source, Func<T, string> predicate)
        => string.Join(", ", source.Select(predicate));

    public static string CommaJoined<T>(this IEnumerable<T> source, Func<T, int, string> predicate)
        => string.Join(", ", source.Select(predicate));

    public static string LineJoined<T>(this IEnumerable<T> source, Func<T, string> predicate)
        => string.Join(Environment.NewLine, source.Select(predicate));

    public static string LineJoined<T>(this IEnumerable<T> source, Func<T, int, string> predicate)
        => string.Join(Environment.NewLine, source.Select(predicate));

    public static string LineJoined<T>(this IEnumerable<T> source, int indentLevel, Func<T, string> predicate)
        => string.Join(Environment.NewLine + new string(' ', 4 * indentLevel), source.Select(predicate));

    public static string LineJoined<T>(this IEnumerable<T> source, int indentLevel, Func<T, int, string> predicate)
        => string.Join(Environment.NewLine + new string(' ', 4 * indentLevel), source.Select(predicate));
}
